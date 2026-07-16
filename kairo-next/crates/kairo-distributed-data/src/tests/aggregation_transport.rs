use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
struct CountingGCounterCodec {
    encode_count: Arc<AtomicUsize>,
}

impl CrdtDataCodec<GCounter> for CountingGCounterCodec {
    fn manifest(&self) -> &'static str {
        GCounterCodec.manifest()
    }

    fn encode_payload(&self, data: &GCounter) -> kairo_serialization::Result<bytes::Bytes> {
        self.encode_count.fetch_add(1, Ordering::SeqCst);
        GCounterCodec.encode_payload(data)
    }

    fn decode_payload(
        &self,
        payload: bytes::Bytes,
        version: u16,
    ) -> kairo_serialization::Result<GCounter> {
        GCounterCodec.decode_payload(payload, version)
    }
}

#[test]
fn aggregation_transport_sends_primary_write_and_read_messages() {
    let system = ActorSystem::builder("ddata-aggregation-transport")
        .build()
        .unwrap();
    let (write_a, write_rx_a) = forward_ref(&system, "write-a");
    let (read_a, read_rx_a) = forward_ref(&system, "read-a");
    let (write_b, write_rx_b) = forward_ref(&system, "write-b");
    let (read_b, read_rx_b) = forward_ref(&system, "read-b");
    let (write_c, write_rx_c) = forward_ref(&system, "write-c");
    let (read_c, read_rx_c) = forward_ref(&system, "read-c");
    let key = ReplicatorKey::new("counter");
    let remote_nodes = vec![replica("a"), replica("b"), replica("c")];
    let write_state = WriteAggregatorState::new(
        key.clone(),
        &WriteConsistency::majority(Duration::from_secs(1)),
        remote_nodes.clone(),
    )
    .unwrap();
    let read_state = ReadAggregatorState::<GCounter>::new(
        key.clone(),
        &ReadConsistency::majority(Duration::from_secs(1)),
        remote_nodes,
        None,
    )
    .unwrap();
    let write_plan = WriteAggregationPlan::new(
        write_state.clone(),
        write_state.select_replicas(&BTreeSet::new()),
    );
    let read_plan = ReadAggregationPlan::new(
        read_state.clone(),
        read_state.select_replicas(&BTreeSet::new()),
    );
    let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
    transport.set_targets([
        AggregationTarget::new(replica("a"), write_a, read_a),
        AggregationTarget::new(replica("b"), write_b, read_b),
        AggregationTarget::new(replica("c"), write_c, read_c),
    ]);
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(replica("local"), 5)
            .unwrap()
            .reset_delta(),
    );

    let write_report = transport.publish_write(&write_plan, &envelope);
    let read_report = transport.publish_read(&read_plan);

    assert!(write_report.is_success());
    assert_eq!(write_report.sent_to(), &[replica("a"), replica("b")]);
    assert!(read_report.is_success());
    assert_eq!(read_report.sent_to(), &[replica("a"), replica("b")]);

    for rx in [&write_rx_a, &write_rx_b] {
        let wire = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(wire.key, key.as_str());
        assert_eq!(wire.from, Some(replica("local")));
        assert_eq!(
            decode_data_envelope::<GCounter, _>(&wire.envelope, &GCounterCodec)
                .unwrap()
                .data()
                .value()
                .unwrap(),
            5
        );
    }
    for rx in [&read_rx_a, &read_rx_b] {
        let wire = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(wire.key, key.as_str());
        assert_eq!(wire.from, Some(replica("local")));
    }
    assert!(write_rx_c.recv_timeout(Duration::from_millis(100)).is_err());
    assert!(read_rx_c.recv_timeout(Duration::from_millis(100)).is_err());

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn aggregation_transport_reports_missing_targets_without_stopping_other_sends() {
    let system = ActorSystem::builder("ddata-aggregation-transport-missing")
        .build()
        .unwrap();
    let (write_a, write_rx_a) = forward_ref(&system, "write-a");
    let (read_a, read_rx_a) = forward_ref(&system, "read-a");
    let nodes = vec![replica("a"), replica("b")];
    let key = ReplicatorKey::new("counter");
    let write_state = WriteAggregatorState::new(
        key.clone(),
        &WriteConsistency::to(3, Duration::from_secs(1)).unwrap(),
        nodes.clone(),
    )
    .unwrap();
    let read_state = ReadAggregatorState::<GCounter>::new(
        key.clone(),
        &ReadConsistency::from(3, Duration::from_secs(1)).unwrap(),
        nodes,
        None,
    )
    .unwrap();
    let write_plan = WriteAggregationPlan::new(
        write_state.clone(),
        write_state.select_replicas(&BTreeSet::new()),
    );
    let read_plan = ReadAggregationPlan::new(
        read_state.clone(),
        read_state.select_replicas(&BTreeSet::new()),
    );
    let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(AggregationTarget::new(replica("a"), write_a, read_a));
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(replica("local"), 1)
            .unwrap()
            .reset_delta(),
    );

    let write_report = transport.publish_write(&write_plan, &envelope);
    let read_report = transport.publish_read(&read_plan);

    assert_eq!(write_report.sent_to(), &[replica("a")]);
    assert!(matches!(
        write_report.failures(),
        [AggregationTransportFailure::MissingTarget { replica: failed_replica, operation }]
            if failed_replica == &replica("b") && operation == &AggregationTransportOperation::Write
    ));
    assert_eq!(read_report.sent_to(), &[replica("a")]);
    assert!(matches!(
        read_report.failures(),
        [AggregationTransportFailure::MissingTarget { replica: failed_replica, operation }]
            if failed_replica == &replica("b") && operation == &AggregationTransportOperation::Read
    ));
    let write = write_rx_a.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(write.key, key.as_str());
    let read = read_rx_a.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(read.key, key.as_str());

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn aggregation_transport_encodes_a_write_once_for_the_whole_fanout() {
    let system = ActorSystem::builder("ddata-aggregation-transport-single-encode")
        .build()
        .unwrap();
    let (write_a, write_rx_a) = forward_ref(&system, "write-a");
    let (read_a, _read_rx_a) = forward_ref(&system, "read-a");
    let (write_b, write_rx_b) = forward_ref(&system, "write-b");
    let (read_b, _read_rx_b) = forward_ref(&system, "read-b");
    let key = ReplicatorKey::new("counter");
    let write_state = WriteAggregatorState::new(
        key.clone(),
        &WriteConsistency::majority(Duration::from_secs(1)),
        vec![replica("a"), replica("b")],
    )
    .unwrap();
    let write_plan = WriteAggregationPlan::new(
        write_state.clone(),
        write_state.select_replicas(&BTreeSet::new()),
    );
    let encode_count = Arc::new(AtomicUsize::new(0));
    let codec = CountingGCounterCodec {
        encode_count: encode_count.clone(),
    };
    let mut transport = AggregationTransport::new(replica("local"), codec);
    transport.set_targets([
        AggregationTarget::new(replica("a"), write_a, read_a),
        AggregationTarget::new(replica("b"), write_b, read_b),
    ]);
    let envelope = DataEnvelope::new(full_counter("local", 3));

    let report = transport.publish_write_to_replicas(
        &[replica("missing"), replica("a"), replica("b")],
        &write_plan,
        &envelope,
    );

    assert_eq!(encode_count.load(Ordering::SeqCst), 1);
    assert_eq!(report.sent_to(), &[replica("a"), replica("b")]);
    assert!(matches!(
        report.failures(),
        [AggregationTransportFailure::MissingTarget { replica: failed_replica, operation }]
            if failed_replica == &replica("missing")
                && operation == &AggregationTransportOperation::Write
    ));
    for rx in [&write_rx_a, &write_rx_b] {
        let wire = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(wire.key, key.as_str());
        assert_eq!(
            decode_data_envelope::<GCounter, _>(&wire.envelope, &GCounterCodec)
                .unwrap()
                .data()
                .value()
                .unwrap(),
            3
        );
    }

    system.terminate(Duration::from_secs(1)).unwrap();
}
