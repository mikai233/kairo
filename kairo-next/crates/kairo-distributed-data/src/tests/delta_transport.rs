use super::*;
use bytes::Bytes;
use kairo_actor::Recipient;
use kairo_serialization::SerializationError;

use crate::{DeltaPropagation, DeltaPropagationTargetRegistry};

#[derive(Clone)]
struct ChannelRecipient {
    tx: mpsc::Sender<ReplicatorDeltaPropagation>,
}

impl Recipient<ReplicatorDeltaPropagation> for ChannelRecipient {
    fn tell(
        &self,
        message: ReplicatorDeltaPropagation,
    ) -> Result<(), kairo_actor::SendError<ReplicatorDeltaPropagation>> {
        self.tx
            .send(message)
            .map_err(|error| kairo_actor::SendError::new(error.0, "channel closed"))
    }
}

#[derive(Clone, Copy)]
struct SelectiveFailCodec;

impl CrdtDataCodec<GCounter> for SelectiveFailCodec {
    fn manifest(&self) -> &'static str {
        GCounterCodec.manifest()
    }

    fn encode_payload(&self, data: &GCounter) -> kairo_serialization::Result<Bytes> {
        if data.state().values().copied().sum::<u128>() == 2 {
            return Err(SerializationError::Message(
                "deliberate delta encode failure".to_string(),
            ));
        }
        GCounterCodec.encode_payload(data)
    }

    fn decode_payload(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<GCounter> {
        GCounterCodec.decode_payload(payload, version)
    }
}

fn propagation_for(
    target: ReplicaId,
    amount: u128,
) -> BTreeMap<ReplicaId, DeltaPropagation<GCounter>> {
    let mut log = DeltaPropagationLog::new([target]);
    log.record_delta(
        ReplicatorKey::new(format!("counter-{amount}")),
        Some(delta_counter("writer", amount)),
    );
    log.collect_propagations()
}

#[test]
fn delta_transport_publishes_collected_propagations_to_targets() {
    let system = ActorSystem::builder("ddata-delta-transport")
        .build()
        .unwrap();
    let (target_ref, target_rx) = forward_ref(&system, "remote-replicator");
    let local = replica("local");
    let remote = replica("remote");
    let key = ReplicatorKey::new("counter");
    let mut log = DeltaPropagationLog::new([remote.clone()]);
    log.record_delta(key.clone(), Some(delta_counter("a", 5)));
    let propagations = log.collect_propagations();
    let transport = DeltaPropagationTransport::new(local.clone(), GCounterCodec);
    transport.insert_target(DeltaPropagationTarget::new(remote.clone(), target_ref));

    let report = transport.publish(propagations);

    assert!(report.is_success());
    assert_eq!(report.sent_to(), &[remote]);
    let wire = target_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(wire.from, local);
    assert!(!wire.reply);
    let decoded = decode_delta_propagation(&wire, &GCounterCodec).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].key(), &key);
    assert_eq!(decoded[0].delta().value().unwrap(), 5);

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn delta_transport_reports_missing_targets_without_dropping_other_sends() {
    let system = ActorSystem::builder("ddata-delta-transport-missing")
        .build()
        .unwrap();
    let (target_ref, target_rx) = forward_ref(&system, "remote-a");
    let remote_a = replica("remote-a");
    let remote_b = replica("remote-b");
    let mut log = DeltaPropagationLog::new([remote_a.clone(), remote_b.clone()]);
    log.record_delta(ReplicatorKey::new("counter"), Some(delta_counter("a", 1)));
    let propagations = log.collect_propagations();
    let transport = DeltaPropagationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(DeltaPropagationTarget::new(remote_a.clone(), target_ref));

    let report = transport.publish(propagations);

    assert_eq!(report.sent_to(), &[remote_a]);
    assert!(matches!(
        report.failures(),
        [DeltaTransportFailure::MissingTarget { replica }] if replica == &remote_b
    ));
    target_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn delta_transport_clones_observe_target_replacement_and_removal() {
    let registry = DeltaPropagationTargetRegistry::new();
    let transport = DeltaPropagationTransport::with_target_registry(
        replica("local"),
        GCounterCodec,
        registry.clone(),
    );
    let remote = replica("remote");
    let (first_tx, first_rx) = mpsc::channel();
    registry.insert_target(DeltaPropagationTarget::new(
        remote.clone(),
        ChannelRecipient { tx: first_tx },
    ));

    assert!(
        transport
            .publish(propagation_for(remote.clone(), 1))
            .is_success()
    );
    first_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let (replacement_tx, replacement_rx) = mpsc::channel();
    transport.insert_target(DeltaPropagationTarget::new(
        remote.clone(),
        ChannelRecipient { tx: replacement_tx },
    ));

    assert!(
        transport
            .publish(propagation_for(remote.clone(), 2))
            .is_success()
    );
    replacement_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(first_rx.try_recv().is_err());

    registry.remove_target(&remote);

    assert_eq!(transport.target_count(), 0);
    assert_eq!(
        transport
            .publish(propagation_for(remote.clone(), 3))
            .failures(),
        &[DeltaTransportFailure::MissingTarget { replica: remote }]
    );
}

#[test]
fn delta_transport_send_failure_does_not_drop_other_targets() {
    let remote_a = replica("remote-a");
    let remote_b = replica("remote-b");
    let mut log = DeltaPropagationLog::new([remote_a.clone(), remote_b.clone()]);
    log.record_delta(
        ReplicatorKey::new("counter"),
        Some(delta_counter("writer", 1)),
    );
    let (open_tx, open_rx) = mpsc::channel();
    let (closed_tx, closed_rx) = mpsc::channel();
    drop(closed_rx);
    let transport = DeltaPropagationTransport::new(replica("local"), GCounterCodec);
    transport.set_targets([
        DeltaPropagationTarget::new(remote_a.clone(), ChannelRecipient { tx: open_tx }),
        DeltaPropagationTarget::new(remote_b.clone(), ChannelRecipient { tx: closed_tx }),
    ]);

    let report = transport.publish(log.collect_propagations());

    assert_eq!(report.sent_to(), &[remote_a]);
    assert_eq!(
        report.failures(),
        &[DeltaTransportFailure::SendFailed {
            replica: remote_b,
            reason: "channel closed".to_string(),
        }]
    );
    open_rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

#[test]
fn delta_transport_encode_failure_does_not_drop_other_targets() {
    let remote_a = replica("remote-a");
    let remote_b = replica("remote-b");
    let mut propagations = propagation_for(remote_a.clone(), 1);
    propagations.extend(propagation_for(remote_b.clone(), 2));
    let (first_tx, first_rx) = mpsc::channel();
    let (second_tx, second_rx) = mpsc::channel();
    let transport =
        DeltaPropagationTransport::new(replica("local"), SelectiveFailCodec).with_reply(true);
    transport.set_targets([
        DeltaPropagationTarget::new(remote_a.clone(), ChannelRecipient { tx: first_tx }),
        DeltaPropagationTarget::new(remote_b.clone(), ChannelRecipient { tx: second_tx }),
    ]);

    let report = transport.publish(propagations);

    assert_eq!(report.sent_to(), &[remote_a]);
    assert_eq!(
        report.failures(),
        &[DeltaTransportFailure::EncodeFailed {
            replica: remote_b,
            reason: "deliberate delta encode failure".to_string(),
        }]
    );
    assert!(first_rx.recv_timeout(Duration::from_secs(1)).unwrap().reply);
    assert!(matches!(
        second_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
}
