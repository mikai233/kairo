use super::*;

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
    let mut transport = DeltaPropagationTransport::new(local.clone(), GCounterCodec);
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
    let mut transport = DeltaPropagationTransport::new(replica("local"), GCounterCodec);
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
