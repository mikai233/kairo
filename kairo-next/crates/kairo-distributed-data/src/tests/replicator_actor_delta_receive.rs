use super::*;

#[test]
fn replicator_actor_applies_causal_delta_and_reports_status() {
    let system = ActorSystem::builder("ddata-replicator-causal-delta")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (status_ref, status_rx) = forward_ref(&system, "delta-status");
    let (get_ref, get_rx) = forward_ref(&system, "get-replies");
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");

    replicator
        .tell(ReplicatorActorMsg::WriteCausalDelta {
            from: remote.clone(),
            key: key.clone(),
            from_version: 1,
            to_version: 1,
            delta: delta_counter("a", 4),
            reply_to: status_ref.clone(),
        })
        .unwrap();
    let applied = status_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(matches!(
        applied,
        DeltaReceiveStatus::Applied {
            previous_version: 0,
            to_version: 1,
            changed: true,
            ..
        }
    ));

    replicator
        .tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::local(),
            reply_to: get_ref,
        })
        .unwrap();
    assert_eq!(
        get_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .data()
            .unwrap()
            .value()
            .unwrap(),
        4
    );

    replicator
        .tell(ReplicatorActorMsg::WriteCausalDelta {
            from: remote,
            key,
            from_version: 3,
            to_version: 3,
            delta: delta_counter("a", 7),
            reply_to: status_ref,
        })
        .unwrap();
    let missing = status_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(matches!(
        missing,
        DeltaReceiveStatus::Missing {
            current_version: 1,
            expected_from_version: 2,
            from_version: 3,
            ..
        }
    ));

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_applies_remote_delta_propagation_with_codec() {
    let system = ActorSystem::builder("ddata-replicator-apply-propagation")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (report_ref, report_rx) = forward_ref(&system, "delta-report");
    let (get_ref, get_rx) = forward_ref(&system, "get-replies");
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let mut log = DeltaPropagationLog::new([replica("local")]);
    log.record_delta(key.clone(), Some(delta_counter("a", 6)));
    let propagation = log.collect_propagations().into_values().next().unwrap();
    let wire = encode_delta_propagation(remote.clone(), true, &propagation, &GCounterCodec)
        .expect("wire propagation should encode");

    replicator
        .tell(ReplicatorActorMsg::ApplyDeltaPropagation {
            propagation: wire,
            codec: Arc::new(GCounterCodec),
            reply_to: report_ref,
        })
        .unwrap();

    let report = report_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(report.from(), &remote);
    assert!(report.is_success());
    assert!(matches!(report.reply(), Some(DeltaReceiveReply::Ack(_))));
    assert!(matches!(
        report.statuses(),
        [DeltaReceiveStatus::Applied {
            previous_version: 0,
            to_version: 1,
            changed: true,
            ..
        }]
    ));

    replicator
        .tell(ReplicatorActorMsg::Get {
            key,
            consistency: ReadConsistency::local(),
            reply_to: get_ref,
        })
        .unwrap();
    assert_eq!(
        get_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .data()
            .unwrap()
            .value()
            .unwrap(),
        6
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}
