use super::*;

#[test]
fn replicator_actor_gossip_tick_sends_status_to_reachable_target() {
    let system = ActorSystem::builder("ddata-replicator-gossip-tick")
        .build()
        .unwrap();
    let remote = replica("remote");
    let (status_ref, status_rx) = forward_ref::<ReplicatorGossipStatus>(&system, "gossip-status");
    let (gossip_ref, _gossip_rx) = forward_ref::<ReplicatorGossip>(&system, "gossip-target");
    let transport = ReplicatorGossipTransport::new();
    transport.insert_target(ReplicatorGossipTarget::new(
        remote.clone(),
        status_ref,
        gossip_ref,
    ));
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GCounter>::with_gossip(transport, Arc::new(GCounterCodec))
                    .with_self_system_uid(1)
            }),
        )
        .unwrap();
    let (tick_ref, tick_rx) = forward_ref::<ReplicatorGossipTickReport>(&system, "gossip-reply");

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![remote.clone()],
            unreachable: BTreeSet::new(),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::WriteFull {
            key: ReplicatorKey::new("counter"),
            envelope: DataEnvelope::new(full_counter("local", 3)),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::RunGossip {
            reply_to: Some(tick_ref),
        })
        .unwrap();

    let report = tick_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(report.target(), Some(&remote));
    assert_eq!(report.transport().sent_status_to(), &[remote]);
    let status = status_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(status.entries.len(), 1);
    assert_eq!(status.entries[0].key, "counter");
    assert_ne!(status.entries[0].digest, 0);
    assert_eq!(status.from_system_uid, Some(1));

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_schedules_gossip_ticks_with_manual_time() {
    let manual = ManualScheduler::new();
    let system = ActorSystem::builder("ddata-replicator-gossip-scheduled")
        .manual_scheduler(manual.clone())
        .build()
        .unwrap();
    let remote = replica("remote");
    let (status_ref, status_rx) =
        forward_ref::<ReplicatorGossipStatus>(&system, "scheduled-status");
    let (gossip_ref, _gossip_rx) = forward_ref::<ReplicatorGossip>(&system, "scheduled-gossip");
    let transport = ReplicatorGossipTransport::new();
    transport.insert_target(ReplicatorGossipTarget::new(
        remote.clone(),
        status_ref,
        gossip_ref,
    ));
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GCounter>::with_gossip_interval(
                    transport,
                    Arc::new(GCounterCodec),
                    Duration::from_millis(25),
                )
            }),
        )
        .unwrap();

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![remote],
            unreachable: BTreeSet::new(),
        })
        .unwrap();
    let (barrier_ref, barrier_rx) =
        forward_ref::<ReplicatorGossipTickReport>(&system, "scheduled-barrier");
    replicator
        .tell(ReplicatorActorMsg::RunGossip {
            reply_to: Some(barrier_ref),
        })
        .unwrap();
    barrier_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    status_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    manual.advance(Duration::from_millis(24));
    assert!(status_rx.recv_timeout(Duration::from_millis(50)).is_err());
    manual.advance(Duration::from_millis(1));
    let status = status_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(status.chunk, 0);
    assert_eq!(status.total_chunks, 1);

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_receives_gossip_status_and_sends_gossip_responses() {
    let system = ActorSystem::builder("ddata-replicator-gossip-status")
        .build()
        .unwrap();
    let remote = replica("remote");
    let (status_ref, status_rx) = forward_ref::<ReplicatorGossipStatus>(&system, "missing-status");
    let (gossip_ref, gossip_rx) = forward_ref::<ReplicatorGossip>(&system, "full-gossip");
    let transport = ReplicatorGossipTransport::new();
    transport.insert_target(ReplicatorGossipTarget::new(
        remote.clone(),
        status_ref,
        gossip_ref,
    ));
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GCounter>::with_gossip(transport, Arc::new(GCounterCodec))
            }),
        )
        .unwrap();
    let (reply_ref, reply_rx) =
        forward_ref::<ReplicatorGossipStatusReceiveReport>(&system, "status-report");

    replicator
        .tell(ReplicatorActorMsg::WriteFull {
            key: ReplicatorKey::new("different"),
            envelope: DataEnvelope::new(full_counter("local", 10)),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::WriteFull {
            key: ReplicatorKey::new("local-only"),
            envelope: DataEnvelope::new(full_counter("local", 20)),
        })
        .unwrap();
    let remote_envelope = DataEnvelope::new(full_counter("remote", 99));
    let remote_digest = crate::digest_envelope(&remote_envelope, &GCounterCodec).unwrap();

    replicator
        .tell(ReplicatorActorMsg::ReceiveGossipStatus {
            from: remote.clone(),
            status: ReplicatorGossipStatus {
                entries: vec![
                    crate::ReplicatorGossipDigest {
                        key: "different".to_string(),
                        digest: remote_digest,
                        used_timestamp_millis: 0,
                    },
                    crate::ReplicatorGossipDigest {
                        key: "remote-only".to_string(),
                        digest: 7,
                        used_timestamp_millis: 0,
                    },
                ],
                chunk: 0,
                total_chunks: 1,
                to_system_uid: Some(1),
                from_system_uid: Some(2),
            },
            codec: Arc::new(GCounterCodec),
            reply_to: Some(reply_ref),
        })
        .unwrap();

    let report = reply_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(report.plan().gossip().is_some());
    assert!(report.plan().missing_status().is_some());
    assert_eq!(
        report.transport().sent_gossip_to(),
        std::slice::from_ref(&remote)
    );
    assert_eq!(
        report.transport().sent_status_to(),
        std::slice::from_ref(&remote)
    );
    let gossip = gossip_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(gossip.send_back);
    assert_eq!(
        gossip
            .entries
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>(),
        vec!["different", "local-only"]
    );
    let missing = status_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(missing.entries[0].key, "remote-only");
    assert_eq!(missing.entries[0].digest, 0);

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_receives_gossip_merges_state_and_sends_reply() {
    let system = ActorSystem::builder("ddata-replicator-gossip-receive")
        .build()
        .unwrap();
    let remote = replica("remote");
    let (status_ref, _status_rx) = forward_ref::<ReplicatorGossipStatus>(&system, "status-target");
    let (gossip_ref, gossip_rx) = forward_ref::<ReplicatorGossip>(&system, "gossip-replies");
    let transport = ReplicatorGossipTransport::new();
    transport.insert_target(ReplicatorGossipTarget::new(
        remote.clone(),
        status_ref,
        gossip_ref,
    ));
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GCounter>::with_gossip(transport, Arc::new(GCounterCodec))
            }),
        )
        .unwrap();
    let (reply_ref, reply_rx) =
        forward_ref::<ReplicatorGossipReceiveReport>(&system, "gossip-report");

    replicator
        .tell(ReplicatorActorMsg::WriteFull {
            key: ReplicatorKey::new("counter"),
            envelope: DataEnvelope::new(full_counter("local", 1)),
        })
        .unwrap();
    let remote_envelope = encode_data_envelope(
        &DataEnvelope::new(full_counter("remote", 5)),
        &GCounterCodec,
    )
    .unwrap();
    replicator
        .tell(ReplicatorActorMsg::ReceiveGossip {
            from: remote.clone(),
            gossip: ReplicatorGossip {
                entries: vec![crate::ReplicatorGossipEntry {
                    key: "counter".to_string(),
                    envelope: remote_envelope,
                    used_timestamp_millis: 0,
                }],
                send_back: true,
                to_system_uid: Some(1),
                from_system_uid: Some(2),
            },
            codec: Arc::new(GCounterCodec),
            reply_to: Some(reply_ref),
        })
        .unwrap();

    let report = reply_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(
        report
            .apply()
            .changed_keys()
            .contains(&ReplicatorKey::new("counter"))
    );
    assert_eq!(report.transport().sent_gossip_to(), &[remote]);
    let reply = gossip_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(!reply.send_back);
    assert_eq!(reply.entries.len(), 1);

    let (get_ref, get_rx) = forward_ref::<GetResponse<GCounter>>(&system, "get-after-gossip");
    replicator
        .tell(ReplicatorActorMsg::Get {
            key: ReplicatorKey::new("counter"),
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
