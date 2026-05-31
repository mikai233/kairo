use super::*;

#[test]
fn replicator_actor_collects_delta_propagations_from_local_updates() {
    let system = ActorSystem::builder("ddata-replicator-delta")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let (delta_ref, delta_rx) = forward_ref::<BTreeMap<ReplicaId, crate::DeltaPropagation<GCounter>>>(
        &system,
        "delta-replies",
    );
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let node = replica("local");

    replicator
        .tell(ReplicatorActorMsg::SetDeltaNodes {
            nodes: vec![remote.clone()],
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GCounter::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(move |counter| {
                counter
                    .increment(node.clone(), 3)
                    .map_err(|e| e.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();
    update_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    replicator
        .tell(ReplicatorActorMsg::CollectDeltaPropagations {
            reply_to: delta_ref,
        })
        .unwrap();

    let propagations = delta_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let entry = propagations
        .get(&remote)
        .unwrap()
        .entries()
        .get(&key)
        .unwrap();
    assert_eq!(entry.from_version(), 1);
    assert_eq!(entry.to_version(), 1);
    assert_eq!(entry.delta().value().unwrap(), 3);

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_delta_loop_publishes_and_cleans_on_manual_tick() {
    let system = ActorSystem::builder("ddata-replicator-delta-loop")
        .build()
        .unwrap();
    let (target_ref, target_rx) =
        forward_ref::<ReplicatorDeltaPropagation>(&system, "delta-target");
    let mut transport = DeltaPropagationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(DeltaPropagationTarget::new(replica("remote"), target_ref));
    let delta_loop = DeltaPropagationLoop::new(transport).with_cleanup_every_ticks(1);
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GCounter>::with_delta_propagation_loop(delta_loop)
            }),
        )
        .unwrap();
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let (tick_ref, tick_rx) = forward_ref::<DeltaPropagationTickReport>(&system, "tick-replies");
    let key = ReplicatorKey::new("counter");

    replicator
        .tell(ReplicatorActorMsg::SetDeltaNodes {
            nodes: vec![replica("remote")],
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GCounter::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(|counter| {
                counter
                    .increment(replica("local"), 5)
                    .map_err(|error| error.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();
    update_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    replicator
        .tell(ReplicatorActorMsg::RunDeltaPropagation { reply_to: tick_ref })
        .unwrap();
    let report = tick_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(report.propagation_count(), 1);
    assert!(report.cleaned_delta_entries());
    assert_eq!(report.transport().sent_to(), &[replica("remote")]);
    let outbound = target_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(outbound.from, replica("local"));
    assert!(!outbound.reply);
    assert_eq!(outbound.deltas.len(), 1);
    assert_eq!(outbound.deltas[0].key, key.as_str());

    let (tick_ref, tick_rx) = forward_ref::<DeltaPropagationTickReport>(&system, "tick-replies-2");
    replicator
        .tell(ReplicatorActorMsg::RunDeltaPropagation { reply_to: tick_ref })
        .unwrap();
    let report = tick_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(report.propagation_count(), 2);
    assert!(report.cleaned_delta_entries());
    assert!(report.transport().sent_to().is_empty());
    assert!(target_rx.recv_timeout(Duration::from_millis(50)).is_err());

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_schedules_delta_loop_ticks_with_manual_time() {
    let manual = ManualScheduler::new();
    let system = ActorSystem::builder("ddata-replicator-delta-loop-scheduled")
        .manual_scheduler(manual.clone())
        .build()
        .unwrap();
    let (target_ref, target_rx) =
        forward_ref::<ReplicatorDeltaPropagation>(&system, "delta-target");
    let mut transport = DeltaPropagationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(DeltaPropagationTarget::new(replica("remote"), target_ref));
    let delta_loop = DeltaPropagationLoop::new(transport).with_cleanup_every_ticks(5);
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GCounter>::with_delta_propagation_loop_interval(
                    delta_loop,
                    Duration::from_millis(25),
                )
            }),
        )
        .unwrap();
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");

    replicator
        .tell(ReplicatorActorMsg::SetDeltaNodes {
            nodes: vec![replica("remote")],
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key: ReplicatorKey::new("counter"),
            initial: GCounter::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(|counter| {
                counter
                    .increment(replica("local"), 8)
                    .map_err(|error| error.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();
    update_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    manual.advance(Duration::from_millis(24));
    assert!(target_rx.recv_timeout(Duration::from_millis(50)).is_err());
    manual.advance(Duration::from_millis(1));
    let outbound = target_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(outbound.from, replica("local"));
    assert_eq!(outbound.deltas.len(), 1);

    system.terminate(Duration::from_secs(1)).unwrap();
}
