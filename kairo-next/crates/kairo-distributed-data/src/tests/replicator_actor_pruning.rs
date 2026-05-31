use super::*;

#[test]
fn replicator_actor_runs_removed_node_pruning_tick_after_seen_marker() {
    let system = ActorSystem::builder("ddata-replicator-pruning")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let key = ReplicatorKey::new("counter");
    let self_replica = replica("self");
    let peer = replica("peer");
    let removed = replica("removed");
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(self_replica.clone(), 2)
            .unwrap()
            .increment(removed.clone(), 4)
            .unwrap()
            .reset_delta(),
    );
    let (pruning_ref, pruning_rx) =
        forward_ref::<RemovedNodePruningTickReport>(&system, "pruning-reports");
    let (seen_ref, seen_rx) = forward_ref::<BTreeSet<ReplicatorKey>>(&system, "seen-reports");
    let (read_ref, read_rx) =
        forward_ref::<Result<DirectReadResult, String>>(&system, "read-results");

    replicator
        .tell(ReplicatorActorMsg::WriteFull {
            key: key.clone(),
            envelope,
        })
        .unwrap();

    replicator
        .tell(ReplicatorActorMsg::RunRemovedNodePruning {
            tick: pruning_tick(self_replica.clone(), [peer.clone()], 0, 10, 1_000, true),
            reply_to: pruning_ref.clone(),
        })
        .unwrap();
    let first = pruning_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(first.collected_removed, BTreeSet::from([removed.clone()]));
    assert!(first.initialized.is_empty());
    assert!(first.performed.is_empty());

    replicator
        .tell(ReplicatorActorMsg::RunRemovedNodePruning {
            tick: pruning_tick(self_replica.clone(), [peer.clone()], 11, 10, 1_000, true),
            reply_to: pruning_ref.clone(),
        })
        .unwrap();
    let initialized = pruning_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(initialized.initialized, BTreeSet::from([key.clone()]));
    assert!(initialized.performed.is_empty());

    replicator
        .tell(ReplicatorActorMsg::MarkRemovedNodePruningSeen {
            seen_by: peer.clone(),
            reply_to: seen_ref,
        })
        .unwrap();
    assert_eq!(
        seen_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        BTreeSet::from([key.clone()])
    );

    replicator
        .tell(ReplicatorActorMsg::RunRemovedNodePruning {
            tick: pruning_tick(self_replica.clone(), [peer], 12, 10, 1_000, true),
            reply_to: pruning_ref,
        })
        .unwrap();
    let performed = pruning_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(performed.performed, BTreeSet::from([key.clone()]));
    assert!(performed.failures.is_empty());

    replicator
        .tell(ReplicatorActorMsg::ServeRead {
            read: encode_read(&key, None),
            codec: Arc::new(GCounterCodec),
            reply_to: read_ref,
        })
        .unwrap();
    let read_result = read_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    let decoded = decode_read_result(read_result.message(), &GCounterCodec)
        .unwrap()
        .unwrap();
    assert_eq!(decoded.data().replica_value(&removed), 0);
    assert_eq!(decoded.data().replica_value(&self_replica), 6);
    assert_eq!(
        decoded.pruning().get(&removed),
        Some(&PruningState::Performed(PruningPerformed::new(1_100)))
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}

fn pruning_tick(
    self_replica: ReplicaId,
    live_replicas: impl IntoIterator<Item = ReplicaId>,
    all_reachable_time_nanos: u64,
    max_pruning_dissemination_nanos: u64,
    now_millis: u64,
    is_leader: bool,
) -> RemovedNodePruningTick {
    RemovedNodePruningTick {
        self_replica,
        live_replicas: live_replicas.into_iter().collect(),
        unreachable_replicas: BTreeSet::new(),
        all_reachable_time_nanos,
        max_pruning_dissemination_nanos,
        now_millis,
        pruning_marker_ttl_millis: 100,
        is_leader,
    }
}
