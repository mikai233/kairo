use super::*;

#[test]
fn replicator_actor_plans_remote_read_with_local_value_and_reachable_first_targets() {
    let system = ActorSystem::builder("ddata-replicator-plan-read")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let (plan_ref, plan_rx) = forward_ref::<Result<ReadAggregationPlan<GCounter>, AggregationError>>(
        &system,
        "read-plans",
    );
    let key = ReplicatorKey::new("counter");

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![replica("a"), replica("b"), replica("c")],
            unreachable: BTreeSet::from([replica("b")]),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GCounter::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(|counter| {
                counter
                    .increment(replica("local"), 4)
                    .map_err(|e| e.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();
    update_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    replicator
        .tell(ReplicatorActorMsg::PlanRead {
            key: key.clone(),
            consistency: ReadConsistency::majority(Duration::from_secs(1)),
            reply_to: plan_ref,
        })
        .unwrap();

    let plan = plan_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(plan.state().key(), &key);
    assert_eq!(plan.state().required_remote_reads(), 2);
    assert_eq!(plan.selection().primary(), &[replica("a"), replica("c")]);
    assert_eq!(plan.selection().secondary(), &[replica("b")]);

    let mut state = plan.into_state();
    assert!(matches!(
        state.record_read(Some(DataEnvelope::new(
            GCounter::new()
                .increment(replica("a"), 3)
                .unwrap()
                .reset_delta()
        ))),
        ReadAggregationOutcome::InProgress
    ));
    match state.record_read(None) {
        ReadAggregationOutcome::Success { envelope } => {
            assert_eq!(envelope.data().value().unwrap(), 7);
        }
        other => panic!("expected read success, got {other:?}"),
    }

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_plans_remote_write_and_reports_quorum_errors() {
    let system = ActorSystem::builder("ddata-replicator-plan-write")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (plan_ref, plan_rx) =
        forward_ref::<Result<WriteAggregationPlan, AggregationError>>(&system, "write-plans");
    let (error_ref, error_rx) =
        forward_ref::<Result<WriteAggregationPlan, AggregationError>>(&system, "write-errors");
    let key = ReplicatorKey::new("counter");

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![replica("a"), replica("b")],
            unreachable: BTreeSet::new(),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::PlanWrite {
            key: key.clone(),
            consistency: WriteConsistency::to(3, Duration::from_secs(1)).unwrap(),
            reply_to: plan_ref,
        })
        .unwrap();

    let plan = plan_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(plan.state().key(), &key);
    assert_eq!(plan.state().required_remote_acks(), 2);
    assert_eq!(plan.selection().primary(), &[replica("a"), replica("b")]);
    assert!(plan.selection().secondary().is_empty());

    replicator
        .tell(ReplicatorActorMsg::PlanWrite {
            key,
            consistency: WriteConsistency::to(4, Duration::from_secs(1)).unwrap(),
            reply_to: error_ref,
        })
        .unwrap();
    assert_eq!(
        error_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .expect_err("insufficient remote replicas should fail"),
        AggregationError::NotEnoughReplicas {
            required: 3,
            available: 2,
        }
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_applies_cluster_route_updates_to_remote_plans() {
    let system = ActorSystem::builder("ddata-replicator-cluster-routes")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (route_ref, route_rx) =
        forward_ref::<ReplicatorClusterRouteReport>(&system, "route-replies");
    let (plan_ref, plan_rx) =
        forward_ref::<Result<WriteAggregationPlan, AggregationError>>(&system, "route-plans");

    replicator
        .tell(ReplicatorActorMsg::ApplyClusterRouteUpdate {
            update: ReplicatorClusterRouteUpdate::new(
                [replica("c"), replica("a"), replica("b")],
                [replica("b")],
                [replica("removed")],
                true,
            ),
            all_reachable_time_nanos: 42,
            reply_to: route_ref,
        })
        .unwrap();

    let report = route_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(
        report.remote_replicas,
        vec![replica("a"), replica("b"), replica("c")]
    );
    assert_eq!(report.unreachable_replicas, BTreeSet::from([replica("b")]));
    assert_eq!(
        report.recorded_removed,
        BTreeSet::from([replica("removed")])
    );

    replicator
        .tell(ReplicatorActorMsg::PlanWrite {
            key: ReplicatorKey::new("counter"),
            consistency: WriteConsistency::majority(Duration::from_secs(1)),
            reply_to: plan_ref,
        })
        .unwrap();

    let plan = plan_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(plan.selection().primary(), &[replica("a"), replica("c")]);
    assert_eq!(plan.selection().secondary(), &[replica("b")]);

    system.terminate(Duration::from_secs(1)).unwrap();
}
