use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use kairo_actor::{Actor, ActorResult, ActorSystem, Address, Context, ManualScheduler, Props};
use kairo_cluster::UniqueAddress;

use crate::{
    AggregationError, AggregationTarget, AggregationTransport, AggregationTransportFailure,
    AggregationTransportOperation, ConsistencyError, CrdtDataCodec, CrdtError, DataEnvelope,
    DeltaPropagationLog, DeltaPropagationLoop, DeltaPropagationTarget, DeltaPropagationTickReport,
    DeltaPropagationTransport, DeltaReceiveFailure, DeltaReceiveReply, DeltaReceiveStatus,
    DeltaReceiveTracker, DeltaReplicatedData, DeltaTransportFailure, DirectReadResult,
    DirectWriteResult, GCounter, GCounterCodec, GSet, GSetStringCodec, GetResponse, ORSet,
    PNCounter, PNCounterCodec, PruningPerformed, PruningState, ReadAggregationOutcome,
    ReadAggregationPlan, ReadAggregatorState, ReadConsistency, RemovedNodePruning,
    RemovedNodePruningTick, RemovedNodePruningTickReport, ReplicaId, ReplicatedData,
    ReplicatedDelta, ReplicatorActor, ReplicatorActorMsg, ReplicatorAggregation,
    ReplicatorClusterRouteReport, ReplicatorClusterRouteUpdate, ReplicatorDeltaPropagation,
    ReplicatorGossip, ReplicatorGossipReceiveReport, ReplicatorGossipStatus,
    ReplicatorGossipStatusReceiveReport, ReplicatorGossipTarget, ReplicatorGossipTickReport,
    ReplicatorGossipTransport, ReplicatorKey, ReplicatorState, UpdateResponse,
    WriteAggregationOutcome, WriteAggregationPlan, WriteAggregatorState, WriteConsistency,
    calculate_majority, decode_data_envelope, decode_delta_propagation, decode_read_result,
    encode_data_envelope, encode_delta_propagation, encode_read, encode_read_result, encode_write,
};

mod aggregation_core;
mod aggregation_transport;
mod aggregation_wire;
mod crdt_codecs;
mod crdt_foundation;
mod delta_log;
mod delta_receive_tracker;
mod delta_transport;
mod delta_wire;
mod direct_receive;
mod replicator_actor_client;
mod replicator_actor_delta_loop;
mod replicator_actor_gossip;
mod replicator_actor_pruning;
mod replicator_state;

fn replica(id: &str) -> ReplicaId {
    ReplicaId::new(id)
}

fn delta_counter(id: &str, amount: u128) -> GCounter {
    GCounter::new()
        .increment(replica(id), amount)
        .unwrap()
        .delta()
        .unwrap()
}

fn full_counter(id: &str, amount: u128) -> GCounter {
    delta_counter(id, amount).reset_delta()
}

struct Forward<M> {
    tx: mpsc::Sender<M>,
}

impl<M> Actor for Forward<M>
where
    M: Send + 'static,
{
    type Msg = M;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.tx
            .send(msg)
            .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
    }
}

fn forward_ref<M>(system: &ActorSystem, name: &str) -> (kairo_actor::ActorRef<M>, mpsc::Receiver<M>)
where
    M: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let actor = system
        .spawn(name, Props::new(move || Forward { tx }))
        .expect("forward actor should spawn");
    (actor, rx)
}

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

#[test]
fn replicator_actor_applies_remote_write_and_serves_remote_read_messages() {
    let system = ActorSystem::builder("ddata-replicator-direct-read-write")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (write_result_ref, write_result_rx) = forward_ref(&system, "write-results");
    let (read_result_ref, read_result_rx) =
        forward_ref::<Result<DirectReadResult, String>>(&system, "read-results");
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(remote.clone(), 8)
            .unwrap()
            .reset_delta(),
    );
    let write = encode_write(&key, Some(remote.clone()), &envelope, &GCounterCodec).unwrap();
    let write_codec: Arc<dyn CrdtDataCodec<GCounter> + Send + Sync> = Arc::new(GCounterCodec);
    let read_codec: Arc<dyn CrdtDataCodec<GCounter> + Send + Sync> = Arc::new(GCounterCodec);

    replicator
        .tell(ReplicatorActorMsg::ApplyWrite {
            write,
            codec: write_codec,
            reply_to: write_result_ref,
        })
        .unwrap();
    assert!(matches!(
        write_result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        DirectWriteResult::Ack { changed: true, .. }
    ));

    replicator
        .tell(ReplicatorActorMsg::ServeRead {
            read: encode_read(&key, Some(remote.clone())),
            codec: read_codec,
            reply_to: read_result_ref,
        })
        .unwrap();

    let read_result = read_result_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(read_result.key(), &key);
    assert_eq!(read_result.from(), Some(&remote));
    assert_eq!(
        decode_read_result(read_result.message(), &GCounterCodec)
            .unwrap()
            .unwrap()
            .data()
            .value()
            .unwrap(),
        8
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_nacks_remote_write_decode_failures() {
    let system = ActorSystem::builder("ddata-replicator-direct-write-nack")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (write_result_ref, write_result_rx) = forward_ref(&system, "write-results");
    let (get_ref, get_rx) = forward_ref(&system, "get-replies");
    let key = ReplicatorKey::new("counter");
    let write = crate::ReplicatorWrite {
        key: key.as_str().to_string(),
        from: Some(replica("remote")),
        envelope: crate::ReplicatorDataEnvelope {
            crdt_manifest: crate::GSET_STRING_MANIFEST.to_string(),
            crdt_version: crate::CRDT_CODEC_VERSION,
            payload: bytes::Bytes::new(),
            pruning: Vec::new(),
        },
    };
    let codec: Arc<dyn CrdtDataCodec<GCounter> + Send + Sync> = Arc::new(GCounterCodec);

    replicator
        .tell(ReplicatorActorMsg::ApplyWrite {
            write,
            codec,
            reply_to: write_result_ref,
        })
        .unwrap();
    assert!(matches!(
        write_result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        DirectWriteResult::Nack { reason, .. } if reason.contains("expected CRDT manifest")
    ));

    replicator
        .tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::local(),
            reply_to: get_ref,
        })
        .unwrap();
    assert_eq!(
        get_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        GetResponse::NotFound { key }
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}

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
