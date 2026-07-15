use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};

use crate::gossip::{apply_gossip, apply_gossip_with_seen};
use crate::{
    AggregationError, CrdtDataCodec, DataEnvelope, DeltaPropagation, DeltaPropagationLog,
    DeltaPropagationLoop, DeltaPropagationReceiveReport, DeltaPropagationTickReport,
    DeltaReceiveStatus, DeltaReceiveTracker, DeltaReplicatedData, DirectReadResult,
    DirectWriteResult, GetResponse, ReadAggregationPlan, ReadAggregatorState, ReadConsistency,
    RemovedNodePruning, RemovedNodePruningTick, RemovedNodePruningTickReport,
    RemovedNodePruningTracker, ReplicaId, ReplicatedDelta, ReplicatorAggregation, ReplicatorChange,
    ReplicatorClusterRouteReport, ReplicatorClusterRouteUpdate, ReplicatorDeltaPropagation,
    ReplicatorGossip, ReplicatorGossipReceiveReport, ReplicatorGossipStatus,
    ReplicatorGossipStatusReceiveReport, ReplicatorGossipTickReport,
    ReplicatorGossipTickSkipReason, ReplicatorGossipTransport, ReplicatorGossipTransportReport,
    ReplicatorKey, ReplicatorRead, ReplicatorState, ReplicatorWrite, UpdateResponse,
    WriteAggregationPlan, WriteAggregatorState, WriteConsistency, build_gossip_status,
    respond_to_gossip_status,
};

mod client_ops;
mod cluster_ops;
mod construction;
mod delta_ops;
mod gossip_ops;
mod pruning_ops;

pub struct ReplicatorActor<D>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    state: ReplicatorState<D>,
    delta_log: DeltaPropagationLog<D::Delta>,
    delta_receive: DeltaReceiveTracker,
    subscribers: BTreeMap<ReplicatorKey, Vec<ActorRef<ReplicatorChange<D>>>>,
    remote_nodes: Vec<ReplicaId>,
    unreachable_nodes: BTreeSet<ReplicaId>,
    remote_replica_count: usize,
    aggregation: Option<ReplicatorAggregation<D>>,
    delta_loop: Option<DeltaPropagationLoop<D::Delta>>,
    delta_tick_interval: Option<Duration>,
    gossip_transport: Option<ReplicatorGossipTransport>,
    gossip_codec: Option<Arc<dyn CrdtDataCodec<D> + Send + Sync>>,
    gossip_tick_interval: Option<Duration>,
    gossip_max_entries: usize,
    gossip_next_index: usize,
    gossip_next_chunk: u32,
    self_system_uid: Option<u64>,
    self_replica: Option<ReplicaId>,
    removed_node_pruning: RemovedNodePruningTracker,
}

impl<D> Default for ReplicatorActor<D>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

pub enum ReplicatorActorMsg<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    Get {
        key: ReplicatorKey,
        consistency: ReadConsistency,
        reply_to: ActorRef<GetResponse<D>>,
    },
    Update {
        key: ReplicatorKey,
        initial: D,
        consistency: WriteConsistency,
        modify: Box<dyn FnOnce(D) -> Result<D, String> + Send>,
        reply_to: ActorRef<UpdateResponse<D::Delta>>,
    },
    WriteFull {
        key: ReplicatorKey,
        envelope: DataEnvelope<D>,
    },
    ApplyReadRepair {
        key: ReplicatorKey,
        envelope: DataEnvelope<D>,
        reply_to: ActorRef<()>,
    },
    WriteDelta {
        key: ReplicatorKey,
        delta: D::Delta,
    },
    WriteCausalDelta {
        from: ReplicaId,
        key: ReplicatorKey,
        from_version: u64,
        to_version: u64,
        delta: D::Delta,
        reply_to: ActorRef<DeltaReceiveStatus>,
    },
    ApplyDeltaPropagation {
        propagation: ReplicatorDeltaPropagation,
        codec: Arc<dyn CrdtDataCodec<D::Delta> + Send + Sync>,
        reply_to: ActorRef<DeltaPropagationReceiveReport>,
    },
    ApplyWrite {
        write: ReplicatorWrite,
        codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
        reply_to: ActorRef<DirectWriteResult>,
    },
    ServeRead {
        read: ReplicatorRead,
        codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
        reply_to: ActorRef<Result<DirectReadResult, String>>,
    },
    SetRemoteReplicas {
        nodes: Vec<ReplicaId>,
        unreachable: BTreeSet<ReplicaId>,
    },
    PlanRead {
        key: ReplicatorKey,
        consistency: ReadConsistency,
        reply_to: ActorRef<Result<ReadAggregationPlan<D>, AggregationError>>,
    },
    PlanWrite {
        key: ReplicatorKey,
        consistency: WriteConsistency,
        reply_to: ActorRef<Result<WriteAggregationPlan, AggregationError>>,
    },
    SetDeltaNodes {
        nodes: Vec<ReplicaId>,
    },
    ApplyClusterRouteUpdate {
        update: ReplicatorClusterRouteUpdate,
        all_reachable_time_nanos: u64,
        reply_to: ActorRef<ReplicatorClusterRouteReport>,
    },
    CollectDeltaPropagations {
        reply_to: ActorRef<BTreeMap<ReplicaId, DeltaPropagation<D::Delta>>>,
    },
    CleanupDeltaEntries,
    RunDeltaPropagation {
        reply_to: ActorRef<DeltaPropagationTickReport>,
    },
    DeltaPropagationTick,
    RunGossip {
        reply_to: Option<ActorRef<ReplicatorGossipTickReport>>,
    },
    GossipTick,
    ReceiveGossipStatus {
        from: ReplicaId,
        status: ReplicatorGossipStatus,
        codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
        reply_to: Option<ActorRef<ReplicatorGossipStatusReceiveReport>>,
    },
    ReceiveGossip {
        from: ReplicaId,
        gossip: ReplicatorGossip,
        codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
        reply_to: Option<ActorRef<ReplicatorGossipReceiveReport>>,
    },
    MarkRemovedNodePruningSeen {
        seen_by: ReplicaId,
        reply_to: ActorRef<BTreeSet<ReplicatorKey>>,
    },
    RunRemovedNodePruning {
        tick: RemovedNodePruningTick,
        reply_to: ActorRef<RemovedNodePruningTickReport>,
    },
    Subscribe {
        key: ReplicatorKey,
        subscriber: ActorRef<ReplicatorChange<D>>,
    },
    Unsubscribe {
        key: ReplicatorKey,
        subscriber: ActorRef<ReplicatorChange<D>>,
    },
    FlushChanges,
}

impl<D> Actor for ReplicatorActor<D>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    type Msg = ReplicatorActorMsg<D>;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.schedule_delta_propagation_tick(ctx);
        self.schedule_gossip_tick(ctx);
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ReplicatorActorMsg::Get {
                key,
                consistency,
                reply_to,
            } => self.handle_get(ctx, key, consistency, reply_to)?,
            ReplicatorActorMsg::Update {
                key,
                initial,
                consistency,
                modify,
                reply_to,
            } => self.handle_update(ctx, key, initial, consistency, modify, reply_to)?,
            ReplicatorActorMsg::WriteFull { key, envelope } => {
                self.state.write_full(key, envelope);
            }
            ReplicatorActorMsg::ApplyReadRepair {
                key,
                envelope,
                reply_to,
            } => {
                self.state
                    .write_full_pruned(key.clone(), envelope, wall_millis());
                if let Some(self_replica) = &self.self_replica {
                    self.state.mark_key_pruning_seen(&key, self_replica.clone());
                }
                tell_or_actor_error(&reply_to, ())?;
            }
            ReplicatorActorMsg::WriteDelta { key, delta } => {
                self.state.write_delta(key, delta);
            }
            ReplicatorActorMsg::WriteCausalDelta {
                from,
                key,
                from_version,
                to_version,
                delta,
                reply_to,
            } => {
                let status = self.delta_receive.apply_delta(
                    &mut self.state,
                    from,
                    key,
                    from_version,
                    to_version,
                    delta,
                );
                tell_or_actor_error(&reply_to, status)?;
            }
            ReplicatorActorMsg::ApplyDeltaPropagation {
                propagation,
                codec,
                reply_to,
            } => {
                let source_removed = self.removed_node_pruning.contains(&propagation.from)
                    || propagation.deltas.iter().any(|delta| {
                        self.state
                            .envelope(&ReplicatorKey::new(delta.key.clone()))
                            .is_some_and(|envelope| {
                                envelope.pruning().get(&propagation.from).is_some()
                            })
                    });
                let report = if source_removed {
                    DeltaPropagationReceiveReport::ignored(propagation.from, propagation.reply)
                } else if let Some(self_replica) = &self.self_replica {
                    self.delta_receive.apply_propagation_with_seen(
                        &mut self.state,
                        &propagation,
                        codec.as_ref(),
                        self_replica,
                    )
                } else {
                    self.delta_receive.apply_propagation(
                        &mut self.state,
                        &propagation,
                        codec.as_ref(),
                    )
                };
                tell_or_actor_error(&reply_to, report)?;
            }
            ReplicatorActorMsg::ApplyWrite {
                write,
                codec,
                reply_to,
            } => {
                let result = if let Some(self_replica) = &self.self_replica {
                    crate::read_write_receive::apply_write_with_seen(
                        &mut self.state,
                        &write,
                        codec.as_ref(),
                        self_replica,
                    )
                } else {
                    crate::apply_write(&mut self.state, &write, codec.as_ref())
                };
                tell_or_actor_error(&reply_to, result)?;
            }
            ReplicatorActorMsg::ServeRead {
                read,
                codec,
                reply_to,
            } => {
                let result =
                    crate::serve_read(&self.state, &read, codec.as_ref()).map_err(|error| {
                        format!("failed to encode read result for key {}: {error}", read.key)
                    });
                tell_or_actor_error(&reply_to, result)?;
            }
            ReplicatorActorMsg::SetRemoteReplicas { nodes, unreachable } => {
                self.remote_replica_count = nodes.len();
                self.remote_nodes = nodes;
                self.unreachable_nodes = unreachable;
            }
            ReplicatorActorMsg::PlanRead {
                key,
                consistency,
                reply_to,
            } => {
                let response = self.plan_read(key, &consistency);
                tell_or_actor_error(&reply_to, response)?;
            }
            ReplicatorActorMsg::PlanWrite {
                key,
                consistency,
                reply_to,
            } => {
                let response = self.plan_write(key, &consistency);
                tell_or_actor_error(&reply_to, response)?;
            }
            ReplicatorActorMsg::SetDeltaNodes { nodes } => {
                self.delta_log.set_nodes(nodes);
            }
            ReplicatorActorMsg::ApplyClusterRouteUpdate {
                update,
                all_reachable_time_nanos,
                reply_to,
            } => {
                let report = self.apply_cluster_route_update(update, all_reachable_time_nanos);
                tell_or_actor_error(&reply_to, report)?;
            }
            ReplicatorActorMsg::CollectDeltaPropagations { reply_to } => {
                let mut propagations = self.delta_log.collect_propagations();
                for propagation in propagations.values_mut() {
                    propagation.attach_pruning(|key| {
                        self.state
                            .envelope(key)
                            .map(|envelope| envelope.pruning().clone())
                            .unwrap_or_default()
                    });
                }
                tell_or_actor_error(&reply_to, propagations)?;
            }
            ReplicatorActorMsg::CleanupDeltaEntries => {
                self.delta_log.cleanup_delta_entries();
            }
            ReplicatorActorMsg::RunDeltaPropagation { reply_to } => {
                let report = self.run_delta_propagation_tick();
                tell_or_actor_error(&reply_to, report)?;
            }
            ReplicatorActorMsg::DeltaPropagationTick => {
                self.run_delta_propagation_tick();
                self.schedule_delta_propagation_tick(ctx);
            }
            ReplicatorActorMsg::RunGossip { reply_to } => {
                let report = self.run_gossip_tick()?;
                if let Some(reply_to) = reply_to {
                    tell_or_actor_error(&reply_to, report)?;
                }
            }
            ReplicatorActorMsg::GossipTick => {
                self.run_gossip_tick()?;
                self.schedule_gossip_tick(ctx);
            }
            ReplicatorActorMsg::ReceiveGossipStatus {
                from,
                status,
                codec,
                reply_to,
            } => {
                let report = self.receive_gossip_status(from, &status, codec.as_ref())?;
                if let Some(reply_to) = reply_to {
                    tell_or_actor_error(&reply_to, report)?;
                }
            }
            ReplicatorActorMsg::ReceiveGossip {
                from,
                gossip,
                codec,
                reply_to,
            } => {
                let report = self.receive_gossip(from, &gossip, codec.as_ref())?;
                if let Some(reply_to) = reply_to {
                    tell_or_actor_error(&reply_to, report)?;
                }
            }
            ReplicatorActorMsg::MarkRemovedNodePruningSeen { seen_by, reply_to } => {
                let changed = self.state.mark_pruning_seen(seen_by);
                tell_or_actor_error(&reply_to, changed)?;
            }
            ReplicatorActorMsg::RunRemovedNodePruning { tick, reply_to } => {
                let report = self.run_removed_node_pruning_tick(tick);
                tell_or_actor_error(&reply_to, report)?;
            }
            ReplicatorActorMsg::Subscribe { key, subscriber } => {
                self.subscribe(key, subscriber);
            }
            ReplicatorActorMsg::Unsubscribe { key, subscriber } => {
                self.unsubscribe(&key, &subscriber);
            }
            ReplicatorActorMsg::FlushChanges => {
                self.flush_changes();
            }
        }
        Ok(())
    }
}

fn tell_or_actor_error<M>(target: &ActorRef<M>, message: M) -> ActorResult
where
    M: Send + 'static,
{
    target
        .tell(message)
        .map_err(|error| ActorError::Message(error.reason().to_string()))
}

fn wall_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
