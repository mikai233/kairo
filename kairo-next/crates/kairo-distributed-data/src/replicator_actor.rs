use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};

use crate::{
    AggregationError, CrdtDataCodec, DataEnvelope, DeltaPropagation, DeltaPropagationLog,
    DeltaPropagationReceiveReport, DeltaReceiveStatus, DeltaReceiveTracker, DeltaReplicatedData,
    GetResponse, ReadAggregationPlan, ReadAggregatorState, ReadConsistency, ReplicaId,
    ReplicatorChange, ReplicatorDeltaPropagation, ReplicatorKey, ReplicatorState, UpdateResponse,
    WriteAggregationPlan, WriteAggregatorState, WriteConsistency,
};

pub struct ReplicatorActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    state: ReplicatorState<D>,
    delta_log: DeltaPropagationLog<D::Delta>,
    delta_receive: DeltaReceiveTracker,
    subscribers: BTreeMap<ReplicatorKey, Vec<ActorRef<ReplicatorChange<D>>>>,
    remote_nodes: Vec<ReplicaId>,
    unreachable_nodes: BTreeSet<ReplicaId>,
    remote_replica_count: usize,
}

impl<D> ReplicatorActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    pub fn new() -> Self {
        Self::with_remote_replica_count(0)
    }

    pub fn with_remote_replica_count(remote_replica_count: usize) -> Self {
        Self {
            state: ReplicatorState::new(),
            delta_log: DeltaPropagationLog::new([]),
            delta_receive: DeltaReceiveTracker::new(),
            subscribers: BTreeMap::new(),
            remote_nodes: Vec::new(),
            unreachable_nodes: BTreeSet::new(),
            remote_replica_count,
        }
    }

    pub fn state(&self) -> &ReplicatorState<D> {
        &self.state
    }

    pub fn delta_log(&self) -> &DeltaPropagationLog<D::Delta> {
        &self.delta_log
    }

    pub fn delta_receive(&self) -> &DeltaReceiveTracker {
        &self.delta_receive
    }

    pub fn remote_nodes(&self) -> &[ReplicaId] {
        &self.remote_nodes
    }
}

impl<D> Default for ReplicatorActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
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
    CollectDeltaPropagations {
        reply_to: ActorRef<BTreeMap<ReplicaId, DeltaPropagation<D::Delta>>>,
    },
    CleanupDeltaEntries,
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
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    type Msg = ReplicatorActorMsg<D>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ReplicatorActorMsg::Get {
                key,
                consistency,
                reply_to,
            } => {
                let response = if consistency.is_local(self.effective_remote_replica_count()) {
                    self.state.get_local(&key)
                } else {
                    GetResponse::Failure {
                        key,
                        reason: "non-local read aggregation is not wired yet".to_string(),
                    }
                };
                tell_or_actor_error(&reply_to, response)?;
            }
            ReplicatorActorMsg::Update {
                key,
                initial,
                consistency,
                modify,
                reply_to,
            } => {
                let response = match self.state.update_local(key.clone(), initial, modify) {
                    Ok(outcome) => {
                        self.delta_log
                            .record_delta(key.clone(), outcome.delta().cloned());
                        if consistency.is_local(self.effective_remote_replica_count()) {
                            UpdateResponse::Success(outcome)
                        } else {
                            UpdateResponse::Timeout { key }
                        }
                    }
                    Err(reason) => UpdateResponse::ModifyFailure { key, reason },
                };
                tell_or_actor_error(&reply_to, response)?;
            }
            ReplicatorActorMsg::WriteFull { key, envelope } => {
                self.state.write_full(key, envelope);
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
                let report = self.delta_receive.apply_propagation(
                    &mut self.state,
                    &propagation,
                    codec.as_ref(),
                );
                tell_or_actor_error(&reply_to, report)?;
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
            ReplicatorActorMsg::CollectDeltaPropagations { reply_to } => {
                let propagations = self.delta_log.collect_propagations();
                tell_or_actor_error(&reply_to, propagations)?;
            }
            ReplicatorActorMsg::CleanupDeltaEntries => {
                self.delta_log.cleanup_delta_entries();
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

impl<D> ReplicatorActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    fn subscribe(&mut self, key: ReplicatorKey, subscriber: ActorRef<ReplicatorChange<D>>) {
        let current = self.state.get_local(&key).data().cloned();
        if let Some(data) = current {
            let change = ReplicatorChange::new(key.clone(), data);
            if subscriber.tell(change).is_err() {
                return;
            }
        }

        let subscribers = self.subscribers.entry(key).or_default();
        if subscribers
            .iter()
            .all(|existing| existing.path() != subscriber.path())
        {
            subscribers.push(subscriber);
        }
    }

    fn unsubscribe(&mut self, key: &ReplicatorKey, subscriber: &ActorRef<ReplicatorChange<D>>) {
        if let Some(subscribers) = self.subscribers.get_mut(key) {
            subscribers.retain(|existing| existing.path() != subscriber.path());
            if subscribers.is_empty() {
                self.subscribers.remove(key);
            }
        }
    }

    fn flush_changes(&mut self) {
        for change in self.state.flush_changes() {
            if let Some(subscribers) = self.subscribers.get_mut(change.key()) {
                subscribers.retain(|subscriber| subscriber.tell(change.clone()).is_ok());
            }
        }
        self.subscribers
            .retain(|_, subscribers| !subscribers.is_empty());
    }

    fn plan_read(
        &self,
        key: ReplicatorKey,
        consistency: &ReadConsistency,
    ) -> Result<ReadAggregationPlan<D>, AggregationError> {
        let state = ReadAggregatorState::new(
            key.clone(),
            consistency,
            self.remote_nodes.clone(),
            self.state.envelope(&key).cloned(),
        )?;
        let selection = state.select_replicas(&self.unreachable_nodes);
        Ok(ReadAggregationPlan::new(state, selection))
    }

    fn plan_write(
        &self,
        key: ReplicatorKey,
        consistency: &WriteConsistency,
    ) -> Result<WriteAggregationPlan, AggregationError> {
        let state = WriteAggregatorState::new(key, consistency, self.remote_nodes.clone())?;
        let selection = state.select_replicas(&self.unreachable_nodes);
        Ok(WriteAggregationPlan::new(state, selection))
    }

    fn effective_remote_replica_count(&self) -> usize {
        if self.remote_nodes.is_empty() {
            self.remote_replica_count
        } else {
            self.remote_nodes.len()
        }
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
