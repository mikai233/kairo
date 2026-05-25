use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};

use crate::{
    AggregationError, CrdtDataCodec, DataEnvelope, DeltaPropagation, DeltaPropagationLog,
    DeltaPropagationLoop, DeltaPropagationReceiveReport, DeltaPropagationTickReport,
    DeltaReceiveStatus, DeltaReceiveTracker, DeltaReplicatedData, DirectReadResult,
    DirectWriteResult, GetResponse, ReadAggregationPlan, ReadAggregatorState, ReadConsistency,
    RemovedNodePruning, RemovedNodePruningTick, RemovedNodePruningTickReport,
    RemovedNodePruningTracker, ReplicaId, ReplicatedDelta, ReplicatorAggregation, ReplicatorChange,
    ReplicatorClusterRouteReport, ReplicatorClusterRouteUpdate, ReplicatorDeltaPropagation,
    ReplicatorKey, ReplicatorRead, ReplicatorState, ReplicatorWrite, UpdateResponse,
    WriteAggregationPlan, WriteAggregatorState, WriteConsistency,
};

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
    removed_node_pruning: RemovedNodePruningTracker,
}

impl<D> ReplicatorActor<D>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
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
            aggregation: None,
            delta_loop: None,
            delta_tick_interval: None,
            removed_node_pruning: RemovedNodePruningTracker::new(),
        }
    }

    pub fn with_aggregation(aggregation: ReplicatorAggregation<D>) -> Self
    where
        D::Delta: ReplicatedDelta + Send + 'static,
    {
        let mut actor = Self::new();
        actor.aggregation = Some(aggregation);
        actor
    }

    pub fn with_delta_propagation_loop(delta_loop: DeltaPropagationLoop<D::Delta>) -> Self {
        let mut actor = Self::new();
        actor.delta_loop = Some(delta_loop);
        actor
    }

    pub fn with_delta_propagation_loop_interval(
        delta_loop: DeltaPropagationLoop<D::Delta>,
        interval: Duration,
    ) -> Self {
        let mut actor = Self::with_delta_propagation_loop(delta_loop);
        actor.delta_tick_interval = Some(interval);
        actor
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
            ReplicatorActorMsg::ApplyWrite {
                write,
                codec,
                reply_to,
            } => {
                let result = crate::apply_write(&mut self.state, &write, codec.as_ref());
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
                let propagations = self.delta_log.collect_propagations();
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

impl<D> ReplicatorActor<D>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    fn handle_get(
        &mut self,
        ctx: &Context<ReplicatorActorMsg<D>>,
        key: ReplicatorKey,
        consistency: ReadConsistency,
        reply_to: ActorRef<GetResponse<D>>,
    ) -> ActorResult {
        if consistency.is_local(self.effective_remote_replica_count()) {
            tell_or_actor_error(&reply_to, self.state.get_local(&key))?;
            return Ok(());
        }

        let Some(aggregation) = &self.aggregation else {
            tell_or_actor_error(
                &reply_to,
                GetResponse::Failure {
                    key,
                    reason: "non-local read aggregation is not configured".to_string(),
                },
            )?;
            return Ok(());
        };

        let timeout = consistency
            .timeout()
            .expect("non-local read consistency always carries a timeout");
        match self.plan_read(key.clone(), &consistency) {
            Ok(plan) => {
                aggregation.spawn_read(ctx, plan, timeout, reply_to)?;
            }
            Err(error) => {
                tell_or_actor_error(
                    &reply_to,
                    GetResponse::Failure {
                        key,
                        reason: format!("failed to plan read aggregation: {error:?}"),
                    },
                )?;
            }
        }
        Ok(())
    }

    fn handle_update(
        &mut self,
        ctx: &Context<ReplicatorActorMsg<D>>,
        key: ReplicatorKey,
        initial: D,
        consistency: WriteConsistency,
        modify: Box<dyn FnOnce(D) -> Result<D, String> + Send>,
        reply_to: ActorRef<UpdateResponse<D::Delta>>,
    ) -> ActorResult {
        let outcome = match self.state.update_local(key.clone(), initial, modify) {
            Ok(outcome) => outcome,
            Err(reason) => {
                tell_or_actor_error(&reply_to, UpdateResponse::ModifyFailure { key, reason })?;
                return Ok(());
            }
        };

        self.delta_log
            .record_delta(key.clone(), outcome.delta().cloned());

        if consistency.is_local(self.effective_remote_replica_count()) {
            tell_or_actor_error(&reply_to, UpdateResponse::Success(outcome))?;
            return Ok(());
        }

        let Some(aggregation) = &self.aggregation else {
            tell_or_actor_error(&reply_to, UpdateResponse::Timeout { key })?;
            return Ok(());
        };

        let timeout = consistency
            .timeout()
            .expect("non-local write consistency always carries a timeout");
        let envelope = match self.state.envelope(&key).cloned() {
            Some(envelope) => envelope,
            None => {
                tell_or_actor_error(
                    &reply_to,
                    UpdateResponse::Failure {
                        key,
                        reason: "local update did not leave state to replicate".to_string(),
                    },
                )?;
                return Ok(());
            }
        };

        match self.plan_write(key.clone(), &consistency) {
            Ok(plan) => {
                aggregation.spawn_write(ctx, plan, envelope, outcome, timeout, reply_to)?;
            }
            Err(error) => {
                tell_or_actor_error(
                    &reply_to,
                    UpdateResponse::Failure {
                        key,
                        reason: format!("failed to plan write aggregation: {error:?}"),
                    },
                )?;
            }
        }
        Ok(())
    }

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

    fn apply_cluster_route_update(
        &mut self,
        update: ReplicatorClusterRouteUpdate,
        all_reachable_time_nanos: u64,
    ) -> ReplicatorClusterRouteReport {
        self.remote_replica_count = update.remote_replicas.len();
        self.remote_nodes = update.remote_replicas;
        self.unreachable_nodes = update.unreachable_replicas;
        self.delta_log.set_nodes(self.remote_nodes.clone());

        let mut recorded_removed = BTreeSet::new();
        for replica in update.removed_replicas {
            self.delta_log.cleanup_removed_node(&replica);
            if self
                .removed_node_pruning
                .record_removed(replica.clone(), all_reachable_time_nanos)
            {
                recorded_removed.insert(replica);
            }
        }

        ReplicatorClusterRouteReport {
            remote_replicas: self.remote_nodes.clone(),
            unreachable_replicas: self.unreachable_nodes.clone(),
            recorded_removed,
        }
    }

    fn run_delta_propagation_tick(&mut self) -> DeltaPropagationTickReport {
        match &self.delta_loop {
            Some(delta_loop) => delta_loop.run_tick(&mut self.delta_log),
            None => DeltaPropagationTickReport::skipped(self.delta_log.propagation_count()),
        }
    }

    fn schedule_delta_propagation_tick(&self, ctx: &Context<ReplicatorActorMsg<D>>) {
        if let Some(interval) = self.delta_tick_interval {
            ctx.schedule_once_self(interval, ReplicatorActorMsg::DeltaPropagationTick);
        }
    }

    fn run_removed_node_pruning_tick(
        &mut self,
        tick: RemovedNodePruningTick,
    ) -> RemovedNodePruningTickReport {
        if !tick.unreachable_replicas.is_empty() {
            return RemovedNodePruningTickReport::skipped_unreachable();
        }

        let mut report = RemovedNodePruningTickReport::default();

        if tick.is_leader {
            let mut known_nodes = tick.live_replicas.clone();
            known_nodes.insert(tick.self_replica.clone());
            known_nodes.extend(self.removed_node_pruning.removed_nodes().keys().cloned());

            let modified_by = self.state.modified_by_replica_ids();
            report.collected_removed = self.removed_node_pruning.record_unknown_modified_nodes(
                modified_by.iter(),
                &known_nodes,
                &tick.self_replica,
                tick.all_reachable_time_nanos,
            );

            for removed in self.removed_node_pruning.ready_to_initialize(
                tick.all_reachable_time_nanos,
                tick.max_pruning_dissemination_nanos,
            ) {
                report.initialized.extend(
                    self.state
                        .init_removed_node_pruning(&removed, &tick.self_replica),
                );
            }
        }

        let (performed, failures) = self.state.perform_removed_node_pruning(
            &tick.self_replica,
            &tick.live_replicas,
            tick.pruning_performed(),
        );
        report.performed = performed;
        report.failures = failures;

        let (obsolete_markers, forgotten_removed) = self
            .state
            .remove_obsolete_pruning_performed(tick.now_millis);
        report.obsolete_markers = obsolete_markers;
        for removed in forgotten_removed {
            self.removed_node_pruning.forget_removed(&removed);
            report.forgotten_removed.insert(removed);
        }

        report
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
