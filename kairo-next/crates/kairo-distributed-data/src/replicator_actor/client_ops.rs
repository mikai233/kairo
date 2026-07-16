use super::*;

impl<D> ReplicatorActor<D>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    pub(super) fn handle_get(
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

    pub(super) fn handle_update(
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
            Err(AggregationError::NotEnoughReplicas { .. }) => {
                tell_or_actor_error(&reply_to, UpdateResponse::Timeout { key })?;
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

    pub(super) fn subscribe(
        &mut self,
        key: ReplicatorKey,
        subscriber: ActorRef<ReplicatorChange<D>>,
    ) {
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

    pub(super) fn unsubscribe(
        &mut self,
        key: &ReplicatorKey,
        subscriber: &ActorRef<ReplicatorChange<D>>,
    ) {
        if let Some(subscribers) = self.subscribers.get_mut(key) {
            subscribers.retain(|existing| existing.path() != subscriber.path());
            if subscribers.is_empty() {
                self.subscribers.remove(key);
            }
        }
    }

    pub(super) fn flush_changes(&mut self) {
        for change in self.state.flush_changes() {
            if let Some(subscribers) = self.subscribers.get_mut(change.key()) {
                subscribers.retain(|subscriber| subscriber.tell(change.clone()).is_ok());
            }
        }
        self.subscribers
            .retain(|_, subscribers| !subscribers.is_empty());
    }

    pub(super) fn plan_read(
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

    pub(super) fn plan_write(
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
