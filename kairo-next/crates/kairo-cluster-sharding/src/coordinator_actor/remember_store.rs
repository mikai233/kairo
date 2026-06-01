use super::*;

impl<M> ShardCoordinatorActor<M>
where
    M: Clone + Send + 'static,
{
    pub(super) fn receive_loading(
        &mut self,
        ctx: &mut Context<ShardCoordinatorMsg<M>>,
        msg: ShardCoordinatorMsg<M>,
    ) -> ActorResult {
        match msg {
            ShardCoordinatorMsg::RememberStoreLoadResult { result } => {
                self.apply_remember_store_load(ctx, result)?;
                ctx.unstash_all()
            }
            other => ctx.stash(other),
        }
    }

    pub(super) fn spawn_local_remember_store_if_needed(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
    ) -> ActorResult {
        if self.remember_store.is_some() {
            return Ok(());
        }
        let Some(provider) = &mut self.local_remember_store_provider else {
            return Ok(());
        };
        self.remember_store = Some(provider.spawn(ctx)?);
        Ok(())
    }

    pub(super) fn request_remember_store_load(
        &self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
    ) -> ActorResult {
        if let Some(store) = &self.remember_store {
            store.load(ctx)?;
        }
        Ok(())
    }

    pub(super) fn apply_remember_store_load(
        &mut self,
        ctx: &mut Context<ShardCoordinatorMsg<M>>,
        result: Result<RememberedShards, AskError>,
    ) -> ActorResult {
        let remembered = match result {
            Ok(remembered) => remembered,
            Err(_) => return self.stop_for_remember_store_failure(ctx),
        };
        self.runtime.merge_remembered_shards(remembered.shards);
        self.waiting_for_remember_store_load = false;
        self.allocate_remembered_shard_homes(ctx)?;
        Ok(())
    }

    pub(super) fn persist_allocated_shard(
        &self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        result: &Result<GetShardHomePlan, ShardingError>,
    ) -> ActorResult {
        let Ok(plan) = result else {
            return Ok(());
        };
        self.persist_allocated_shard_plan(ctx, plan)
    }

    pub(super) fn persist_allocated_shard_plan(
        &self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        plan: &GetShardHomePlan,
    ) -> ActorResult {
        let GetShardHomePlan::Allocated {
            event: CoordinatorEvent::ShardHomeAllocated { shard, region: _ },
            host_region: _,
            host_shard: _,
        } = plan
        else {
            return Ok(());
        };

        if let Some(store) = &self.remember_store {
            store.add_shard(ctx, shard.clone())?;
        }
        Ok(())
    }

    pub(super) fn allocate_remembered_shard_homes(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
    ) -> ActorResult {
        let plans = self
            .runtime
            .allocate_remembered_shard_homes("coordinator", self.strategy.as_ref())
            .map_err(|error| ActorError::Message(error.to_string()))?;
        for plan in &plans {
            self.persist_allocated_shard_plan(ctx, plan)?;
            if let Some(handoff) = &self.handoff {
                handoff.dispatch_host_shard(ctx, plan)?;
            }
        }
        Ok(())
    }

    pub(super) fn stop_for_remember_store_failure(
        &mut self,
        ctx: &mut Context<ShardCoordinatorMsg<M>>,
    ) -> ActorResult {
        ctx.stop(ctx.myself())
    }
}
