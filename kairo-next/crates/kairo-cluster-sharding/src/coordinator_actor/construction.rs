use super::*;

impl ShardCoordinatorActor<()> {
    pub fn new(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state),
            strategy: Box::new(strategy),
            rebalance_interval: None,
            remember_store: None,
            local_remember_store_provider: None,
            waiting_for_remember_store_load: false,
            handoff: None,
            remote_regions: CoordinatorRemoteRegions::new(),
        }
    }

    pub fn with_rebalance_interval(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        interval: Duration,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state),
            strategy: Box::new(strategy),
            rebalance_interval: Some(interval),
            remember_store: None,
            local_remember_store_provider: None,
            waiting_for_remember_store_load: false,
            handoff: None,
            remote_regions: CoordinatorRemoteRegions::new(),
        }
    }

    pub fn with_remember_store(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        remember_store: ActorRef<RememberCoordinatorStoreMsg>,
        timeout: Duration,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state.with_remember_entities(true)),
            strategy: Box::new(strategy),
            rebalance_interval: None,
            remember_store: Some(CoordinatorRememberStore::new(remember_store, timeout)),
            local_remember_store_provider: None,
            waiting_for_remember_store_load: true,
            handoff: None,
            remote_regions: CoordinatorRemoteRegions::new(),
        }
    }

    pub fn with_local_remember_store(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        store_state: RememberCoordinatorStoreState,
        timeout: Duration,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state.with_remember_entities(true)),
            strategy: Box::new(strategy),
            rebalance_interval: None,
            remember_store: None,
            local_remember_store_provider: Some(LocalCoordinatorRememberStoreProvider::new(
                store_state,
                timeout,
            )),
            waiting_for_remember_store_load: true,
            handoff: None,
            remote_regions: CoordinatorRemoteRegions::new(),
        }
    }

    pub fn with_least_shard_strategy(state: CoordinatorState) -> Self {
        Self::new(state, LeastShardAllocationStrategy::default())
    }

    pub fn props(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
    ) -> Props<Self> {
        Props::new(move || Self::new(state, strategy))
    }

    pub fn props_with_rebalance_interval(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        interval: Duration,
    ) -> Props<Self> {
        Props::new(move || Self::with_rebalance_interval(state, strategy, interval))
    }

    pub fn props_with_least_shard_strategy(state: CoordinatorState) -> Props<Self> {
        Props::new(move || Self::with_least_shard_strategy(state))
    }

    pub fn props_with_remember_store(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        remember_store: ActorRef<RememberCoordinatorStoreMsg>,
        timeout: Duration,
        stash_capacity: usize,
    ) -> Props<Self> {
        Props::new(move || Self::with_remember_store(state, strategy, remember_store, timeout))
            .with_stash_capacity(stash_capacity)
    }

    pub fn props_with_local_remember_store(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        store_state: RememberCoordinatorStoreState,
        timeout: Duration,
        stash_capacity: usize,
    ) -> Props<Self> {
        Props::new(move || Self::with_local_remember_store(state, strategy, store_state, timeout))
            .with_stash_capacity(stash_capacity)
    }
}

impl<M> ShardCoordinatorActor<M>
where
    M: Clone + Send + 'static,
{
    pub fn with_handoff(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state),
            strategy: Box::new(strategy),
            rebalance_interval: None,
            remember_store: None,
            local_remember_store_provider: None,
            waiting_for_remember_store_load: false,
            handoff: Some(CoordinatorHandoff::new(
                stop_message,
                handoff_timeout,
                transport,
            )),
            remote_regions: CoordinatorRemoteRegions::new(),
        }
    }

    pub fn props_with_handoff(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
    ) -> Props<Self> {
        Props::new(move || {
            Self::with_handoff(state, strategy, stop_message, handoff_timeout, transport)
        })
    }

    pub fn runtime(&self) -> &CoordinatorRuntime {
        &self.runtime
    }
}
