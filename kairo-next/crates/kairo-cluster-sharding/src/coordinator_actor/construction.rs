use super::*;

impl ShardCoordinatorActor<()> {
    /// Creates a coordinator with an explicit allocation strategy.
    ///
    /// This constructor omits periodic rebalancing, remember storage, and
    /// handoff transport; callers may drive its pure planning messages directly.
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
            region_watch_by_path: HashMap::new(),
        }
    }

    /// Creates a coordinator that schedules periodic rebalance turns.
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
            region_watch_by_path: HashMap::new(),
        }
    }

    /// Creates a coordinator that loads and updates an external local remember store.
    ///
    /// User traffic is stashed until the initial load succeeds. An ask or store
    /// failure stops this actor so its lifecycle owner can replace it cleanly.
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
            region_watch_by_path: HashMap::new(),
        }
    }

    /// Creates a coordinator that owns an in-memory remember-store child.
    ///
    /// The supplied state seeds the child and is loaded before coordinator
    /// requests are served.
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
            region_watch_by_path: HashMap::new(),
        }
    }

    /// Creates a coordinator using the default bounded least-shards strategy.
    pub fn with_least_shard_strategy(state: CoordinatorState) -> Self {
        Self::new(state, LeastShardAllocationStrategy::default())
    }

    /// Returns restartable actor properties for [`Self::new`].
    pub fn props(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
    ) -> Props<Self> {
        Props::new(move || Self::new(state, strategy))
    }

    /// Returns restartable actor properties with periodic rebalancing enabled.
    pub fn props_with_rebalance_interval(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        interval: Duration,
    ) -> Props<Self> {
        Props::new(move || Self::with_rebalance_interval(state, strategy, interval))
    }

    /// Returns restartable actor properties using the default allocation strategy.
    pub fn props_with_least_shard_strategy(state: CoordinatorState) -> Props<Self> {
        Props::new(move || Self::with_least_shard_strategy(state))
    }

    /// Returns restartable actor properties backed by an external local remember store.
    ///
    /// `stash_capacity` bounds traffic retained while the initial store load is pending.
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

    /// Returns restartable actor properties with an owned in-memory remember store.
    ///
    /// `stash_capacity` bounds traffic retained while the initial store load is pending.
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
    fn with_periodic_rebalance(mut self, interval: Duration) -> Self {
        self.rebalance_interval = Some(interval);
        self
    }

    /// Creates a coordinator with two-phase shard handoff orchestration.
    ///
    /// The transport receives region commands and `stop_message` is forwarded
    /// to entities during phase-two handoff.
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
            region_watch_by_path: HashMap::new(),
        }
    }

    /// Creates a handoff-capable coordinator with an owned in-memory remember store.
    pub fn with_local_remember_store_and_handoff(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        store_state: RememberCoordinatorStoreState,
        store_timeout: Duration,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state.with_remember_entities(true)),
            strategy: Box::new(strategy),
            rebalance_interval: None,
            remember_store: None,
            local_remember_store_provider: Some(LocalCoordinatorRememberStoreProvider::new(
                store_state,
                store_timeout,
            )),
            waiting_for_remember_store_load: true,
            handoff: Some(CoordinatorHandoff::new(
                stop_message,
                handoff_timeout,
                transport,
            )),
            remote_regions: CoordinatorRemoteRegions::new(),
            region_watch_by_path: HashMap::new(),
        }
    }

    /// Creates a handoff-capable coordinator backed by an external local remember store.
    pub fn with_remember_store_and_handoff(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        remember_store: ActorRef<RememberCoordinatorStoreMsg>,
        store_timeout: Duration,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state.with_remember_entities(true)),
            strategy: Box::new(strategy),
            rebalance_interval: None,
            remember_store: Some(CoordinatorRememberStore::new(remember_store, store_timeout)),
            local_remember_store_provider: None,
            waiting_for_remember_store_load: true,
            handoff: Some(CoordinatorHandoff::new(
                stop_message,
                handoff_timeout,
                transport,
            )),
            remote_regions: CoordinatorRemoteRegions::new(),
            region_watch_by_path: HashMap::new(),
        }
    }

    /// Creates a handoff-capable coordinator backed by distributed-data storage.
    ///
    /// The store preserves shard existence across coordinator replacement; it
    /// does not preserve stale region ownership.
    pub fn with_ddata_remember_store_and_handoff(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        remember_store: ActorRef<RememberCoordinatorDDataStoreMsg>,
        store_timeout: Duration,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state.with_remember_entities(true)),
            strategy: Box::new(strategy),
            rebalance_interval: None,
            remember_store: Some(CoordinatorRememberStore::from_distributed_data(
                remember_store,
                store_timeout,
            )),
            local_remember_store_provider: None,
            waiting_for_remember_store_load: true,
            handoff: Some(CoordinatorHandoff::new(
                stop_message,
                handoff_timeout,
                transport,
            )),
            remote_regions: CoordinatorRemoteRegions::new(),
            region_watch_by_path: HashMap::new(),
        }
    }

    /// Returns restartable actor properties with two-phase handoff enabled.
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

    pub(crate) fn props_with_rebalance_and_handoff(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
        rebalance_interval: Duration,
    ) -> Props<Self> {
        Props::new(move || {
            Self::with_handoff(state, strategy, stop_message, handoff_timeout, transport)
                .with_periodic_rebalance(rebalance_interval)
        })
    }

    #[allow(clippy::too_many_arguments)]
    /// Returns restartable handoff properties backed by an external local remember store.
    ///
    /// `stash_capacity` bounds traffic retained while the initial store load is pending.
    pub fn props_with_remember_store_and_handoff(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        remember_store: ActorRef<RememberCoordinatorStoreMsg>,
        store_timeout: Duration,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
        stash_capacity: usize,
    ) -> Props<Self> {
        Props::new(move || {
            Self::with_remember_store_and_handoff(
                state,
                strategy,
                remember_store,
                store_timeout,
                stop_message,
                handoff_timeout,
                transport,
            )
        })
        .with_stash_capacity(stash_capacity)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn props_with_rebalance_remember_store_and_handoff(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        remember_store: ActorRef<RememberCoordinatorStoreMsg>,
        store_timeout: Duration,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
        rebalance_interval: Duration,
        stash_capacity: usize,
    ) -> Props<Self> {
        Props::new(move || {
            Self::with_remember_store_and_handoff(
                state,
                strategy,
                remember_store,
                store_timeout,
                stop_message,
                handoff_timeout,
                transport,
            )
            .with_periodic_rebalance(rebalance_interval)
        })
        .with_stash_capacity(stash_capacity)
    }

    #[allow(clippy::too_many_arguments)]
    /// Returns restartable handoff properties backed by distributed-data storage.
    ///
    /// `stash_capacity` bounds traffic retained while the initial store load is pending.
    pub fn props_with_ddata_remember_store_and_handoff(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        remember_store: ActorRef<RememberCoordinatorDDataStoreMsg>,
        store_timeout: Duration,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
        stash_capacity: usize,
    ) -> Props<Self> {
        Props::new(move || {
            Self::with_ddata_remember_store_and_handoff(
                state,
                strategy,
                remember_store,
                store_timeout,
                stop_message,
                handoff_timeout,
                transport,
            )
        })
        .with_stash_capacity(stash_capacity)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn props_with_rebalance_ddata_remember_store_and_handoff(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        remember_store: ActorRef<RememberCoordinatorDDataStoreMsg>,
        store_timeout: Duration,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
        rebalance_interval: Duration,
        stash_capacity: usize,
    ) -> Props<Self> {
        Props::new(move || {
            Self::with_ddata_remember_store_and_handoff(
                state,
                strategy,
                remember_store,
                store_timeout,
                stop_message,
                handoff_timeout,
                transport,
            )
            .with_periodic_rebalance(rebalance_interval)
        })
        .with_stash_capacity(stash_capacity)
    }

    /// Borrows the pure coordinator runtime for diagnostics and focused tests.
    pub fn runtime(&self) -> &CoordinatorRuntime {
        &self.runtime
    }
}
