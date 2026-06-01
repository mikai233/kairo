use super::*;

impl<M> ShardRegionActor<M>
where
    M: Send + 'static,
{
    pub fn new(self_region: impl Into<RegionId>, buffer_capacity: usize) -> Self {
        Self {
            runtime: ShardRegionRuntime::new(self_region, buffer_capacity),
            local_shard_spawner: None,
            local_shards: BTreeMap::new(),
            registration: None,
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
        }
    }

    pub fn new_with_local_shards(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
    ) -> Self {
        Self {
            runtime: ShardRegionRuntime::new(self_region, region_buffer_capacity),
            local_shard_spawner: Some(LocalShardSpawner::plain(shard_buffer_capacity)),
            local_shards: BTreeMap::new(),
            registration: None,
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
        }
    }

    pub fn new_with_local_shards_and_registration(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        coordinator: ActorRef<ShardCoordinatorMsg<M>>,
        retry_interval: Duration,
    ) -> Self
    where
        M: Send + 'static,
    {
        Self {
            runtime: ShardRegionRuntime::new(self_region, region_buffer_capacity),
            local_shard_spawner: Some(LocalShardSpawner::plain(shard_buffer_capacity)),
            local_shards: BTreeMap::new(),
            registration: Some(RegionRegistration::new(RegionRegistrationConfig::new(
                coordinator,
                retry_interval,
            ))),
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
        }
    }

    pub fn new_with_local_entity_shards(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
    ) -> Self
    where
        M: Clone,
    {
        Self {
            runtime: ShardRegionRuntime::new(self_region, region_buffer_capacity),
            local_shard_spawner: Some(LocalShardSpawner::entity_backed(
                shard_buffer_capacity,
                entity_factory,
            )),
            local_shards: BTreeMap::new(),
            registration: None,
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
        }
    }

    pub fn new_with_local_entity_shards_and_registration(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
        coordinator: ActorRef<ShardCoordinatorMsg<M>>,
        retry_interval: Duration,
    ) -> Self
    where
        M: Clone,
    {
        Self {
            runtime: ShardRegionRuntime::new(self_region, region_buffer_capacity),
            local_shard_spawner: Some(LocalShardSpawner::entity_backed(
                shard_buffer_capacity,
                entity_factory,
            )),
            local_shards: BTreeMap::new(),
            registration: Some(RegionRegistration::new(RegionRegistrationConfig::new(
                coordinator,
                retry_interval,
            ))),
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
        }
    }

    pub fn new_with_local_remember_store_shards(
        self_region: impl Into<RegionId>,
        type_name: impl Into<String>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
        timeout: Duration,
    ) -> Self {
        Self {
            runtime: ShardRegionRuntime::new(self_region, region_buffer_capacity),
            local_shard_spawner: Some(LocalShardSpawner::with_local_remember_stores(
                type_name,
                shard_buffer_capacity,
                remembered_entities_by_shard,
                timeout,
            )),
            local_shards: BTreeMap::new(),
            registration: None,
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
        }
    }

    pub fn new_with_local_remember_store_shards_and_registration(
        self_region: impl Into<RegionId>,
        type_name: impl Into<String>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
        timeout: Duration,
        registration: RegionRegistrationConfig<M>,
    ) -> Self
    where
        M: Send + 'static,
    {
        Self {
            runtime: ShardRegionRuntime::new(self_region, region_buffer_capacity),
            local_shard_spawner: Some(LocalShardSpawner::with_local_remember_stores(
                type_name,
                shard_buffer_capacity,
                remembered_entities_by_shard,
                timeout,
            )),
            local_shards: BTreeMap::new(),
            registration: Some(RegionRegistration::new(registration)),
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
        }
    }

    pub fn with_coordinator_discovery(
        mut self,
        discovery: RegionCoordinatorDiscoveryConfig<M>,
    ) -> Self {
        self.registration = None;
        self.remote_coordinator.set_target(None, None);
        self.coordinator_discovery = Some(RegionCoordinatorDiscovery::new(discovery));
        self
    }

    pub fn with_remote_coordinator_transport(
        mut self,
        transport: RegionRemoteCoordinatorTransport,
    ) -> Self {
        self.remote_coordinator_transport = Some(transport);
        self
    }

    pub fn with_region_route_transport(mut self, route_transport: RegionRouteTransport<M>) -> Self {
        self.route_transport = Some(route_transport);
        self
    }

    pub fn with_remote_handoff_stop_message_factory(
        mut self,
        stop_message: impl Fn() -> M + Send + Sync + 'static,
        timeout: Duration,
    ) -> Self {
        self.remote_handoff = Some(RegionRemoteHandOff::new(stop_message, timeout));
        self
    }

    pub fn with_remote_handoff_stop_message(mut self, stop_message: M, timeout: Duration) -> Self
    where
        M: Clone + Send + Sync + 'static,
    {
        self.remote_handoff = Some(RegionRemoteHandOff::from_message(stop_message, timeout));
        self
    }

    pub fn props(self_region: impl Into<RegionId>, buffer_capacity: usize) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || Self::new(self_region, buffer_capacity))
    }

    pub fn props_with_local_shards(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || {
            Self::new_with_local_shards(self_region, region_buffer_capacity, shard_buffer_capacity)
        })
    }

    pub fn props_with_local_shards_and_registration(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        coordinator: ActorRef<ShardCoordinatorMsg<M>>,
        retry_interval: Duration,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || {
            Self::new_with_local_shards_and_registration(
                self_region,
                region_buffer_capacity,
                shard_buffer_capacity,
                coordinator.clone(),
                retry_interval,
            )
        })
    }

    pub fn props_with_local_shards_and_coordinator_discovery(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        discovery: RegionCoordinatorDiscoveryConfig<M>,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || {
            Self::new_with_local_shards(self_region, region_buffer_capacity, shard_buffer_capacity)
                .with_coordinator_discovery(discovery.clone())
        })
    }

    pub fn props_with_local_entity_shards(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
    ) -> Props<Self>
    where
        M: Clone + Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || {
            Self::new_with_local_entity_shards(
                self_region,
                region_buffer_capacity,
                shard_buffer_capacity,
                entity_factory.clone(),
            )
        })
    }

    pub fn props_with_local_entity_shards_and_registration(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
        coordinator: ActorRef<ShardCoordinatorMsg<M>>,
        retry_interval: Duration,
    ) -> Props<Self>
    where
        M: Clone + Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || {
            Self::new_with_local_entity_shards_and_registration(
                self_region,
                region_buffer_capacity,
                shard_buffer_capacity,
                entity_factory.clone(),
                coordinator.clone(),
                retry_interval,
            )
        })
    }

    pub fn props_with_local_remember_store_shards(
        self_region: impl Into<RegionId>,
        type_name: impl Into<String>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
        timeout: Duration,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        let type_name = type_name.into();
        Props::new(move || {
            Self::new_with_local_remember_store_shards(
                self_region,
                type_name,
                region_buffer_capacity,
                shard_buffer_capacity,
                remembered_entities_by_shard,
                timeout,
            )
        })
    }

    pub fn props_with_local_remember_store_shards_and_registration(
        self_region: impl Into<RegionId>,
        type_name: impl Into<String>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
        timeout: Duration,
        registration: RegionRegistrationConfig<M>,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        let type_name = type_name.into();
        Props::new(move || {
            Self::new_with_local_remember_store_shards_and_registration(
                self_region,
                type_name.clone(),
                region_buffer_capacity,
                shard_buffer_capacity,
                remembered_entities_by_shard.clone(),
                timeout,
                registration.clone(),
            )
        })
    }

    pub fn runtime(&self) -> &ShardRegionRuntime<M> {
        &self.runtime
    }
}
