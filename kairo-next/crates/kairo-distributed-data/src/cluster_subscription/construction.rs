use super::*;

impl<D> ReplicatorClusterConnector<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    pub fn new(
        cluster: Cluster,
        self_node: UniqueAddress,
        replicator: ActorRef<ReplicatorActorMsg<D>>,
    ) -> Self {
        Self {
            cluster,
            routes: ReplicatorClusterRoutes::new(self_node),
            required_roles: Vec::new(),
            replicator,
            cluster_subscription: None,
            route_report_adapter: None,
            pruning_report_adapter: None,
            last_report: None,
            last_pruning_report: None,
            all_reachable_time_nanos: 0,
            previous_clock_time_nanos: None,
            pruning_settings: ReplicatorClusterPruningSettings::new(0, 0),
            timing_settings: ReplicatorClusterConnectorTimingSettings::default(),
            clock: std::sync::Arc::new(SystemReplicatorClusterConnectorClock::new()),
            remote_route_targets: None,
            delta_target_registry: None,
            aggregation_target_registry: None,
            gossip_target_registry: None,
            last_target_registration: None,
            remote_source_replicas: None,
        }
    }

    pub fn with_required_roles(
        cluster: Cluster,
        self_node: UniqueAddress,
        replicator: ActorRef<ReplicatorActorMsg<D>>,
        roles: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let roles = roles.into_iter().map(Into::into).collect::<Vec<_>>();
        Self {
            cluster,
            routes: ReplicatorClusterRoutes::with_required_roles(self_node, roles.iter().cloned()),
            required_roles: roles,
            replicator,
            cluster_subscription: None,
            route_report_adapter: None,
            pruning_report_adapter: None,
            last_report: None,
            last_pruning_report: None,
            all_reachable_time_nanos: 0,
            previous_clock_time_nanos: None,
            pruning_settings: ReplicatorClusterPruningSettings::new(0, 0),
            timing_settings: ReplicatorClusterConnectorTimingSettings::default(),
            clock: std::sync::Arc::new(SystemReplicatorClusterConnectorClock::new()),
            remote_route_targets: None,
            delta_target_registry: None,
            aggregation_target_registry: None,
            gossip_target_registry: None,
            last_target_registration: None,
            remote_source_replicas: None,
        }
    }

    pub fn with_all_reachable_time_nanos(mut self, all_reachable_time_nanos: u64) -> Self {
        self.all_reachable_time_nanos = all_reachable_time_nanos;
        self
    }

    pub fn with_pruning_settings(mut self, settings: ReplicatorClusterPruningSettings) -> Self {
        self.pruning_settings = settings;
        self
    }

    pub fn with_timing_settings(
        mut self,
        settings: ReplicatorClusterConnectorTimingSettings,
    ) -> Self {
        self.timing_settings = settings;
        self
    }

    pub fn with_clock(mut self, clock: SharedReplicatorClusterConnectorClock) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_remote_route_targets(
        mut self,
        targets: ReplicatorRemoteRouteTargets,
        delta_registry: Option<DeltaPropagationTargetRegistry>,
        aggregation_registry: Option<AggregationTargetRegistry>,
        gossip_registry: Option<ReplicatorGossipTargetRegistry>,
    ) -> Self {
        self.remote_route_targets = Some(targets);
        self.delta_target_registry = delta_registry;
        self.aggregation_target_registry = aggregation_registry;
        self.gossip_target_registry = gossip_registry;
        self
    }

    pub fn with_remote_source_replicas(
        mut self,
        replicas: crate::ReplicatorRemoteSourceMap,
    ) -> Self {
        self.remote_source_replicas = Some(replicas);
        self
    }
}
