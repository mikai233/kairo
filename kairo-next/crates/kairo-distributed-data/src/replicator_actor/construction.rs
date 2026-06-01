use super::*;

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
            gossip_transport: None,
            gossip_codec: None,
            gossip_tick_interval: None,
            gossip_max_entries: 10,
            gossip_next_index: 0,
            gossip_next_chunk: 0,
            self_system_uid: None,
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

    pub fn with_gossip(
        transport: ReplicatorGossipTransport,
        codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
    ) -> Self {
        let mut actor = Self::new();
        actor.gossip_transport = Some(transport);
        actor.gossip_codec = Some(codec);
        actor
    }

    pub fn with_gossip_interval(
        transport: ReplicatorGossipTransport,
        codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
        interval: Duration,
    ) -> Self {
        let mut actor = Self::with_gossip(transport, codec);
        actor.gossip_tick_interval = Some(interval);
        actor
    }

    pub fn with_gossip_max_entries(mut self, max_entries: usize) -> Self {
        self.gossip_max_entries = max_entries.max(1);
        self
    }

    pub fn with_self_system_uid(mut self, uid: u64) -> Self {
        self.self_system_uid = Some(uid);
        self
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
