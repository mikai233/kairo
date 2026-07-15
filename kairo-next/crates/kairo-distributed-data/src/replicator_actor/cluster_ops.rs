use super::*;

impl<D> ReplicatorActor<D>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    pub(super) fn apply_cluster_route_update(
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
            self.delta_receive.clear_from(&replica);
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
}
