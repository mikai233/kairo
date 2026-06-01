use super::*;

impl<D> ReplicatorActor<D>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    pub(super) fn run_removed_node_pruning_tick(
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
