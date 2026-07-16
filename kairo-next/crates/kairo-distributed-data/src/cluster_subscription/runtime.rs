use std::collections::BTreeMap;

use super::*;

impl<D> ReplicatorClusterConnector<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    pub(super) fn apply_snapshot(
        &mut self,
        state: &CurrentClusterState,
    ) -> ReplicatorClusterRouteUpdate {
        self.routes = ReplicatorClusterRoutes::from_current_state(
            self.routes.self_node().clone(),
            state,
            self.required_roles.iter().cloned(),
        );
        self.routes.update()
    }

    pub(super) fn schedule_timers(&self, ctx: &mut Context<ReplicatorClusterConnectorMsg>) {
        if let Some(interval) = self.timing_settings.clock_interval {
            ctx.start_timer_with_fixed_delay(
                CLOCK_TIMER_KEY,
                self.timing_settings.periodic_tasks_initial_delay,
                interval,
                ReplicatorClusterConnectorMsg::ClockTimerTick,
            );
        }

        if let Some(interval) = self.timing_settings.pruning_interval {
            ctx.start_timer_with_fixed_delay(
                PRUNING_TIMER_KEY,
                self.timing_settings.periodic_tasks_initial_delay,
                interval,
                ReplicatorClusterConnectorMsg::PruningTimerTick,
            );
        }
    }

    pub(super) fn apply_route_update(
        &mut self,
        update: ReplicatorClusterRouteUpdate,
    ) -> ActorResult {
        self.register_remote_route_targets()?;

        let Some(reply_to) = self.route_report_adapter.clone() else {
            return Err(ActorError::Message(
                "replicator cluster connector route adapter is not initialized".to_string(),
            ));
        };

        self.replicator
            .tell(ReplicatorActorMsg::ApplyClusterRouteUpdate {
                update,
                all_reachable_time_nanos: self.all_reachable_time_nanos,
                reply_to,
            })
            .map_err(|error| ActorError::Message(error.reason().to_string()))
    }

    pub(super) fn register_remote_route_targets(&mut self) -> ActorResult {
        let prepared_sources = self.prepare_remote_sources()?;
        let Some(targets) = &self.remote_route_targets else {
            self.replace_remote_sources(prepared_sources);
            return Ok(());
        };

        // Validate every route before mutating any shared transport projection. Target
        // construction is otherwise repeated by each registry setter below, and a malformed
        // member or configured path must not leave inbound source identities newer than the
        // outbound target registries.
        if self.delta_target_registry.is_some()
            || self.aggregation_target_registry.is_some()
            || self.gossip_target_registry.is_some()
        {
            for node in self.routes.remote_nodes() {
                targets
                    .target_for_node(&node)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }

        self.replace_remote_sources(prepared_sources);

        let delta_registered = if let Some(registry) = &self.delta_target_registry {
            targets
                .set_delta_target_registry(&self.routes, registry)
                .map_err(|error| ActorError::Message(error.to_string()))?
                .registered()
                .to_vec()
        } else {
            Vec::new()
        };

        let aggregation_registered = if let Some(registry) = &self.aggregation_target_registry {
            targets
                .set_aggregation_target_registry(&self.routes, registry)
                .map_err(|error| ActorError::Message(error.to_string()))?
                .registered()
                .to_vec()
        } else {
            Vec::new()
        };

        let gossip_registered = if let Some(registry) = &self.gossip_target_registry {
            targets
                .set_gossip_target_registry(&self.routes, registry)
                .map_err(|error| ActorError::Message(error.to_string()))?
                .registered()
                .to_vec()
        } else {
            Vec::new()
        };

        self.last_target_registration = Some(ReplicatorRemoteRouteRegistrationReport::new(
            delta_registered,
            aggregation_registered,
            gossip_registered,
        ));
        Ok(())
    }

    fn prepare_remote_sources(
        &self,
    ) -> Result<Option<BTreeMap<RemoteAssociationAddress, ReplicaId>>, ActorError> {
        if self.remote_source_replicas.is_none() {
            return Ok(None);
        };

        let mut sources = BTreeMap::new();
        for node in self.routes.remote_nodes() {
            let host = node.address.host().ok_or_else(|| {
                ActorError::Message(format!(
                    "distributed-data cluster replica {} has no remote host",
                    node.ordering_key()
                ))
            })?;
            let address = RemoteAssociationAddress::new(
                node.address.protocol(),
                node.address.system(),
                host,
                node.address.port(),
            )
            .map_err(|error| ActorError::Message(error.to_string()))?;
            sources.insert(address, ReplicaId::from(node));
        }
        Ok(Some(sources))
    }

    fn replace_remote_sources(
        &self,
        prepared_sources: Option<BTreeMap<RemoteAssociationAddress, ReplicaId>>,
    ) {
        let (Some(replicas), Some(sources)) = (&self.remote_source_replicas, prepared_sources)
        else {
            return;
        };
        *replicas
            .lock()
            .expect("replicator remote source map lock poisoned") = sources;
    }

    pub(super) fn advance_all_reachable_clock(&mut self, now_nanos: u64) {
        if let Some(previous) = self.previous_clock_time_nanos
            && self.routes.unreachable_replicas().is_empty()
        {
            self.all_reachable_time_nanos = self
                .all_reachable_time_nanos
                .saturating_add(now_nanos.saturating_sub(previous));
        }
        self.previous_clock_time_nanos = Some(now_nanos);
    }

    pub(super) fn run_removed_node_pruning(&self, now_millis: u64) -> ActorResult {
        let Some(reply_to) = self.pruning_report_adapter.clone() else {
            return Err(ActorError::Message(
                "replicator cluster connector pruning adapter is not initialized".to_string(),
            ));
        };

        self.replicator
            .tell(ReplicatorActorMsg::RunRemovedNodePruning {
                tick: self.removed_node_pruning_tick(now_millis),
                reply_to,
            })
            .map_err(|error| ActorError::Message(error.reason().to_string()))
    }

    fn removed_node_pruning_tick(&self, now_millis: u64) -> RemovedNodePruningTick {
        RemovedNodePruningTick {
            self_replica: ReplicaId::from(self.routes.self_node()),
            live_replicas: self.routes.remote_replicas().into_iter().collect(),
            unreachable_replicas: self.routes.unreachable_replicas(),
            all_reachable_time_nanos: self.all_reachable_time_nanos,
            max_pruning_dissemination_nanos: self.pruning_settings.max_pruning_dissemination_nanos,
            now_millis,
            pruning_marker_ttl_millis: self.pruning_settings.pruning_marker_ttl_millis,
            is_leader: self.routes.is_leader(),
        }
    }

    pub(super) fn snapshot(&self) -> ReplicatorClusterConnectorSnapshot {
        ReplicatorClusterConnectorSnapshot {
            remote_replicas: self.routes.remote_replicas(),
            unreachable_replicas: self.routes.unreachable_replicas(),
            is_leader: self.routes.is_leader(),
            all_reachable_time_nanos: self.all_reachable_time_nanos,
            last_report: self.last_report.clone(),
            last_pruning_report: self.last_pruning_report.clone(),
            last_target_registration: self.last_target_registration.clone(),
        }
    }
}
