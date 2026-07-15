use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};
use kairo_cluster::{
    Cluster, ClusterSubscriptionEvent, ClusterSubscriptionInitialState, CurrentClusterState,
    UniqueAddress,
};
use kairo_remote::RemoteAssociationAddress;

use crate::{
    AggregationTargetRegistry, DeltaPropagationTargetRegistry, DeltaReplicatedData,
    RemovedNodePruningTick, RemovedNodePruningTickReport, ReplicaId, ReplicatorActorMsg,
    ReplicatorClusterConnectorTimingSettings, ReplicatorClusterRouteReport,
    ReplicatorClusterRouteUpdate, ReplicatorClusterRoutes, ReplicatorGossipTargetRegistry,
    ReplicatorRemoteRouteRegistrationReport, ReplicatorRemoteRouteTargets,
    SharedReplicatorClusterConnectorClock, SystemReplicatorClusterConnectorClock,
};

use crate::cluster_connector_timing::{CLOCK_TIMER_KEY, PRUNING_TIMER_KEY};

mod construction;
mod runtime;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicatorClusterPruningSettings {
    pub max_pruning_dissemination_nanos: u64,
    pub pruning_marker_ttl_millis: u64,
}

impl ReplicatorClusterPruningSettings {
    pub fn new(max_pruning_dissemination_nanos: u64, pruning_marker_ttl_millis: u64) -> Self {
        Self {
            max_pruning_dissemination_nanos,
            pruning_marker_ttl_millis,
        }
    }
}

pub struct ReplicatorClusterConnector<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    cluster: Cluster,
    routes: ReplicatorClusterRoutes,
    required_roles: Vec<String>,
    replicator: ActorRef<ReplicatorActorMsg<D>>,
    cluster_subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    route_report_adapter: Option<ActorRef<ReplicatorClusterRouteReport>>,
    pruning_report_adapter: Option<ActorRef<RemovedNodePruningTickReport>>,
    last_report: Option<ReplicatorClusterRouteReport>,
    last_pruning_report: Option<RemovedNodePruningTickReport>,
    all_reachable_time_nanos: u64,
    previous_clock_time_nanos: Option<u64>,
    pruning_settings: ReplicatorClusterPruningSettings,
    timing_settings: ReplicatorClusterConnectorTimingSettings,
    clock: SharedReplicatorClusterConnectorClock,
    remote_route_targets: Option<ReplicatorRemoteRouteTargets>,
    delta_target_registry: Option<DeltaPropagationTargetRegistry>,
    aggregation_target_registry: Option<AggregationTargetRegistry>,
    gossip_target_registry: Option<ReplicatorGossipTargetRegistry>,
    last_target_registration: Option<ReplicatorRemoteRouteRegistrationReport>,
    remote_source_replicas: Option<crate::ReplicatorRemoteSourceMap>,
}

#[derive(Debug, Clone)]
pub enum ReplicatorClusterConnectorMsg {
    Cluster(ClusterSubscriptionEvent),
    RouteApplied(ReplicatorClusterRouteReport),
    PruningTickApplied(RemovedNodePruningTickReport),
    ClockTick {
        now_nanos: u64,
    },
    SetAllReachableTimeNanos(u64),
    SetPruningSettings(ReplicatorClusterPruningSettings),
    RunRemovedNodePruning {
        now_millis: u64,
    },
    ClockTimerTick,
    PruningTimerTick,
    Snapshot {
        reply_to: ActorRef<ReplicatorClusterConnectorSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorClusterConnectorSnapshot {
    pub remote_replicas: Vec<ReplicaId>,
    pub unreachable_replicas: std::collections::BTreeSet<ReplicaId>,
    pub is_leader: bool,
    pub all_reachable_time_nanos: u64,
    pub last_report: Option<ReplicatorClusterRouteReport>,
    pub last_pruning_report: Option<RemovedNodePruningTickReport>,
    pub last_target_registration: Option<ReplicatorRemoteRouteRegistrationReport>,
}

impl<D> Actor for ReplicatorClusterConnector<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    type Msg = ReplicatorClusterConnectorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription = ctx.message_adapter(ReplicatorClusterConnectorMsg::Cluster)?;
        let route_report = ctx.message_adapter(ReplicatorClusterConnectorMsg::RouteApplied)?;
        let pruning_report =
            ctx.message_adapter(ReplicatorClusterConnectorMsg::PruningTickApplied)?;
        self.cluster_subscription = Some(subscription.clone());
        self.route_report_adapter = Some(route_report);
        self.pruning_report_adapter = Some(pruning_report);
        self.cluster
            .subscribe_with_initial_state(
                subscription.clone(),
                ClusterSubscriptionInitialState::Events,
            )
            .map_err(|error| ActorError::Message(error.to_string()))?;
        if self.timing_settings.clock_interval.is_some() {
            self.previous_clock_time_nanos = Some(self.clock.monotonic_nanos());
        }
        self.schedule_timers(ctx);
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        if let Some(subscription) = self.cluster_subscription.take() {
            let _ = self.cluster.unsubscribe(subscription);
        }
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ReplicatorClusterConnectorMsg::Cluster(event) => {
                if let ClusterSubscriptionEvent::Event(event) = &event
                    && self.is_self_removed_event(event)
                {
                    ctx.stop(ctx.myself())?;
                    return Ok(());
                }
                let update = match event {
                    ClusterSubscriptionEvent::CurrentState(state) => self.apply_snapshot(&state),
                    ClusterSubscriptionEvent::Event(event) => self.routes.apply_event(&event),
                };
                self.apply_route_update(update)?;
            }
            ReplicatorClusterConnectorMsg::RouteApplied(report) => {
                self.last_report = Some(report);
            }
            ReplicatorClusterConnectorMsg::PruningTickApplied(report) => {
                self.last_pruning_report = Some(report);
            }
            ReplicatorClusterConnectorMsg::ClockTick { now_nanos } => {
                self.advance_all_reachable_clock(now_nanos);
            }
            ReplicatorClusterConnectorMsg::SetAllReachableTimeNanos(nanos) => {
                self.all_reachable_time_nanos = nanos;
            }
            ReplicatorClusterConnectorMsg::SetPruningSettings(settings) => {
                self.pruning_settings = settings;
            }
            ReplicatorClusterConnectorMsg::RunRemovedNodePruning { now_millis } => {
                self.run_removed_node_pruning(now_millis)?;
            }
            ReplicatorClusterConnectorMsg::ClockTimerTick => {
                self.advance_all_reachable_clock(self.clock.monotonic_nanos());
            }
            ReplicatorClusterConnectorMsg::PruningTimerTick => {
                self.run_removed_node_pruning(self.clock.wall_millis())?;
            }
            ReplicatorClusterConnectorMsg::Snapshot { reply_to } => {
                tell_or_actor_error(&reply_to, self.snapshot())?;
            }
        }
        Ok(())
    }
}

impl<D> ReplicatorClusterConnector<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    fn is_self_removed_event(&self, event: &kairo_cluster::ClusterEvent) -> bool {
        matches!(
            event,
            kairo_cluster::ClusterEvent::Member(kairo_cluster::MemberEvent::Removed {
                member,
                ..
            }) if member.unique_address.address == self.routes.self_node().address
        )
    }
}

fn tell_or_actor_error<M>(target: &ActorRef<M>, message: M) -> ActorResult
where
    M: Send + 'static,
{
    target
        .tell(message)
        .map_err(|error| ActorError::Message(error.reason().to_string()))
}
