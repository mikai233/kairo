use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};
use kairo_cluster::{
    Cluster, ClusterSubscriptionEvent, ClusterSubscriptionInitialState, CurrentClusterState,
    UniqueAddress,
};

use crate::{
    AggregationTargetRegistry, DeltaPropagationTargetRegistry, DeltaReplicatedData,
    RemovedNodePruningTick, RemovedNodePruningTickReport, ReplicaId, ReplicatorActorMsg,
    ReplicatorClusterConnectorTimingSettings, ReplicatorClusterRouteReport,
    ReplicatorClusterRouteUpdate, ReplicatorClusterRoutes, ReplicatorRemoteRouteRegistrationReport,
    ReplicatorRemoteRouteTargets, SharedReplicatorClusterConnectorClock,
    SystemReplicatorClusterConnectorClock,
};

use crate::cluster_connector_timing::{CLOCK_TIMER_KEY, PRUNING_TIMER_KEY};

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
    last_target_registration: Option<ReplicatorRemoteRouteRegistrationReport>,
}

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
            last_target_registration: None,
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
            last_target_registration: None,
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
    ) -> Self {
        self.remote_route_targets = Some(targets);
        self.delta_target_registry = delta_registry;
        self.aggregation_target_registry = aggregation_registry;
        self
    }
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

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ReplicatorClusterConnectorMsg::Cluster(event) => {
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
    fn apply_snapshot(&mut self, state: &CurrentClusterState) -> ReplicatorClusterRouteUpdate {
        self.routes = ReplicatorClusterRoutes::from_current_state(
            self.routes.self_node().clone(),
            state,
            self.required_roles.iter().cloned(),
        );
        self.routes.update()
    }

    fn schedule_timers(&self, ctx: &mut Context<ReplicatorClusterConnectorMsg>) {
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

    fn apply_route_update(&mut self, update: ReplicatorClusterRouteUpdate) -> ActorResult {
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

    fn register_remote_route_targets(&mut self) -> ActorResult {
        let Some(targets) = &self.remote_route_targets else {
            return Ok(());
        };

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

        self.last_target_registration = Some(ReplicatorRemoteRouteRegistrationReport::new(
            delta_registered,
            aggregation_registered,
        ));
        Ok(())
    }

    fn advance_all_reachable_clock(&mut self, now_nanos: u64) {
        if let Some(previous) = self.previous_clock_time_nanos
            && self.routes.unreachable_replicas().is_empty()
        {
            self.all_reachable_time_nanos = self
                .all_reachable_time_nanos
                .saturating_add(now_nanos.saturating_sub(previous));
        }
        self.previous_clock_time_nanos = Some(now_nanos);
    }

    fn run_removed_node_pruning(&self, now_millis: u64) -> ActorResult {
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

    fn snapshot(&self) -> ReplicatorClusterConnectorSnapshot {
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

fn tell_or_actor_error<M>(target: &ActorRef<M>, message: M) -> ActorResult
where
    M: Send + 'static,
{
    target
        .tell(message)
        .map_err(|error| ActorError::Message(error.reason().to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    use kairo_actor::{ActorSystem, Address, ManualScheduler, Props};
    use kairo_cluster::{
        ClusterEvent, ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberEvent,
        MemberStatus, Reachability, ReachabilityEvent,
    };
    use kairo_serialization::Registry;

    use super::*;
    use crate::{
        DeltaPropagationLog, DeltaPropagationTransport, GCounter, GCounterCodec, ReplicatorActor,
        ReplicatorClusterConnectorClock, ReplicatorKey, ReplicatorRemoteEnvelope,
        register_ddata_protocol_codecs,
    };

    #[test]
    fn connector_subscribes_to_cluster_events_and_updates_replicator_routes() {
        let system = ActorSystem::builder("ddata-cluster-connector")
            .build()
            .unwrap();
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let weak = node("weak", 3);
        let other_role = node("other", 4);
        let publisher = system
            .spawn(
                "publisher",
                Props::new({
                    let self_node = self_node.clone();
                    move || ClusterEventPublisher::new(self_node)
                }),
            )
            .unwrap();
        let cluster = Cluster::new(publisher.clone());
        let replicator = system
            .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
            .unwrap();

        let gossip = Gossip::from_members([
            member(self_node.clone(), MemberStatus::Up, ["ddata"]),
            member(peer.clone(), MemberStatus::Up, ["ddata"]),
            member(weak.clone(), MemberStatus::WeaklyUp, ["ddata"]),
            member(other_role, MemberStatus::Up, ["other"]),
        ])
        .with_reachability(Reachability::new().unreachable(self_node.clone(), weak.clone()));
        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(gossip))
            .unwrap();

        let connector = system
            .spawn(
                "connector",
                Props::new({
                    let cluster = cluster.clone();
                    let self_node = self_node.clone();
                    let replicator = replicator.clone();
                    move || {
                        ReplicatorClusterConnector::with_required_roles(
                            cluster,
                            self_node,
                            replicator,
                            ["ddata"],
                        )
                        .with_pruning_settings(ReplicatorClusterPruningSettings::new(10, 100))
                    }
                }),
            )
            .unwrap();
        let (snapshot_ref, snapshot_rx) =
            forward_ref::<ReplicatorClusterConnectorSnapshot>(&system, "snapshots");

        let snapshot = eventually_snapshot(&connector, &snapshot_ref, &snapshot_rx, |snapshot| {
            snapshot.remote_replicas.len() == 2
                && snapshot
                    .last_report
                    .as_ref()
                    .is_some_and(|report| report.remote_replicas.len() == 2)
        });
        assert_eq!(
            snapshot.remote_replicas,
            vec![ReplicaId::from(&peer), ReplicaId::from(&weak)]
        );
        assert_eq!(
            snapshot.unreachable_replicas,
            BTreeSet::from([ReplicaId::from(&weak)])
        );
        assert_eq!(
            snapshot.last_report.unwrap().remote_replicas,
            vec![ReplicaId::from(&peer), ReplicaId::from(&weak)]
        );

        connector
            .tell(ReplicatorClusterConnectorMsg::ClockTick { now_nanos: 100 })
            .unwrap();
        connector
            .tell(ReplicatorClusterConnectorMsg::ClockTick { now_nanos: 130 })
            .unwrap();
        connector
            .tell(ReplicatorClusterConnectorMsg::RunRemovedNodePruning { now_millis: 1_000 })
            .unwrap();

        let snapshot = eventually_snapshot(&connector, &snapshot_ref, &snapshot_rx, |snapshot| {
            snapshot
                .last_pruning_report
                .as_ref()
                .is_some_and(|report| report.skipped_unreachable)
        });
        assert_eq!(snapshot.all_reachable_time_nanos, 0);

        publisher
            .tell(ClusterEventPublisherMsg::PublishEvent(
                ClusterEvent::Reachability(ReachabilityEvent::Reachable(member(
                    weak.clone(),
                    MemberStatus::WeaklyUp,
                    ["ddata"],
                ))),
            ))
            .unwrap();
        let snapshot = eventually_snapshot(&connector, &snapshot_ref, &snapshot_rx, |snapshot| {
            snapshot.unreachable_replicas.is_empty()
        });
        assert_eq!(
            snapshot.remote_replicas,
            vec![ReplicaId::from(&peer), ReplicaId::from(&weak)]
        );

        connector
            .tell(ReplicatorClusterConnectorMsg::ClockTick { now_nanos: 200 })
            .unwrap();
        connector
            .tell(ReplicatorClusterConnectorMsg::RunRemovedNodePruning { now_millis: 1_100 })
            .unwrap();
        let snapshot = eventually_snapshot(&connector, &snapshot_ref, &snapshot_rx, |snapshot| {
            snapshot.all_reachable_time_nanos == 70
                && snapshot
                    .last_pruning_report
                    .as_ref()
                    .is_some_and(|report| !report.skipped_unreachable)
        });
        assert_eq!(snapshot.all_reachable_time_nanos, 70);

        publisher
            .tell(ClusterEventPublisherMsg::PublishEvent(
                ClusterEvent::Member(MemberEvent::Removed {
                    member: member(peer.clone(), MemberStatus::Removed, ["ddata"]),
                    previous_status: MemberStatus::Up,
                }),
            ))
            .unwrap();

        let snapshot = eventually_snapshot(&connector, &snapshot_ref, &snapshot_rx, |snapshot| {
            snapshot.remote_replicas == vec![ReplicaId::from(&weak)]
                && snapshot
                    .last_report
                    .as_ref()
                    .is_some_and(|report| report.recorded_removed.contains(&ReplicaId::from(&peer)))
        });
        assert_eq!(snapshot.remote_replicas, vec![ReplicaId::from(&weak)]);

        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn connector_schedules_clock_and_pruning_ticks_with_manual_time() {
        let manual = ManualScheduler::new();
        let system = ActorSystem::builder("ddata-cluster-connector-timers")
            .manual_scheduler(manual.clone())
            .build()
            .unwrap();
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let publisher = system
            .spawn(
                "publisher",
                Props::new({
                    let self_node = self_node.clone();
                    move || ClusterEventPublisher::new(self_node)
                }),
            )
            .unwrap();
        let cluster = Cluster::new(publisher.clone());
        let replicator = system
            .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
            .unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([
                    member(self_node.clone(), MemberStatus::Up, ["ddata"]),
                    member(peer.clone(), MemberStatus::Up, ["ddata"]),
                ]),
            ))
            .unwrap();

        let clock = Arc::new(ManualConnectorClock {
            scheduler: manual.clone(),
            wall_offset_millis: 1_000,
        });
        let connector = system
            .spawn(
                "connector",
                Props::new({
                    let cluster = cluster.clone();
                    let self_node = self_node.clone();
                    let replicator = replicator.clone();
                    let clock = clock.clone();
                    move || {
                        ReplicatorClusterConnector::with_required_roles(
                            cluster,
                            self_node,
                            replicator,
                            ["ddata"],
                        )
                        .with_pruning_settings(ReplicatorClusterPruningSettings::new(10, 100))
                        .with_timing_settings(
                            ReplicatorClusterConnectorTimingSettings::new(
                                Duration::from_millis(50),
                                Duration::from_millis(100),
                            )
                            .with_periodic_tasks_initial_delay(Duration::from_millis(10)),
                        )
                        .with_clock(clock)
                    }
                }),
            )
            .unwrap();
        let (snapshot_ref, snapshot_rx) =
            forward_ref::<ReplicatorClusterConnectorSnapshot>(&system, "timer-snapshots");

        eventually_snapshot(&connector, &snapshot_ref, &snapshot_rx, |snapshot| {
            snapshot.remote_replicas == vec![ReplicaId::from(&peer)]
        });

        manual.advance(Duration::from_millis(10));
        let snapshot = eventually_snapshot(&connector, &snapshot_ref, &snapshot_rx, |snapshot| {
            snapshot.all_reachable_time_nanos == 10_000_000
                && snapshot
                    .last_pruning_report
                    .as_ref()
                    .is_some_and(|report| !report.skipped_unreachable)
        });
        assert_eq!(snapshot.all_reachable_time_nanos, 10_000_000);

        manual.advance(Duration::from_millis(50));
        let snapshot = eventually_snapshot(&connector, &snapshot_ref, &snapshot_rx, |snapshot| {
            snapshot.all_reachable_time_nanos == 60_000_000
        });
        assert_eq!(snapshot.all_reachable_time_nanos, 60_000_000);

        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn connector_registers_remote_route_targets_from_cluster_routes() {
        let system = ActorSystem::builder("ddata-cluster-connector-targets")
            .build()
            .unwrap();
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let weak = node("weak", 3);
        let publisher = system
            .spawn(
                "publisher",
                Props::new({
                    let self_node = self_node.clone();
                    move || ClusterEventPublisher::new(self_node)
                }),
            )
            .unwrap();
        let cluster = Cluster::new(publisher.clone());
        let replicator = system
            .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
            .unwrap();
        let (outbound, outbound_rx) =
            forward_ref::<ReplicatorRemoteEnvelope>(&system, "remote-out");
        let delta_targets = DeltaPropagationTargetRegistry::new();
        let aggregation_targets = AggregationTargetRegistry::new();
        let route_targets = ReplicatorRemoteRouteTargets::new(registry(), outbound);

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([
                    member(self_node.clone(), MemberStatus::Up, ["ddata"]),
                    member(peer.clone(), MemberStatus::Up, ["ddata"]),
                    member(weak.clone(), MemberStatus::WeaklyUp, ["ddata"]),
                ]),
            ))
            .unwrap();

        let connector = system
            .spawn(
                "connector",
                Props::new({
                    let cluster = cluster.clone();
                    let self_node = self_node.clone();
                    let replicator = replicator.clone();
                    let route_targets = route_targets.clone();
                    let delta_targets = delta_targets.clone();
                    let aggregation_targets = aggregation_targets.clone();
                    move || {
                        ReplicatorClusterConnector::with_required_roles(
                            cluster,
                            self_node,
                            replicator,
                            ["ddata"],
                        )
                        .with_remote_route_targets(
                            route_targets,
                            Some(delta_targets),
                            Some(aggregation_targets),
                        )
                    }
                }),
            )
            .unwrap();
        let (snapshot_ref, snapshot_rx) =
            forward_ref::<ReplicatorClusterConnectorSnapshot>(&system, "target-snapshots");

        let snapshot = eventually_snapshot(&connector, &snapshot_ref, &snapshot_rx, |snapshot| {
            snapshot
                .last_target_registration
                .as_ref()
                .is_some_and(|report| report.delta_registered().len() == 2)
        });
        let registration = snapshot.last_target_registration.unwrap();
        assert_eq!(
            registration.delta_registered(),
            &[ReplicaId::from(&peer), ReplicaId::from(&weak)]
        );
        assert_eq!(
            registration.aggregation_registered(),
            registration.delta_registered()
        );
        assert_eq!(delta_targets.target_count(), 2);
        assert_eq!(aggregation_targets.target_count(), 2);

        let transport = DeltaPropagationTransport::with_target_registry(
            ReplicaId::from(&self_node),
            GCounterCodec,
            delta_targets,
        );
        let key = ReplicatorKey::new("counter");
        let mut log = DeltaPropagationLog::new([ReplicaId::from(&peer)]);
        log.record_delta(key, Some(delta_counter("self", 5)));
        let report = transport.publish(log.collect_propagations());
        assert!(report.is_success());

        let remote = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(remote.target, ReplicaId::from(&peer));
        assert_eq!(
            remote.envelope.recipient.path(),
            "kairo://ddata@peer.example.test:2552/system/ddata"
        );

        system.terminate(Duration::from_secs(1)).unwrap();
    }

    fn eventually_snapshot(
        connector: &ActorRef<ReplicatorClusterConnectorMsg>,
        reply_to: &ActorRef<ReplicatorClusterConnectorSnapshot>,
        rx: &mpsc::Receiver<ReplicatorClusterConnectorSnapshot>,
        matches: impl Fn(&ReplicatorClusterConnectorSnapshot) -> bool,
    ) -> ReplicatorClusterConnectorSnapshot {
        for _ in 0..20 {
            connector
                .tell(ReplicatorClusterConnectorMsg::Snapshot {
                    reply_to: reply_to.clone(),
                })
                .unwrap();
            let snapshot = rx.recv_timeout(Duration::from_millis(100)).unwrap();
            if matches(&snapshot) {
                return snapshot;
            }
        }

        panic!("snapshot condition was not met")
    }

    struct Forward<M> {
        tx: mpsc::Sender<M>,
    }

    impl<M> Actor for Forward<M>
    where
        M: Send + 'static,
    {
        type Msg = M;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            self.tx
                .send(msg)
                .map_err(|error| ActorError::Message(error.to_string()))
        }
    }

    fn forward_ref<M>(system: &ActorSystem, name: &str) -> (ActorRef<M>, mpsc::Receiver<M>)
    where
        M: Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let actor = system
            .spawn(name, Props::new(move || Forward { tx }))
            .expect("forward actor should spawn");
        (actor, rx)
    }

    fn node(name: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new(
                "kairo",
                "ddata",
                Some(format!("{name}.example.test")),
                Some(2552),
            ),
            uid,
        )
    }

    fn member(
        node: UniqueAddress,
        status: MemberStatus,
        roles: impl IntoIterator<Item = &'static str>,
    ) -> Member {
        Member::new(node, roles.into_iter().map(str::to_string).collect()).with_status(status)
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_ddata_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn delta_counter(replica: &str, value: u128) -> GCounter {
        GCounter::new()
            .increment(ReplicaId::new(replica), value)
            .unwrap()
    }

    struct ManualConnectorClock {
        scheduler: ManualScheduler,
        wall_offset_millis: u64,
    }

    impl ReplicatorClusterConnectorClock for ManualConnectorClock {
        fn monotonic_nanos(&self) -> u64 {
            self.scheduler.now().as_nanos().min(u128::from(u64::MAX)) as u64
        }

        fn wall_millis(&self) -> u64 {
            self.wall_offset_millis
                .saturating_add(self.scheduler.now().as_millis().min(u128::from(u64::MAX)) as u64)
        }
    }
}
