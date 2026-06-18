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
use kairo_testkit::await_assert;

use super::*;
use crate::{
    DeltaPropagationLog, DeltaPropagationTransport, DeltaTransportFailure, GCounter, GCounterCodec,
    ReplicatorActor, ReplicatorClusterConnectorClock, ReplicatorKey, ReplicatorRemoteEnvelope,
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
fn connector_stops_when_self_member_is_removed() {
    let system = ActorSystem::builder("ddata-cluster-connector-self-removed")
        .build()
        .unwrap();
    let self_node = node("self-removed", 1);
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
                }
            }),
        )
        .unwrap();
    let (snapshot_ref, snapshot_rx) =
        forward_ref::<ReplicatorClusterConnectorSnapshot>(&system, "self-removed-snapshots");

    eventually_snapshot(&connector, &snapshot_ref, &snapshot_rx, |snapshot| {
        snapshot.remote_replicas == vec![ReplicaId::from(&peer)]
    });

    publisher
        .tell(ClusterEventPublisherMsg::PublishEvent(
            ClusterEvent::Member(MemberEvent::Removed {
                member: member(self_node.clone(), MemberStatus::Removed, ["ddata"]),
                previous_status: MemberStatus::Up,
            }),
        ))
        .unwrap();

    assert!(
        connector.wait_for_stop(Duration::from_secs(1)),
        "connector should stop when its own cluster member is removed"
    );

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
    let (outbound, outbound_rx) = forward_ref::<ReplicatorRemoteEnvelope>(&system, "remote-out");
    let delta_targets = DeltaPropagationTargetRegistry::new();
    let aggregation_targets = AggregationTargetRegistry::new();
    let gossip_targets = ReplicatorGossipTargetRegistry::new();
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
                let gossip_targets = gossip_targets.clone();
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
                        Some(gossip_targets),
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
    assert_eq!(
        registration.gossip_registered(),
        registration.delta_registered()
    );
    assert_eq!(delta_targets.target_count(), 2);
    assert_eq!(aggregation_targets.target_count(), 2);
    assert_eq!(gossip_targets.target_count(), 2);

    let transport = DeltaPropagationTransport::with_target_registry(
        ReplicaId::from(&self_node),
        GCounterCodec,
        delta_targets.clone(),
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

    publisher
        .tell(ClusterEventPublisherMsg::PublishEvent(
            ClusterEvent::Member(MemberEvent::Removed {
                member: member(weak.clone(), MemberStatus::Removed, ["ddata"]),
                previous_status: MemberStatus::WeaklyUp,
            }),
        ))
        .unwrap();

    let snapshot = eventually_snapshot(&connector, &snapshot_ref, &snapshot_rx, |snapshot| {
        snapshot
            .last_target_registration
            .as_ref()
            .is_some_and(|report| report.delta_registered() == [ReplicaId::from(&peer)])
    });
    let registration = snapshot.last_target_registration.unwrap();
    assert_eq!(registration.delta_registered(), &[ReplicaId::from(&peer)]);
    assert_eq!(
        registration.aggregation_registered(),
        registration.delta_registered()
    );
    assert_eq!(
        registration.gossip_registered(),
        registration.delta_registered()
    );
    assert_eq!(delta_targets.target_count(), 1);
    assert_eq!(aggregation_targets.target_count(), 1);
    assert_eq!(gossip_targets.target_count(), 1);

    let mut log = DeltaPropagationLog::new([ReplicaId::from(&peer), ReplicaId::from(&weak)]);
    log.record_delta(
        ReplicatorKey::new("after-removal"),
        Some(delta_counter("self", 7)),
    );
    let report = transport.publish(log.collect_propagations());
    assert_eq!(report.sent_to(), &[ReplicaId::from(&peer)]);
    assert_eq!(
        report.failures(),
        &[DeltaTransportFailure::MissingTarget {
            replica: ReplicaId::from(&weak)
        }]
    );
    let remote = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(remote.target, ReplicaId::from(&peer));

    system.terminate(Duration::from_secs(1)).unwrap();
}

fn eventually_snapshot(
    connector: &ActorRef<ReplicatorClusterConnectorMsg>,
    reply_to: &ActorRef<ReplicatorClusterConnectorSnapshot>,
    rx: &mpsc::Receiver<ReplicatorClusterConnectorSnapshot>,
    matches: impl Fn(&ReplicatorClusterConnectorSnapshot) -> bool,
) -> ReplicatorClusterConnectorSnapshot {
    let mut last_snapshot = None;
    await_assert(
        Duration::from_secs(2),
        Duration::from_millis(10),
        || -> Result<ReplicatorClusterConnectorSnapshot, String> {
            connector
                .tell(ReplicatorClusterConnectorMsg::Snapshot {
                    reply_to: reply_to.clone(),
                })
                .map_err(|error| error.to_string())?;
            let snapshot = rx
                .recv_timeout(Duration::from_millis(100))
                .map_err(|error| format!("snapshot response was not received: {error}"))?;
            if matches(&snapshot) {
                Ok(snapshot)
            } else {
                last_snapshot = Some(snapshot);
                Err(format!(
                    "snapshot condition was not met; last snapshot: {last_snapshot:?}"
                ))
            }
        },
    )
    .unwrap()
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
