use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};
use kairo_cluster::{
    Cluster, ClusterSubscriptionEvent, ClusterSubscriptionInitialState, CurrentClusterState,
    UniqueAddress,
};

use crate::{
    DeltaReplicatedData, ReplicaId, ReplicatorActorMsg, ReplicatorClusterRouteReport,
    ReplicatorClusterRouteUpdate, ReplicatorClusterRoutes,
};

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
    last_report: Option<ReplicatorClusterRouteReport>,
    all_reachable_time_nanos: u64,
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
            last_report: None,
            all_reachable_time_nanos: 0,
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
            last_report: None,
            all_reachable_time_nanos: 0,
        }
    }

    pub fn with_all_reachable_time_nanos(mut self, all_reachable_time_nanos: u64) -> Self {
        self.all_reachable_time_nanos = all_reachable_time_nanos;
        self
    }
}

#[derive(Debug, Clone)]
pub enum ReplicatorClusterConnectorMsg {
    Cluster(ClusterSubscriptionEvent),
    RouteApplied(ReplicatorClusterRouteReport),
    SetAllReachableTimeNanos(u64),
    Snapshot {
        reply_to: ActorRef<ReplicatorClusterConnectorSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorClusterConnectorSnapshot {
    pub remote_replicas: Vec<ReplicaId>,
    pub unreachable_replicas: std::collections::BTreeSet<ReplicaId>,
    pub is_leader: bool,
    pub last_report: Option<ReplicatorClusterRouteReport>,
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
        self.cluster_subscription = Some(subscription.clone());
        self.route_report_adapter = Some(route_report);
        self.cluster
            .subscribe_with_initial_state(
                subscription.clone(),
                ClusterSubscriptionInitialState::Events,
            )
            .map_err(|error| ActorError::Message(error.to_string()))?;
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
            ReplicatorClusterConnectorMsg::SetAllReachableTimeNanos(nanos) => {
                self.all_reachable_time_nanos = nanos;
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

    fn apply_route_update(&self, update: ReplicatorClusterRouteUpdate) -> ActorResult {
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

    fn snapshot(&self) -> ReplicatorClusterConnectorSnapshot {
        ReplicatorClusterConnectorSnapshot {
            remote_replicas: self.routes.remote_replicas(),
            unreachable_replicas: self.routes.unreachable_replicas(),
            is_leader: self.routes.is_leader(),
            last_report: self.last_report.clone(),
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
    use std::sync::mpsc;
    use std::time::Duration;

    use kairo_actor::{ActorSystem, Address, Props};
    use kairo_cluster::{
        ClusterEvent, ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberEvent,
        MemberStatus, Reachability,
    };

    use super::*;
    use crate::{GCounter, ReplicatorActor};

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
                    }
                }),
            )
            .unwrap();
        let (snapshot_ref, snapshot_rx) =
            forward_ref::<ReplicatorClusterConnectorSnapshot>(&system, "snapshots");

        let snapshot = eventually_snapshot(&connector, &snapshot_ref, &snapshot_rx, |snapshot| {
            snapshot.last_report.is_some() && snapshot.remote_replicas.len() == 2
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
}
