#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorPath, ActorRef, ActorResult, Context, Recipient, SendError,
};
use kairo_cluster::{
    Cluster, ClusterEvent, ClusterSubscriptionEvent, ClusterSubscriptionInitialState, Member,
    MemberEvent, MemberStatus, UniqueAddress,
};
use kairo_remote::RemoteOutbound;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{
    LocalSingletonManagerMsg, SingletonMessageEnvelope, SingletonOldestTracker, SingletonProxyMsg,
    SingletonProxyTarget, SingletonScope,
};

use super::SingletonDeliveryMsg;

const ROUTE_REFRESH_TIMER_KEY: &str = "cluster-singleton-route-refresh";

pub(super) type SingletonRemoteTargetFactory<M> = Arc<
    dyn Fn(&UniqueAddress) -> Result<SingletonProxyTarget<M>, ActorError> + Send + Sync + 'static,
>;

pub(super) struct ClusterSingletonConnector<M>
where
    M: Send + 'static,
{
    cluster: Cluster,
    self_node: UniqueAddress,
    scope: SingletonScope,
    manager: ActorRef<LocalSingletonManagerMsg<M>>,
    proxy: ActorRef<SingletonProxyMsg<M>>,
    remote_target_factory: Option<SingletonRemoteTargetFactory<M>>,
    delivery: ActorRef<SingletonDeliveryMsg<M>>,
    route_refresh_interval: Duration,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    singleton_reply: Option<ActorRef<Option<ActorRef<M>>>>,
    tracker: Option<SingletonOldestTracker>,
    remote_routes: BTreeMap<String, UniqueAddress>,
    local_route_present: bool,
}

#[derive(Clone)]
/// Internal actor protocol exposed for diagnostics and explicit snapshots.
pub enum ClusterSingletonConnectorMsg<M: Send + 'static> {
    /// Applies an initial cluster snapshot or one subsequent cluster event.
    Cluster(ClusterSubscriptionEvent),
    /// Reconciles the manager's current local singleton child with the proxy.
    LocalSingleton(Option<ActorRef<M>>),
    /// Requests another local-singleton reconciliation from the manager.
    RefreshRoute,
    /// Replies with the connector's current ownership and route view.
    Snapshot {
        /// Recipient for the connector snapshot.
        reply_to: ActorRef<ClusterSingletonConnectorSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Diagnostic snapshot of singleton ownership and installed proxy routes.
pub struct ClusterSingletonConnectorSnapshot {
    /// Oldest eligible member according to the latest cluster view.
    pub oldest: Option<UniqueAddress>,
    /// Eligible remote members with routes installed in the local proxy.
    pub remote_routes: Vec<UniqueAddress>,
    /// Whether the local singleton child is installed as a proxy route.
    pub local_route_present: bool,
}

pub(super) struct ClusterSingletonConnectorConfig<M>
where
    M: Send + 'static,
{
    pub(super) cluster: Cluster,
    pub(super) self_node: UniqueAddress,
    pub(super) scope: SingletonScope,
    pub(super) manager: ActorRef<LocalSingletonManagerMsg<M>>,
    pub(super) proxy: ActorRef<SingletonProxyMsg<M>>,
    pub(super) remote_target_factory: Option<SingletonRemoteTargetFactory<M>>,
    pub(super) delivery: ActorRef<SingletonDeliveryMsg<M>>,
    pub(super) route_refresh_interval: Duration,
}

impl<M> Clone for ClusterSingletonConnectorConfig<M>
where
    M: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            cluster: self.cluster.clone(),
            self_node: self.self_node.clone(),
            scope: self.scope.clone(),
            manager: self.manager.clone(),
            proxy: self.proxy.clone(),
            remote_target_factory: self.remote_target_factory.clone(),
            delivery: self.delivery.clone(),
            route_refresh_interval: self.route_refresh_interval,
        }
    }
}

impl<M> ClusterSingletonConnector<M>
where
    M: Clone + Send + 'static,
{
    pub(super) fn new(config: ClusterSingletonConnectorConfig<M>) -> Self {
        Self {
            cluster: config.cluster,
            self_node: config.self_node,
            scope: config.scope,
            manager: config.manager,
            proxy: config.proxy,
            remote_target_factory: config.remote_target_factory,
            delivery: config.delivery,
            route_refresh_interval: config.route_refresh_interval,
            subscription: None,
            singleton_reply: None,
            tracker: None,
            remote_routes: BTreeMap::new(),
            local_route_present: false,
        }
    }

    fn eligible(&self, member: &Member) -> bool {
        member.up_number.is_some()
            && !matches!(
                member.status,
                MemberStatus::Joining | MemberStatus::Down | MemberStatus::Removed
            )
            && self.scope.includes(member)
    }

    fn add_remote_route(&mut self, member: &Member) {
        let node = member.unique_address.clone();
        if node == self.self_node || !self.eligible(member) {
            return;
        }
        let key = node.ordering_key();
        if self.remote_routes.contains_key(&key) {
            return;
        }
        let Some(factory) = &self.remote_target_factory else {
            return;
        };
        let Ok(singleton) = factory(&node) else {
            return;
        };
        let _ = self.proxy.tell(SingletonProxyMsg::RegisterTarget {
            node: node.clone(),
            singleton,
        });
        self.remote_routes.insert(key, node);
    }

    fn remove_remote_route(&mut self, node: &UniqueAddress) {
        if self.remote_routes.remove(&node.ordering_key()).is_some() {
            let _ = self
                .proxy
                .tell(SingletonProxyMsg::RemoveRoute { node: node.clone() });
        }
    }

    fn request_local_singleton(&self) {
        if let Some(reply_to) = &self.singleton_reply {
            let _ = self.manager.tell(LocalSingletonManagerMsg::GetSingleton {
                reply_to: reply_to.clone(),
            });
        }
    }

    fn apply_cluster_event(&mut self, event: &ClusterEvent) {
        if self.tracker.is_none() {
            return;
        }
        if let ClusterEvent::Member(member_event) = event {
            match member_event {
                MemberEvent::Up(member) => self.add_remote_route(member),
                MemberEvent::Removed { member, .. } => {
                    self.remove_remote_route(&member.unique_address);
                    let _ = self.manager.tell(LocalSingletonManagerMsg::MarkRemoved {
                        node: member.unique_address.clone(),
                        reply_to: None,
                    });
                }
                MemberEvent::Joined(_)
                | MemberEvent::WeaklyUp(_)
                | MemberEvent::Left(_)
                | MemberEvent::Exited(_)
                | MemberEvent::Downed(_) => {}
            }
        }
        if let Some(change) = self
            .tracker
            .as_mut()
            .and_then(|tracker| tracker.apply_cluster_event(event))
        {
            let _ = self
                .manager
                .tell(LocalSingletonManagerMsg::ApplyOldestChange {
                    change: change.clone(),
                    reply_to: None,
                });
            let _ = self
                .proxy
                .tell(SingletonProxyMsg::ApplyOldestChange { change });
        }
        self.request_local_singleton();
    }
}

impl<M> Actor for ClusterSingletonConnector<M>
where
    M: Clone + Send + 'static,
{
    type Msg = ClusterSingletonConnectorMsg<M>;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription = ctx.message_adapter(ClusterSingletonConnectorMsg::Cluster)?;
        let singleton_reply = ctx.message_adapter(ClusterSingletonConnectorMsg::LocalSingleton)?;
        self.subscription = Some(subscription.clone());
        self.singleton_reply = Some(singleton_reply);
        self.cluster
            .subscribe_with_initial_state(subscription, ClusterSubscriptionInitialState::Snapshot)
            .map_err(|error| ActorError::Message(error.to_string()))?;
        ctx.start_timer_with_fixed_delay(
            ROUTE_REFRESH_TIMER_KEY,
            self.route_refresh_interval,
            self.route_refresh_interval,
            ClusterSingletonConnectorMsg::RefreshRoute,
        );
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        if let Some(subscription) = self.subscription.take() {
            let _ = self.cluster.unsubscribe(subscription);
        }
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterSingletonConnectorMsg::Cluster(event) => match event {
                ClusterSubscriptionEvent::CurrentState(state) => {
                    for member in &state.members {
                        self.add_remote_route(member);
                    }
                    let (tracker, observation) = SingletonOldestTracker::from_members(
                        self.self_node.clone(),
                        self.scope.clone(),
                        state.members,
                    );
                    self.tracker = Some(tracker);
                    let _ = self
                        .manager
                        .tell(LocalSingletonManagerMsg::ApplyInitialObservation {
                            observation: observation.clone(),
                            reply_to: None,
                        });
                    let _ = self
                        .proxy
                        .tell(SingletonProxyMsg::ApplyInitialObservation { observation });
                    self.request_local_singleton();
                }
                ClusterSubscriptionEvent::Event(event) => {
                    let self_removed = matches!(
                        &event,
                        ClusterEvent::Member(MemberEvent::Removed { member, .. })
                            if member.unique_address == self.self_node
                    );
                    self.apply_cluster_event(&event);
                    if self_removed {
                        ctx.stop(ctx.myself())?;
                    }
                }
            },
            ClusterSingletonConnectorMsg::LocalSingleton(singleton) => match singleton {
                Some(singleton) => {
                    self.local_route_present = true;
                    let _ = self
                        .delivery
                        .tell(SingletonDeliveryMsg::Update(Some(singleton.clone())));
                    let _ = self.proxy.tell(SingletonProxyMsg::RegisterRoute {
                        node: self.self_node.clone(),
                        singleton,
                    });
                }
                None if self.local_route_present => {
                    self.local_route_present = false;
                    let _ = self.delivery.tell(SingletonDeliveryMsg::Update(None));
                    let _ = self.proxy.tell(SingletonProxyMsg::RemoveRoute {
                        node: self.self_node.clone(),
                    });
                }
                None => {}
            },
            ClusterSingletonConnectorMsg::RefreshRoute => self.request_local_singleton(),
            ClusterSingletonConnectorMsg::Snapshot { reply_to } => {
                let _ = reply_to.tell(ClusterSingletonConnectorSnapshot {
                    oldest: self
                        .tracker
                        .as_ref()
                        .and_then(|tracker| tracker.current_oldest().cloned()),
                    remote_routes: self.remote_routes.values().cloned().collect(),
                    local_route_present: self.local_route_present,
                });
            }
        }
        Ok(())
    }
}

pub(super) fn singleton_remote_target_factory<M>(
    manager_path: String,
    registry: Arc<Registry>,
    outbound: Arc<dyn RemoteOutbound>,
) -> SingletonRemoteTargetFactory<M>
where
    M: RemoteMessage + Send + 'static,
{
    Arc::new(move |node| {
        let recipient = ActorRefWireData::new(format!("{}{}", node.address, manager_path))
            .map_err(|error| ActorError::Message(error.to_string()))?;
        let target_path = ActorPath::new(format!("{}/singleton", recipient.path()));
        Ok(SingletonProxyTarget::from_recipient(
            target_path,
            SingletonMessageRemoteOutbound {
                recipient,
                registry: registry.clone(),
                outbound: outbound.clone(),
                message: std::marker::PhantomData,
            },
        ))
    })
}

#[derive(Clone)]
struct SingletonMessageRemoteOutbound<M>
where
    M: Send + 'static,
{
    recipient: ActorRefWireData,
    registry: Arc<Registry>,
    outbound: Arc<dyn RemoteOutbound>,
    message: std::marker::PhantomData<fn(M)>,
}

impl<M> Recipient<M> for SingletonMessageRemoteOutbound<M>
where
    M: RemoteMessage + Send + 'static,
{
    fn tell(&self, message: M) -> Result<(), SendError<M>> {
        let inner = match self.registry.serialize(&message) {
            Ok(inner) => inner,
            Err(error) => return Err(SendError::new(message, error.to_string())),
        };
        let wire = match self
            .registry
            .serialize(&SingletonMessageEnvelope { message: inner })
        {
            Ok(wire) => wire,
            Err(error) => return Err(SendError::new(message, error.to_string())),
        };
        self.outbound
            .send(RemoteEnvelope::new(self.recipient.clone(), None, wire))
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}
