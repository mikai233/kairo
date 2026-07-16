#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};
use kairo_cluster::{
    Cluster, ClusterEvent, ClusterSubscriptionEvent, ClusterSubscriptionInitialState, Member,
    MemberEvent, MemberStatus, UniqueAddress,
};
use kairo_remote::RemoteOutbound;
use kairo_serialization::{Registry, RemoteMessage};

use crate::{
    DistributedPubSubMediatorMsg, PubSubGossipMsg, PubSubGossipPeer, PubSubGossipWireOutbound,
    PubSubRegistryDelta, PubSubRemoteDeliveryOutbound, PubSubRemoteEnvelopeOutbound,
    PubSubRemoteTarget,
};

const GOSSIP_TIMER_KEY: &str = "distributed-pubsub-gossip";

pub(super) struct DistributedPubSubConnector<M>
where
    M: Send + 'static,
{
    cluster: Cluster,
    self_node: UniqueAddress,
    role: Option<String>,
    gossip_interval: Duration,
    registry: Arc<Registry>,
    outbound: Arc<dyn RemoteOutbound>,
    gossip: ActorRef<PubSubGossipMsg>,
    mediator: ActorRef<DistributedPubSubMediatorMsg<M>>,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    peers: BTreeMap<String, UniqueAddress>,
}

#[derive(Clone)]
/// Internal membership and gossip-control protocol exposed for diagnostics.
pub enum DistributedPubSubConnectorMsg {
    /// Applies the initial cluster snapshot or one later cluster event.
    ///
    /// Pekko-compatible snapshots admit role-matching members except those
    /// still `Joining`; later `Up`/`WeaklyUp` events add peers and
    /// `Left`/`Downed`/`Removed` events remove them.
    Cluster(ClusterSubscriptionEvent),
    /// Merges an accepted remote registry delta into the delivery mediator.
    RemoteDelta(PubSubRegistryDelta),
    /// Asks the gossip actor to select peers and exchange registry state.
    GossipTick,
    /// Replies with the connector's current eligible peer set.
    Snapshot {
        /// Recipient for the diagnostic snapshot.
        reply_to: ActorRef<DistributedPubSubConnectorSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Diagnostic snapshot of peers currently installed for gossip and delivery.
pub struct DistributedPubSubConnectorSnapshot {
    /// Role-eligible remote member incarnations in deterministic address order.
    pub peers: Vec<UniqueAddress>,
}

pub(super) struct DistributedPubSubConnectorConfig<M>
where
    M: Send + 'static,
{
    pub(super) cluster: Cluster,
    pub(super) self_node: UniqueAddress,
    pub(super) role: Option<String>,
    pub(super) gossip_interval: Duration,
    pub(super) registry: Arc<Registry>,
    pub(super) outbound: Arc<dyn RemoteOutbound>,
    pub(super) gossip: ActorRef<PubSubGossipMsg>,
    pub(super) mediator: ActorRef<DistributedPubSubMediatorMsg<M>>,
}

impl<M> DistributedPubSubConnector<M>
where
    M: Send + 'static,
{
    pub(super) fn new(config: DistributedPubSubConnectorConfig<M>) -> Self {
        Self {
            cluster: config.cluster,
            self_node: config.self_node,
            role: config.role,
            gossip_interval: config.gossip_interval,
            registry: config.registry,
            outbound: config.outbound,
            gossip: config.gossip,
            mediator: config.mediator,
            subscription: None,
            peers: BTreeMap::new(),
        }
    }

    fn eligible(&self, member: &Member) -> bool {
        !matches!(member.status, MemberStatus::Joining)
            && self.role.as_ref().is_none_or(|role| member.has_role(role))
    }

    fn add_peer(&mut self, member: &Member)
    where
        M: RemoteMessage,
    {
        let node = member.unique_address.clone();
        if node.address == self.self_node.address || !self.eligible(member) {
            return;
        }
        let key = node.ordering_key();
        if self.peers.contains_key(&key) {
            return;
        }
        let envelope = PubSubRemoteEnvelopeOutbound::from_arc(self.outbound.clone());
        let wire = PubSubGossipWireOutbound::new(node.clone(), self.registry.clone(), envelope);
        let delivery = PubSubRemoteDeliveryOutbound::from_arc(
            node.clone(),
            self.registry.clone(),
            self.outbound.clone(),
        );
        let _ = self.gossip.tell(PubSubGossipMsg::AddPeer {
            peer: PubSubGossipPeer::new(node.clone(), wire),
        });
        let _ = self
            .mediator
            .tell(DistributedPubSubMediatorMsg::AddRemoteTarget {
                target: PubSubRemoteTarget::new(node.clone(), delivery),
            });
        self.peers.insert(key, node);
    }

    fn remove_peer(&mut self, node: &UniqueAddress) {
        self.peers.remove(&node.ordering_key());
        let _ = self
            .gossip
            .tell(PubSubGossipMsg::RemovePeer { node: node.clone() });
        let _ = self
            .mediator
            .tell(DistributedPubSubMediatorMsg::RemoveRemoteMediator { node: node.clone() });
    }

    fn apply_event(&mut self, event: &ClusterEvent)
    where
        M: RemoteMessage,
    {
        let ClusterEvent::Member(event) = event else {
            return;
        };
        match event {
            MemberEvent::Up(member) | MemberEvent::WeaklyUp(member) => self.add_peer(member),
            MemberEvent::Left(member)
            | MemberEvent::Downed(member)
            | MemberEvent::Removed { member, .. } => self.remove_peer(&member.unique_address),
            MemberEvent::Joined(_) | MemberEvent::Exited(_) => {}
        }
    }
}

impl<M> Actor for DistributedPubSubConnector<M>
where
    M: Clone + RemoteMessage + Send + 'static,
{
    type Msg = DistributedPubSubConnectorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription = ctx.message_adapter(DistributedPubSubConnectorMsg::Cluster)?;
        let delta = ctx.message_adapter(DistributedPubSubConnectorMsg::RemoteDelta)?;
        self.subscription = Some(subscription.clone());
        self.cluster
            .subscribe_with_initial_state(subscription, ClusterSubscriptionInitialState::Snapshot)
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.gossip
            .tell(PubSubGossipMsg::SetDeltaSink { sink: delta })
            .map_err(|error| ActorError::Message(error.reason().to_string()))?;
        ctx.start_timer_with_fixed_delay(
            GOSSIP_TIMER_KEY,
            self.gossip_interval,
            self.gossip_interval,
            DistributedPubSubConnectorMsg::GossipTick,
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
            DistributedPubSubConnectorMsg::Cluster(event) => {
                match &event {
                    ClusterSubscriptionEvent::CurrentState(state) => {
                        for member in &state.members {
                            self.add_peer(member);
                        }
                    }
                    ClusterSubscriptionEvent::Event(event) => self.apply_event(event),
                }
                if let ClusterSubscriptionEvent::Event(ClusterEvent::Member(MemberEvent::Removed {
                    member,
                    ..
                })) = &event
                    && member.unique_address.address == self.self_node.address
                {
                    if let ClusterSubscriptionEvent::Event(event) = event {
                        let _ = self
                            .mediator
                            .tell(DistributedPubSubMediatorMsg::ApplyClusterEvent { event });
                    }
                    ctx.stop(self.gossip.clone())?;
                    ctx.stop(ctx.myself())?;
                } else if let ClusterSubscriptionEvent::Event(event) = event {
                    let _ = self
                        .mediator
                        .tell(DistributedPubSubMediatorMsg::ApplyClusterEvent { event });
                }
            }
            DistributedPubSubConnectorMsg::RemoteDelta(delta) => {
                self.mediator
                    .tell(DistributedPubSubMediatorMsg::MergeDelta { delta })
                    .map_err(|error| ActorError::Message(error.reason().to_string()))?;
            }
            DistributedPubSubConnectorMsg::GossipTick => {
                let _ = self.gossip.tell(PubSubGossipMsg::GossipTick);
            }
            DistributedPubSubConnectorMsg::Snapshot { reply_to } => {
                let _ = reply_to.tell(DistributedPubSubConnectorSnapshot {
                    peers: self.peers.values().cloned().collect(),
                });
            }
        }
        Ok(())
    }
}
