use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};
use kairo_remote::RemoteOutbound;
use kairo_serialization::{ActorRefWireData, Registry};

use crate::{
    Cluster, ClusterEvent, ClusterMembershipMsg, ClusterSubscriptionEvent,
    ClusterSubscriptionInitialState, HeartbeatReceiverMsg, HeartbeatRemoteReceiverOutbound,
    HeartbeatSenderMsg, MemberEvent, MemberStatus, UniqueAddress,
};

#[derive(Debug, Clone)]
pub enum ClusterHeartbeatConnectorMsg {
    Cluster(ClusterSubscriptionEvent),
}

/// Owns membership-derived remote routes for the stable heartbeat sender.
pub struct ClusterHeartbeatConnector {
    cluster: Cluster,
    self_node: UniqueAddress,
    membership: ActorRef<ClusterMembershipMsg>,
    sender: ActorRef<HeartbeatSenderMsg>,
    sender_wire: ActorRefWireData,
    registry: Arc<Registry>,
    outbound: Arc<dyn RemoteOutbound>,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    routes: BTreeMap<String, (UniqueAddress, ActorRef<HeartbeatReceiverMsg>)>,
    next_route_id: u64,
}

impl ClusterHeartbeatConnector {
    pub fn new(
        cluster: Cluster,
        self_node: UniqueAddress,
        membership: ActorRef<ClusterMembershipMsg>,
        sender: ActorRef<HeartbeatSenderMsg>,
        sender_wire: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        Self {
            cluster,
            self_node,
            membership,
            sender,
            sender_wire,
            registry,
            outbound,
            subscription: None,
            routes: BTreeMap::new(),
            next_route_id: 1,
        }
    }

    fn apply_subscription(
        &mut self,
        ctx: &mut Context<ClusterHeartbeatConnectorMsg>,
        event: ClusterSubscriptionEvent,
    ) -> ActorResult {
        match event {
            ClusterSubscriptionEvent::CurrentState(state) => {
                let desired = state
                    .members
                    .iter()
                    .filter(|member| {
                        member.unique_address != self.self_node
                            && member.status != MemberStatus::Removed
                    })
                    .map(|member| member.unique_address.clone())
                    .collect::<Vec<_>>();
                self.reconcile_routes(ctx, desired)?;
                self.sender
                    .tell(HeartbeatSenderMsg::Init(state))
                    .map_err(send_error)?;
            }
            ClusterSubscriptionEvent::Event(event) => {
                self.apply_event_route(ctx, &event)?;
                self.sender
                    .tell(HeartbeatSenderMsg::ClusterEvent(event))
                    .map_err(send_error)?;
            }
        }
        Ok(())
    }

    fn apply_event_route(
        &mut self,
        ctx: &mut Context<ClusterHeartbeatConnectorMsg>,
        event: &ClusterEvent,
    ) -> ActorResult {
        let ClusterEvent::Member(member_event) = event else {
            return Ok(());
        };
        match member_event {
            MemberEvent::Removed { member, .. } => {
                self.remove_route(ctx, &member.unique_address)?;
            }
            MemberEvent::Joined(member)
            | MemberEvent::WeaklyUp(member)
            | MemberEvent::Up(member)
            | MemberEvent::Left(member)
            | MemberEvent::Exited(member)
            | MemberEvent::Downed(member) => {
                self.ensure_route(ctx, member.unique_address.clone())?;
            }
        }
        Ok(())
    }

    fn reconcile_routes(
        &mut self,
        ctx: &mut Context<ClusterHeartbeatConnectorMsg>,
        desired: Vec<UniqueAddress>,
    ) -> ActorResult {
        let desired_keys: BTreeSet<_> = desired.iter().map(UniqueAddress::ordering_key).collect();
        let removed: Vec<_> = self
            .routes
            .values()
            .filter(|(node, _)| !desired_keys.contains(&node.ordering_key()))
            .map(|(node, _)| node.clone())
            .collect();
        for node in removed {
            self.remove_route(ctx, &node)?;
        }
        for node in desired {
            self.ensure_route(ctx, node)?;
        }
        Ok(())
    }

    fn ensure_route(
        &mut self,
        ctx: &mut Context<ClusterHeartbeatConnectorMsg>,
        node: UniqueAddress,
    ) -> ActorResult {
        if node == self.self_node || self.routes.contains_key(&node.ordering_key()) {
            return Ok(());
        }
        let id = self.next_route_id;
        self.next_route_id = self.next_route_id.wrapping_add(1);
        let route = ctx.spawn(
            format!("receiver-{id}"),
            Props::new({
                let target = node.clone();
                let registry = self.registry.clone();
                let sender = self.sender_wire.clone();
                let outbound = self.outbound.clone();
                move || {
                    HeartbeatRemoteReceiverOutbound::from_arc(
                        target.clone(),
                        registry.clone(),
                        sender.clone(),
                        outbound.clone(),
                    )
                }
            }),
        )?;
        self.sender
            .tell(HeartbeatSenderMsg::RegisterReceiver {
                node: node.clone(),
                receiver: route.clone(),
            })
            .map_err(send_error)?;
        self.routes.insert(node.ordering_key(), (node, route));
        Ok(())
    }

    fn remove_route(
        &mut self,
        ctx: &mut Context<ClusterHeartbeatConnectorMsg>,
        node: &UniqueAddress,
    ) -> ActorResult {
        if node == &self.self_node {
            ctx.system().stop(&self.sender);
            ctx.stop(ctx.myself())?;
            return Ok(());
        }
        if let Some((node, route)) = self.routes.remove(&node.ordering_key()) {
            self.sender
                .tell(HeartbeatSenderMsg::UnregisterReceiver { node })
                .map_err(send_error)?;
            ctx.stop(route)?;
        }
        Ok(())
    }
}

impl Actor for ClusterHeartbeatConnector {
    type Msg = ClusterHeartbeatConnectorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.sender
            .tell(HeartbeatSenderMsg::RegisterMembership(
                self.membership.clone(),
            ))
            .map_err(send_error)?;
        let subscription = ctx.message_adapter(ClusterHeartbeatConnectorMsg::Cluster)?;
        self.cluster
            .subscribe_with_initial_state(
                subscription.clone(),
                ClusterSubscriptionInitialState::Snapshot,
            )
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.subscription = Some(subscription);
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        if let Some(subscription) = self.subscription.take() {
            let _ = self.cluster.unsubscribe(subscription);
        }
        self.routes.clear();
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterHeartbeatConnectorMsg::Cluster(event) => self.apply_subscription(ctx, event),
        }
    }
}

fn send_error<M>(error: kairo_actor::SendError<M>) -> ActorError {
    ActorError::Message(error.reason().to_string())
}
