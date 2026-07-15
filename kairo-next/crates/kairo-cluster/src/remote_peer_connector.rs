#![deny(missing_docs)]

use std::collections::VecDeque;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};
use kairo_remote::TcpRemotePeerManager;

use crate::{
    Cluster, ClusterAssociationPeerChange, ClusterAssociationPeerState,
    ClusterAssociationPeerTarget, ClusterSubscriptionEvent, ClusterSubscriptionInitialState,
    UniqueAddress,
};

const PEER_REMOVAL_REASON: &str = "cluster membership removed managed peer";

#[derive(Debug, Clone)]
/// Commands accepted by the cluster-to-remoting peer connector actor.
pub enum ClusterRemotePeerConnectorMsg {
    /// Applies a current cluster snapshot or subsequent domain event.
    Cluster(ClusterSubscriptionEvent),
    /// Completes the serialized transport command currently in flight.
    CommandComplete(Result<(), String>),
    /// Requests a diagnostic snapshot of desired peers and queued transport work.
    Snapshot {
        /// Recipient for the diagnostic snapshot.
        reply_to: ActorRef<ClusterRemotePeerConnectorSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Diagnostic state of a cluster-to-remoting peer connector.
pub struct ClusterRemotePeerConnectorSnapshot {
    /// Membership-derived peers that the connector currently intends to manage.
    pub desired_targets: Vec<ClusterAssociationPeerTarget>,
    /// Number of transport changes waiting behind the current command.
    pub queued_commands: usize,
    /// Whether a blocking connect or disconnect operation is running outside the actor turn.
    pub command_in_flight: bool,
    /// Most recent transport-command failure, cleared by the next successful command.
    pub last_error: Option<String>,
}

/// Bridges authoritative cluster events into the shared remoting peer manager.
///
/// Membership remains owned by gossip. This actor only turns the derived
/// reachable-member set into managed transport intent, and serializes blocking
/// connect attempts outside synchronous actor turns.
pub struct ClusterRemotePeerConnector {
    cluster: Cluster,
    peers: ClusterAssociationPeerState,
    peer_manager: TcpRemotePeerManager,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    commands: VecDeque<ClusterAssociationPeerChange>,
    command_in_flight: bool,
    last_error: Option<String>,
}

impl ClusterRemotePeerConnector {
    /// Creates a connector for one cluster facade and shared remote peer manager.
    ///
    /// Locally unreachable peers are retained so heartbeat traffic can drive recovery; membership
    /// reachability itself remains owned by gossip and the failure detector.
    pub fn new(
        cluster: Cluster,
        self_node: UniqueAddress,
        peer_manager: TcpRemotePeerManager,
    ) -> Self {
        Self {
            cluster,
            peers: ClusterAssociationPeerState::new(self_node)
                .with_unreachable_peers_retained(true),
            peer_manager,
            subscription: None,
            commands: VecDeque::new(),
            command_in_flight: false,
            last_error: None,
        }
    }

    fn apply_cluster_event(
        &mut self,
        ctx: &Context<ClusterRemotePeerConnectorMsg>,
        event: ClusterSubscriptionEvent,
    ) -> ActorResult {
        let changes = match event {
            ClusterSubscriptionEvent::CurrentState(state) => self.peers.apply_snapshot(state),
            ClusterSubscriptionEvent::Event(event) => self.peers.apply_event(event),
        }
        .map_err(|error| ActorError::Message(error.to_string()))?;
        self.commands.extend(changes);
        self.start_next_command(ctx)
    }

    fn start_next_command(&mut self, ctx: &Context<ClusterRemotePeerConnectorMsg>) -> ActorResult {
        if self.command_in_flight {
            return Ok(());
        }
        let Some(command) = self.commands.pop_front() else {
            return Ok(());
        };
        self.command_in_flight = true;
        let peer_manager = self.peer_manager.clone();
        ctx.spawn_task(move |myself| {
            let result =
                apply_peer_change(&peer_manager, command).map_err(|error| error.to_string());
            let _ = myself.tell(ClusterRemotePeerConnectorMsg::CommandComplete(result));
        })?;
        Ok(())
    }

    fn complete_command(
        &mut self,
        ctx: &Context<ClusterRemotePeerConnectorMsg>,
        result: Result<(), String>,
    ) -> ActorResult {
        self.command_in_flight = false;
        match result {
            Ok(()) => self.last_error = None,
            Err(error) => self.last_error = Some(error),
        }
        self.start_next_command(ctx)
    }

    fn snapshot(&self) -> ClusterRemotePeerConnectorSnapshot {
        ClusterRemotePeerConnectorSnapshot {
            desired_targets: self.peers.active_targets(),
            queued_commands: self.commands.len(),
            command_in_flight: self.command_in_flight,
            last_error: self.last_error.clone(),
        }
    }
}

impl Actor for ClusterRemotePeerConnector {
    type Msg = ClusterRemotePeerConnectorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription = ctx.message_adapter(ClusterRemotePeerConnectorMsg::Cluster)?;
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
        self.commands.clear();
        for target in self.peers.active_targets() {
            let _ = self
                .peer_manager
                .disconnect(target.association(), PEER_REMOVAL_REASON);
        }
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterRemotePeerConnectorMsg::Cluster(event) => self.apply_cluster_event(ctx, event),
            ClusterRemotePeerConnectorMsg::CommandComplete(result) => {
                self.complete_command(ctx, result)
            }
            ClusterRemotePeerConnectorMsg::Snapshot { reply_to } => reply_to
                .tell(self.snapshot())
                .map_err(|error| ActorError::Message(error.reason().to_string())),
        }
    }
}

fn apply_peer_change(
    manager: &TcpRemotePeerManager,
    change: ClusterAssociationPeerChange,
) -> kairo_remote::Result<()> {
    match change {
        ClusterAssociationPeerChange::Dial(target) => manager.connect(target.association().clone()),
        ClusterAssociationPeerChange::Remove(target) => manager
            .disconnect(target.association(), PEER_REMOVAL_REASON)
            .map(|_| ()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use kairo_actor::{ActorSystem, Address, Props};
    use kairo_remote::{RemoteSettings, TcpRemoteActorRuntime, register_remote_protocol_codecs};
    use kairo_serialization::Registry;
    use kairo_testkit::{ActorSystemTestKit, await_assert};

    use super::*;
    use crate::{
        ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus,
        test_support::cluster_socket_test_lock,
    };

    fn node(system: &str, settings: &RemoteSettings, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new(
                "kairo",
                system,
                Some(settings.canonical_hostname.clone()),
                Some(settings.canonical_port),
            ),
            uid,
        )
    }

    fn member(node: UniqueAddress) -> Member {
        Member::new(node, vec![]).with_status(MemberStatus::Up)
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_remote_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    #[test]
    fn connector_derives_shared_runtime_routes_from_cluster_membership() {
        let _socket_guard = cluster_socket_test_lock();
        let local_kit = ActorSystemTestKit::new("remote-peer-local").unwrap();
        let remote_system = ActorSystem::builder("remote-peer-remote").build().unwrap();
        let registry = registry();
        let local_runtime = TcpRemoteActorRuntime::builder(
            local_kit.system().clone(),
            registry.clone(),
            RemoteSettings::new("127.0.0.1", 0),
            11,
        )
        .bind()
        .unwrap();
        let remote_runtime = TcpRemoteActorRuntime::builder(
            remote_system.clone(),
            registry,
            RemoteSettings::new("127.0.0.1", 0),
            22,
        )
        .bind()
        .unwrap();
        let self_node = node("remote-peer-local", local_runtime.settings(), 1);
        let peer = node("remote-peer-remote", remote_runtime.settings(), 2);
        let publisher = local_kit
            .system()
            .spawn(
                "publisher",
                Props::new({
                    let self_node = self_node.clone();
                    move || ClusterEventPublisher::new(self_node.clone())
                }),
            )
            .unwrap();
        let cluster = Cluster::new(publisher.clone());
        let connector = local_kit
            .system()
            .spawn(
                "remote-peer-connector",
                Props::new({
                    let cluster = cluster.clone();
                    let self_node = self_node.clone();
                    let manager = local_runtime.peer_manager();
                    move || {
                        ClusterRemotePeerConnector::new(
                            cluster.clone(),
                            self_node.clone(),
                            manager.clone(),
                        )
                    }
                }),
            )
            .unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([member(self_node.clone()), member(peer.clone())]),
            ))
            .unwrap();
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            local_runtime
                .peer_manager()
                .is_connected(
                    &ClusterAssociationPeerTarget::new(peer.clone())
                        .unwrap()
                        .association()
                        .clone(),
                )
                .then_some(())
                .ok_or_else(|| "membership route was not installed".to_string())
        })
        .unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([member(self_node)]),
            ))
            .unwrap();
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            (!local_runtime.peer_manager().is_connected(
                ClusterAssociationPeerTarget::new(peer.clone())
                    .unwrap()
                    .association(),
            ))
            .then_some(())
            .ok_or_else(|| "removed membership route is still installed".to_string())
        })
        .unwrap();

        local_kit.system().stop(&connector);
        assert!(connector.wait_for_stop(Duration::from_secs(1)));
        local_runtime.shutdown().unwrap();
        remote_runtime.shutdown().unwrap();
        remote_system.terminate(Duration::from_secs(1)).unwrap();
        local_kit.shutdown(Duration::from_secs(1)).unwrap();
    }
}
