#![deny(missing_docs)]

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

use kairo_actor::ActorRef;

use crate::daemon_bootstrap::ClusterDaemonMsg;
use crate::{
    ClusterEvent, ClusterEventPublisherMsg, ClusterMembershipMsg, ClusterSubscriptionEvent,
    ClusterSubscriptionInitialState, CurrentClusterState, SubscriptionInitialState, UniqueAddress,
};

#[derive(Debug, Clone)]
struct ClusterControls {
    self_node: UniqueAddress,
    membership: ActorRef<ClusterMembershipMsg>,
    daemon: ActorRef<ClusterDaemonMsg>,
}

#[derive(Debug, Clone)]
/// Public typed facade for cluster observation and optional membership control.
///
/// [`Self::new`] creates an event-only facade. ActorSystem-installed cluster
/// extensions also attach the one-shot join and membership control routes.
pub struct Cluster {
    event_publisher: ActorRef<ClusterEventPublisherMsg>,
    controls: Option<ClusterControls>,
}

impl Cluster {
    /// Creates an event-only facade backed by `event_publisher`.
    pub fn new(event_publisher: ActorRef<ClusterEventPublisherMsg>) -> Self {
        Self {
            event_publisher,
            controls: None,
        }
    }

    pub(crate) fn with_membership(
        event_publisher: ActorRef<ClusterEventPublisherMsg>,
        self_node: UniqueAddress,
        membership: ActorRef<ClusterMembershipMsg>,
        daemon: ActorRef<ClusterDaemonMsg>,
    ) -> Self {
        Self {
            event_publisher,
            controls: Some(ClusterControls {
                self_node,
                membership,
                daemon,
            }),
        }
    }

    /// Returns the underlying typed event-publisher actor reference.
    pub fn event_publisher(&self) -> ActorRef<ClusterEventPublisherMsg> {
        self.event_publisher.clone()
    }

    /// Returns the local node incarnation when membership controls are attached.
    pub fn self_node(&self) -> Result<&UniqueAddress, ClusterError> {
        self.controls
            .as_ref()
            .map(|controls| &controls.self_node)
            .ok_or(ClusterError::ControlUnavailable)
    }

    /// Begins graceful leave for the local member.
    pub fn leave_self(&self) -> Result<(), ClusterError> {
        let controls = self.controls()?;
        self.send_to_membership(ClusterMembershipMsg::Leave {
            address: controls.self_node.address.clone(),
        })
    }

    /// Starts the daemon-owned one-shot join flow through `address`.
    ///
    /// The address must use the local remoting protocol and must include a host
    /// unless it is exactly the local canonical address.
    pub fn join(&self, address: kairo_actor::Address) -> Result<(), ClusterError> {
        let controls = self.controls()?;
        if address.protocol() != controls.self_node.address.protocol()
            || (address != controls.self_node.address && address.host().is_none())
        {
            return Err(ClusterError::InvalidJoinAddress {
                address: address.to_string(),
            });
        }
        controls
            .daemon
            .tell(ClusterDaemonMsg::JoinTo { address })
            .map_err(|error| ClusterError::DaemonUnavailable {
                reason: error.reason().to_string(),
            })
    }

    /// Requests graceful leave for the member at `address`.
    pub fn leave(&self, address: kairo_actor::Address) -> Result<(), ClusterError> {
        self.send_to_membership(ClusterMembershipMsg::Leave { address })
    }

    /// Requests that the member at `address` be marked down.
    pub fn down(&self, address: kairo_actor::Address) -> Result<(), ClusterError> {
        self.send_to_membership(ClusterMembershipMsg::DownAddress { address })
    }

    /// Subscribes to a current-state snapshot followed by cluster events.
    pub fn subscribe(
        &self,
        subscriber: ActorRef<ClusterSubscriptionEvent>,
    ) -> Result<(), ClusterError> {
        self.subscribe_with_initial_state(subscriber, ClusterSubscriptionInitialState::Snapshot)
    }

    /// Subscribes to cluster events with an explicit initial-state mode.
    pub fn subscribe_with_initial_state(
        &self,
        subscriber: ActorRef<ClusterSubscriptionEvent>,
        initial_state: ClusterSubscriptionInitialState,
    ) -> Result<(), ClusterError> {
        self.send_to_publisher(ClusterEventPublisherMsg::SubscribeCluster {
            subscriber,
            initial_state,
        })
    }

    /// Subscribes to raw domain events, optionally replaying current state as events.
    pub fn subscribe_events(
        &self,
        subscriber: ActorRef<ClusterEvent>,
        initial_state: SubscriptionInitialState,
    ) -> Result<(), ClusterError> {
        self.send_to_publisher(ClusterEventPublisherMsg::Subscribe {
            subscriber,
            initial_state,
        })
    }

    /// Removes a [`ClusterSubscriptionEvent`] subscription.
    pub fn unsubscribe(
        &self,
        subscriber: ActorRef<ClusterSubscriptionEvent>,
    ) -> Result<(), ClusterError> {
        self.send_to_publisher(ClusterEventPublisherMsg::UnsubscribeCluster { subscriber })
    }

    /// Removes a raw [`ClusterEvent`] subscription.
    pub fn unsubscribe_events(
        &self,
        subscriber: ActorRef<ClusterEvent>,
    ) -> Result<(), ClusterError> {
        self.send_to_publisher(ClusterEventPublisherMsg::Unsubscribe { subscriber })
    }

    /// Sends the latest full cluster snapshot to `reply_to` once.
    pub fn send_current_state(
        &self,
        reply_to: ActorRef<CurrentClusterState>,
    ) -> Result<(), ClusterError> {
        self.send_to_publisher(ClusterEventPublisherMsg::SendCurrentState { reply_to })
    }

    fn send_to_publisher(&self, message: ClusterEventPublisherMsg) -> Result<(), ClusterError> {
        self.event_publisher
            .tell(message)
            .map_err(|error| ClusterError::PublisherUnavailable {
                reason: error.reason().to_string(),
            })
    }

    fn controls(&self) -> Result<&ClusterControls, ClusterError> {
        self.controls
            .as_ref()
            .ok_or(ClusterError::ControlUnavailable)
    }

    fn send_to_membership(&self, message: ClusterMembershipMsg) -> Result<(), ClusterError> {
        self.controls()?.membership.tell(message).map_err(|error| {
            ClusterError::MembershipUnavailable {
                reason: error.reason().to_string(),
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Failure reported by the public cluster facade.
pub enum ClusterError {
    /// The facade was created without membership control routes.
    ControlUnavailable,
    /// The cluster daemon actor route could not accept a join command.
    DaemonUnavailable {
        /// Error reported by the daemon actor route.
        reason: String,
    },
    /// The requested join address cannot identify a compatible remote system.
    InvalidJoinAddress {
        /// Rejected address formatted for diagnostics.
        address: String,
    },
    /// The membership actor route could not accept a leave or down command.
    MembershipUnavailable {
        /// Error reported by the membership actor route.
        reason: String,
    },
    /// The event-publisher route could not accept a subscription or state
    /// request.
    PublisherUnavailable {
        /// Error reported by the event-publisher route.
        reason: String,
    },
}

impl Display for ClusterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ControlUnavailable => {
                write!(f, "cluster membership controls are unavailable")
            }
            Self::DaemonUnavailable { reason } => {
                write!(f, "cluster daemon is unavailable: {reason}")
            }
            Self::InvalidJoinAddress { address } => {
                write!(f, "invalid cluster join address `{address}`")
            }
            Self::MembershipUnavailable { reason } => {
                write!(f, "cluster membership actor is unavailable: {reason}")
            }
            Self::PublisherUnavailable { reason } => {
                write!(f, "cluster event publisher is unavailable: {reason}")
            }
        }
    }
}

impl Error for ClusterError {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kairo_actor::{Address, Props};
    use kairo_testkit::ActorSystemTestKit;

    use super::*;
    use crate::{ClusterEventPublisher, Gossip, Member, MemberEvent, MemberStatus, UniqueAddress};

    #[test]
    fn subscribe_sends_snapshot_by_default_and_then_later_events() {
        let kit = ActorSystemTestKit::new("cluster-facade-snapshot").unwrap();
        let self_node = node("self", 1);
        let node_b = node("b", 2);
        let publisher = spawn_publisher(&kit, self_node.clone(), "publisher");
        let cluster = Cluster::new(publisher.clone());
        let subscriber = kit
            .create_probe::<ClusterSubscriptionEvent>("subscriber")
            .unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([member(self_node.clone(), MemberStatus::Up)]),
            ))
            .unwrap();
        cluster.subscribe(subscriber.actor_ref()).unwrap();

        let ClusterSubscriptionEvent::CurrentState(state) =
            subscriber.expect_msg(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected current cluster state");
        };
        assert_eq!(state.members.len(), 1);

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([
                    member(self_node, MemberStatus::Up),
                    member(node_b, MemberStatus::Up),
                ]),
            ))
            .unwrap();

        assert!(matches!(
            subscriber.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterSubscriptionEvent::Event(ClusterEvent::Member(MemberEvent::Up(_)))
        ));
    }

    #[test]
    fn subscribe_with_events_replays_current_members_as_events() {
        let kit = ActorSystemTestKit::new("cluster-facade-events").unwrap();
        let self_node = node("self", 1);
        let publisher = spawn_publisher(&kit, self_node.clone(), "publisher");
        let cluster = Cluster::new(publisher.clone());
        let subscriber = kit
            .create_probe::<ClusterSubscriptionEvent>("subscriber")
            .unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([member(self_node, MemberStatus::Up)]),
            ))
            .unwrap();
        cluster
            .subscribe_with_initial_state(
                subscriber.actor_ref(),
                ClusterSubscriptionInitialState::Events,
            )
            .unwrap();

        assert!(matches!(
            subscriber.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterSubscriptionEvent::Event(ClusterEvent::Member(MemberEvent::Up(_)))
        ));
    }

    #[test]
    fn unsubscribe_stops_cluster_subscription_events() {
        let kit = ActorSystemTestKit::new("cluster-facade-unsubscribe").unwrap();
        let self_node = node("self", 1);
        let publisher = spawn_publisher(&kit, self_node, "publisher");
        let cluster = Cluster::new(publisher.clone());
        let subscriber = kit
            .create_probe::<ClusterSubscriptionEvent>("subscriber")
            .unwrap();

        cluster
            .subscribe_with_initial_state(
                subscriber.actor_ref(),
                ClusterSubscriptionInitialState::None,
            )
            .unwrap();
        cluster.unsubscribe(subscriber.actor_ref()).unwrap();
        publisher
            .tell(ClusterEventPublisherMsg::PublishEvent(
                ClusterEvent::LeaderChanged { leader: None },
            ))
            .unwrap();

        subscriber.expect_no_msg(Duration::from_millis(50)).unwrap();
    }

    #[test]
    fn send_current_state_requests_snapshot() {
        let kit = ActorSystemTestKit::new("cluster-facade-current-state").unwrap();
        let self_node = node("self", 1);
        let publisher = spawn_publisher(&kit, self_node.clone(), "publisher");
        let cluster = Cluster::new(publisher.clone());
        let state_probe = kit.create_probe::<CurrentClusterState>("state").unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([member(self_node, MemberStatus::Up)]),
            ))
            .unwrap();
        cluster.send_current_state(state_probe.actor_ref()).unwrap();

        assert_eq!(
            state_probe
                .expect_msg(Duration::from_secs(1))
                .unwrap()
                .members
                .len(),
            1
        );
    }

    #[test]
    fn facade_reports_stopped_publisher() {
        let kit = ActorSystemTestKit::new("cluster-facade-stopped-publisher").unwrap();
        let self_node = node("self", 1);
        let publisher = spawn_publisher(&kit, self_node, "publisher");
        let cluster = Cluster::new(publisher.clone());
        let subscriber = kit
            .create_probe::<ClusterSubscriptionEvent>("subscriber")
            .unwrap();

        kit.system().stop(&publisher);
        assert!(publisher.wait_for_stop(Duration::from_secs(1)));

        let error = cluster.subscribe(subscriber.actor_ref()).unwrap_err();
        assert_eq!(
            error,
            ClusterError::PublisherUnavailable {
                reason: "actor is stopped".to_string()
            }
        );
    }

    #[test]
    fn controlled_facade_exposes_self_leave_and_down_operations() {
        let kit = ActorSystemTestKit::new("cluster-facade-controls").unwrap();
        let self_node = node("self", 1);
        let peer = UniqueAddress::new(
            Address::new("kairo", "peer", Some("127.0.0.1".to_string()), Some(2552)),
            2,
        );
        let publisher = spawn_publisher(&kit, self_node.clone(), "publisher");
        let membership = kit
            .create_probe::<ClusterMembershipMsg>("membership")
            .unwrap();
        let daemon = kit.create_probe::<ClusterDaemonMsg>("daemon").unwrap();
        let cluster = Cluster::with_membership(
            publisher,
            self_node.clone(),
            membership.actor_ref(),
            daemon.actor_ref(),
        );

        assert_eq!(cluster.self_node().unwrap(), &self_node);
        cluster.join(peer.address.clone()).unwrap();
        assert!(matches!(
            daemon.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterDaemonMsg::JoinTo { address } if address == peer.address
        ));
        cluster.leave_self().unwrap();
        assert!(matches!(
            membership.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterMembershipMsg::Leave { address } if address == self_node.address
        ));
        cluster.down(peer.address.clone()).unwrap();
        assert!(matches!(
            membership.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterMembershipMsg::DownAddress { address } if address == peer.address
        ));
    }

    #[test]
    fn event_only_facade_rejects_membership_controls() {
        let kit = ActorSystemTestKit::new("cluster-facade-event-only").unwrap();
        let self_node = node("self", 1);
        let publisher = spawn_publisher(&kit, self_node, "publisher");
        let cluster = Cluster::new(publisher);

        assert_eq!(cluster.self_node(), Err(ClusterError::ControlUnavailable));
        assert_eq!(cluster.leave_self(), Err(ClusterError::ControlUnavailable));
    }

    fn spawn_publisher(
        kit: &ActorSystemTestKit,
        self_node: UniqueAddress,
        name: &str,
    ) -> ActorRef<ClusterEventPublisherMsg> {
        kit.system()
            .spawn(
                name,
                Props::new(move || ClusterEventPublisher::new(self_node.clone())),
            )
            .unwrap()
    }

    fn member(unique_address: UniqueAddress, status: MemberStatus) -> Member {
        Member::new(unique_address, Vec::new()).with_status(status)
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }
}
