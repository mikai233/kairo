use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

use kairo_actor::ActorRef;

use crate::{
    ClusterEvent, ClusterEventPublisherMsg, ClusterSubscriptionEvent,
    ClusterSubscriptionInitialState, CurrentClusterState, SubscriptionInitialState,
};

#[derive(Debug, Clone)]
pub struct Cluster {
    event_publisher: ActorRef<ClusterEventPublisherMsg>,
}

impl Cluster {
    pub fn new(event_publisher: ActorRef<ClusterEventPublisherMsg>) -> Self {
        Self { event_publisher }
    }

    pub fn event_publisher(&self) -> ActorRef<ClusterEventPublisherMsg> {
        self.event_publisher.clone()
    }

    pub fn subscribe(
        &self,
        subscriber: ActorRef<ClusterSubscriptionEvent>,
    ) -> Result<(), ClusterError> {
        self.subscribe_with_initial_state(subscriber, ClusterSubscriptionInitialState::Snapshot)
    }

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

    pub fn unsubscribe(
        &self,
        subscriber: ActorRef<ClusterSubscriptionEvent>,
    ) -> Result<(), ClusterError> {
        self.send_to_publisher(ClusterEventPublisherMsg::UnsubscribeCluster { subscriber })
    }

    pub fn unsubscribe_events(
        &self,
        subscriber: ActorRef<ClusterEvent>,
    ) -> Result<(), ClusterError> {
        self.send_to_publisher(ClusterEventPublisherMsg::Unsubscribe { subscriber })
    }

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterError {
    PublisherUnavailable { reason: String },
}

impl Display for ClusterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
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
