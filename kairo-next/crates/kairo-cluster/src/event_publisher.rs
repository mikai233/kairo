use std::collections::{HashMap, HashSet};

use kairo_actor::{Actor, ActorRef, ActorResult, Context};

use crate::{ClusterEvent, ClusterEvents, Gossip, LeaderSelection, Member, UniqueAddress};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionInitialState {
    None,
    Events,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterSubscriptionInitialState {
    None,
    Snapshot,
    Events,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterSubscriptionEvent {
    CurrentState(CurrentClusterState),
    Event(ClusterEvent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentClusterState {
    pub members: Vec<Member>,
    pub unreachable: Vec<Member>,
    pub seen_by: HashSet<UniqueAddress>,
    pub leader: Option<UniqueAddress>,
    pub role_leaders: HashMap<String, Option<UniqueAddress>>,
    pub member_tombstones: HashSet<UniqueAddress>,
}

impl CurrentClusterState {
    pub fn from_gossip(gossip: &Gossip, self_node: &UniqueAddress) -> Self {
        let unreachable = gossip
            .reachability()
            .all_unreachable_or_terminated()
            .into_iter()
            .filter(|node| node != self_node)
            .filter_map(|node| gossip.member(&node).cloned())
            .collect();
        let roles: HashSet<_> = gossip
            .members()
            .iter()
            .flat_map(|member| member.roles.iter().cloned())
            .collect();
        let role_leaders = roles
            .into_iter()
            .map(|role| {
                let leader = LeaderSelection::for_role(gossip, self_node, &role)
                    .leader()
                    .cloned();
                (role, leader)
            })
            .collect();

        Self {
            members: gossip.members().to_vec(),
            unreachable,
            seen_by: gossip.seen_by().clone(),
            leader: LeaderSelection::for_gossip(gossip, self_node)
                .leader()
                .cloned(),
            role_leaders,
            member_tombstones: gossip.tombstones().keys().cloned().collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClusterEventPublisherMsg {
    PublishChanges(Gossip),
    PublishEvent(ClusterEvent),
    Subscribe {
        subscriber: ActorRef<ClusterEvent>,
        initial_state: SubscriptionInitialState,
    },
    Unsubscribe {
        subscriber: ActorRef<ClusterEvent>,
    },
    SubscribeCluster {
        subscriber: ActorRef<ClusterSubscriptionEvent>,
        initial_state: ClusterSubscriptionInitialState,
    },
    UnsubscribeCluster {
        subscriber: ActorRef<ClusterSubscriptionEvent>,
    },
    SendCurrentState {
        reply_to: ActorRef<CurrentClusterState>,
    },
}

pub struct ClusterEventPublisher {
    self_node: UniqueAddress,
    gossip: Gossip,
    subscribers: Vec<ActorRef<ClusterEvent>>,
    cluster_subscribers: Vec<ActorRef<ClusterSubscriptionEvent>>,
}

impl ClusterEventPublisher {
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            self_node,
            gossip: Gossip::new(),
            subscribers: Vec::new(),
            cluster_subscribers: Vec::new(),
        }
    }

    fn subscribe(
        &mut self,
        subscriber: ActorRef<ClusterEvent>,
        initial_state: SubscriptionInitialState,
    ) {
        if !self
            .subscribers
            .iter()
            .any(|existing| existing.path() == subscriber.path())
        {
            self.subscribers.push(subscriber.clone());
        }

        if initial_state == SubscriptionInitialState::Events {
            let empty = Gossip::new();
            for event in ClusterEvents::diff(&empty, &self.gossip, &self.self_node) {
                let _ = subscriber.tell(event);
            }
        }
    }

    fn unsubscribe(&mut self, subscriber: &ActorRef<ClusterEvent>) {
        self.subscribers
            .retain(|existing| existing.path() != subscriber.path());
    }

    fn subscribe_cluster(
        &mut self,
        subscriber: ActorRef<ClusterSubscriptionEvent>,
        initial_state: ClusterSubscriptionInitialState,
    ) {
        if !self
            .cluster_subscribers
            .iter()
            .any(|existing| existing.path() == subscriber.path())
        {
            self.cluster_subscribers.push(subscriber.clone());
        }

        match initial_state {
            ClusterSubscriptionInitialState::None => {}
            ClusterSubscriptionInitialState::Snapshot => {
                let _ = subscriber.tell(ClusterSubscriptionEvent::CurrentState(
                    CurrentClusterState::from_gossip(&self.gossip, &self.self_node),
                ));
            }
            ClusterSubscriptionInitialState::Events => {
                let empty = Gossip::new();
                for event in ClusterEvents::diff(&empty, &self.gossip, &self.self_node) {
                    let _ = subscriber.tell(ClusterSubscriptionEvent::Event(event));
                }
            }
        }
    }

    fn unsubscribe_cluster(&mut self, subscriber: &ActorRef<ClusterSubscriptionEvent>) {
        self.cluster_subscribers
            .retain(|existing| existing.path() != subscriber.path());
    }

    fn publish_changes(&mut self, new_gossip: Gossip) {
        let events = ClusterEvents::diff(&self.gossip, &new_gossip, &self.self_node);
        self.gossip = new_gossip;
        for event in events {
            self.publish(event);
        }
    }

    fn publish(&mut self, event: ClusterEvent) {
        self.subscribers
            .retain(|subscriber| subscriber.tell(event.clone()).is_ok());
        self.cluster_subscribers.retain(|subscriber| {
            subscriber
                .tell(ClusterSubscriptionEvent::Event(event.clone()))
                .is_ok()
        });
    }
}

impl Actor for ClusterEventPublisher {
    type Msg = ClusterEventPublisherMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterEventPublisherMsg::PublishChanges(gossip) => self.publish_changes(gossip),
            ClusterEventPublisherMsg::PublishEvent(event) => self.publish(event),
            ClusterEventPublisherMsg::Subscribe {
                subscriber,
                initial_state,
            } => self.subscribe(subscriber, initial_state),
            ClusterEventPublisherMsg::Unsubscribe { subscriber } => self.unsubscribe(&subscriber),
            ClusterEventPublisherMsg::SubscribeCluster {
                subscriber,
                initial_state,
            } => self.subscribe_cluster(subscriber, initial_state),
            ClusterEventPublisherMsg::UnsubscribeCluster { subscriber } => {
                self.unsubscribe_cluster(&subscriber);
            }
            ClusterEventPublisherMsg::SendCurrentState { reply_to } => {
                let _ = reply_to.tell(CurrentClusterState::from_gossip(
                    &self.gossip,
                    &self.self_node,
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kairo_actor::{Address, Props};
    use kairo_testkit::ActorSystemTestKit;

    use super::*;
    use crate::{Member, MemberEvent, MemberStatus, Reachability};

    #[test]
    fn publisher_delivers_explicit_events_to_subscribers() {
        let kit = ActorSystemTestKit::new("cluster-event-publisher-explicit").unwrap();
        let self_node = node("self", 1);
        let publisher = kit
            .system()
            .spawn(
                "publisher",
                Props::new(move || ClusterEventPublisher::new(self_node.clone())),
            )
            .unwrap();
        let subscriber = kit.create_probe::<ClusterEvent>("events").unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::Subscribe {
                subscriber: subscriber.actor_ref(),
                initial_state: SubscriptionInitialState::None,
            })
            .unwrap();
        publisher
            .tell(ClusterEventPublisherMsg::PublishEvent(
                ClusterEvent::LeaderChanged { leader: None },
            ))
            .unwrap();

        assert_eq!(
            subscriber.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterEvent::LeaderChanged { leader: None }
        );
    }

    #[test]
    fn publisher_publishes_gossip_diffs_and_stores_current_state() {
        let kit = ActorSystemTestKit::new("cluster-event-publisher-diff").unwrap();
        let self_node = node("self", 1);
        let node_b = node("b", 2);
        let publisher = spawn_publisher(&kit, self_node.clone(), "publisher");
        let subscriber = kit.create_probe::<ClusterEvent>("events").unwrap();
        let state_probe = kit.create_probe::<CurrentClusterState>("state").unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::Subscribe {
                subscriber: subscriber.actor_ref(),
                initial_state: SubscriptionInitialState::None,
            })
            .unwrap();
        let gossip = Gossip::from_members([
            member(self_node.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
        ])
        .seen(self_node.clone())
        .with_reachability(Reachability::new().unreachable(self_node.clone(), node_b.clone()));
        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(gossip))
            .unwrap();

        assert!(matches!(
            subscriber.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterEvent::Member(MemberEvent::Up(_))
        ));
        publisher
            .tell(ClusterEventPublisherMsg::SendCurrentState {
                reply_to: state_probe.actor_ref(),
            })
            .unwrap();
        let state = state_probe.expect_msg(Duration::from_secs(1)).unwrap();
        assert_eq!(state.members.len(), 2);
        assert_eq!(state.unreachable[0].unique_address, node_b);
        assert!(state.seen_by.contains(&self_node));
    }

    #[test]
    fn subscribe_with_initial_events_replays_current_state_as_events() {
        let kit = ActorSystemTestKit::new("cluster-event-publisher-initial-events").unwrap();
        let self_node = node("self", 1);
        let publisher = spawn_publisher(&kit, self_node.clone(), "publisher");
        let subscriber = kit.create_probe::<ClusterEvent>("events").unwrap();
        let gossip = Gossip::from_members([member(self_node.clone(), MemberStatus::Up)]);

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(gossip))
            .unwrap();
        publisher
            .tell(ClusterEventPublisherMsg::Subscribe {
                subscriber: subscriber.actor_ref(),
                initial_state: SubscriptionInitialState::Events,
            })
            .unwrap();

        assert!(matches!(
            subscriber.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterEvent::Member(MemberEvent::Up(_))
        ));
    }

    #[test]
    fn unsubscribe_stops_later_delivery() {
        let kit = ActorSystemTestKit::new("cluster-event-publisher-unsubscribe").unwrap();
        let self_node = node("self", 1);
        let publisher = spawn_publisher(&kit, self_node, "publisher");
        let subscriber = kit.create_probe::<ClusterEvent>("events").unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::Subscribe {
                subscriber: subscriber.actor_ref(),
                initial_state: SubscriptionInitialState::None,
            })
            .unwrap();
        publisher
            .tell(ClusterEventPublisherMsg::Unsubscribe {
                subscriber: subscriber.actor_ref(),
            })
            .unwrap();
        publisher
            .tell(ClusterEventPublisherMsg::PublishEvent(
                ClusterEvent::LeaderChanged { leader: None },
            ))
            .unwrap();

        subscriber.expect_no_msg(Duration::from_millis(50)).unwrap();
    }

    #[test]
    fn cluster_subscription_snapshot_sends_current_state_and_later_events() {
        let kit = ActorSystemTestKit::new("cluster-event-publisher-cluster-snapshot").unwrap();
        let self_node = node("self", 1);
        let node_b = node("b", 2);
        let publisher = spawn_publisher(&kit, self_node.clone(), "publisher");
        let subscriber = kit
            .create_probe::<ClusterSubscriptionEvent>("cluster-events")
            .unwrap();
        let gossip = Gossip::from_members([member(self_node.clone(), MemberStatus::Up)]);

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(gossip))
            .unwrap();
        publisher
            .tell(ClusterEventPublisherMsg::SubscribeCluster {
                subscriber: subscriber.actor_ref(),
                initial_state: ClusterSubscriptionInitialState::Snapshot,
            })
            .unwrap();

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
    fn cluster_subscription_as_events_replays_current_state_as_events() {
        let kit = ActorSystemTestKit::new("cluster-event-publisher-cluster-events").unwrap();
        let self_node = node("self", 1);
        let publisher = spawn_publisher(&kit, self_node.clone(), "publisher");
        let subscriber = kit
            .create_probe::<ClusterSubscriptionEvent>("cluster-events")
            .unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([member(self_node, MemberStatus::Up)]),
            ))
            .unwrap();
        publisher
            .tell(ClusterEventPublisherMsg::SubscribeCluster {
                subscriber: subscriber.actor_ref(),
                initial_state: ClusterSubscriptionInitialState::Events,
            })
            .unwrap();

        assert!(matches!(
            subscriber.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterSubscriptionEvent::Event(ClusterEvent::Member(MemberEvent::Up(_)))
        ));
    }

    #[test]
    fn unsubscribe_cluster_stops_later_delivery() {
        let kit = ActorSystemTestKit::new("cluster-event-publisher-unsubscribe-cluster").unwrap();
        let self_node = node("self", 1);
        let publisher = spawn_publisher(&kit, self_node, "publisher");
        let subscriber = kit
            .create_probe::<ClusterSubscriptionEvent>("cluster-events")
            .unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::SubscribeCluster {
                subscriber: subscriber.actor_ref(),
                initial_state: ClusterSubscriptionInitialState::None,
            })
            .unwrap();
        publisher
            .tell(ClusterEventPublisherMsg::UnsubscribeCluster {
                subscriber: subscriber.actor_ref(),
            })
            .unwrap();
        publisher
            .tell(ClusterEventPublisherMsg::PublishEvent(
                ClusterEvent::LeaderChanged { leader: None },
            ))
            .unwrap();

        subscriber.expect_no_msg(Duration::from_millis(50)).unwrap();
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
