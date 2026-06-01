use std::time::Duration;

use kairo_actor::{ActorRef, Address, Props};
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
