use std::sync::{Arc, Mutex};
use std::time::Duration;

use kairo_actor::{ActorRef, Address, Props};
use kairo_testkit::ActorSystemTestKit;

use super::*;
use crate::MemberStatus;

#[test]
fn receiver_replies_to_heartbeat_sender() {
    let kit = ActorSystemTestKit::new("cluster-heartbeat-receiver").unwrap();
    let self_node = node("receiver", 2);
    let receiver = kit
        .system()
        .spawn(
            "receiver",
            Props::new(move || HeartbeatReceiver::new(self_node.clone())),
        )
        .unwrap();
    let reply_probe = kit.create_probe::<HeartbeatSenderMsg>("reply").unwrap();

    receiver
        .tell(HeartbeatReceiverMsg::Heartbeat {
            heartbeat: Heartbeat {
                from: node("sender", 1),
                sequence_nr: 7,
                creation_time_nanos: 42,
            },
            reply_to: reply_probe.actor_ref(),
        })
        .unwrap();

    let reply = reply_probe.expect_msg(Duration::from_secs(1)).unwrap();
    let HeartbeatSenderMsg::HeartbeatResponse(response) = reply else {
        panic!("expected heartbeat response");
    };
    assert_eq!(response.from, node("receiver", 2));
    assert_eq!(response.sequence_nr, 7);
    assert_eq!(response.creation_time_nanos, 42);
}

#[test]
fn sender_sends_heartbeat_to_active_registered_receiver() {
    let kit = ActorSystemTestKit::new("cluster-heartbeat-sender-send").unwrap();
    let sender_node = node("sender", 1);
    let receiver_node = node("receiver", 2);
    let clock = TestHeartbeatClock::new(Duration::from_millis(123));
    let sender = spawn_sender(&kit, sender_node.clone(), clock.clone(), "sender");
    let receiver_probe = kit
        .create_probe::<HeartbeatReceiverMsg>("receiver")
        .unwrap();

    sender
        .tell(HeartbeatSenderMsg::RegisterReceiver {
            node: receiver_node.clone(),
            receiver: receiver_probe.actor_ref(),
        })
        .unwrap();
    sender
        .tell(HeartbeatSenderMsg::Init(cluster_state(
            sender_node.clone(),
            [receiver_node.clone()],
            [],
        )))
        .unwrap();
    sender.tell(HeartbeatSenderMsg::HeartbeatTick).unwrap();

    let HeartbeatReceiverMsg::Heartbeat {
        heartbeat,
        reply_to,
    } = receiver_probe.expect_msg(Duration::from_secs(1)).unwrap();
    assert_eq!(heartbeat.from, sender_node);
    assert_eq!(heartbeat.sequence_nr, 1);
    assert_eq!(heartbeat.creation_time_nanos, 123_000_000);

    clock.set(Duration::from_millis(150));
    reply_to
        .tell(HeartbeatSenderMsg::HeartbeatResponse(HeartbeatRsp {
            from: receiver_node.clone(),
            sequence_nr: heartbeat.sequence_nr,
            creation_time_nanos: heartbeat.creation_time_nanos,
        }))
        .unwrap();

    let snapshot = request_snapshot(&kit, &sender);
    assert!(snapshot.monitored_receivers.contains(&receiver_node));
}

#[test]
fn sender_expected_first_heartbeat_starts_monitoring_without_response() {
    let kit = ActorSystemTestKit::new("cluster-heartbeat-sender-first").unwrap();
    let sender_node = node("sender", 1);
    let receiver_node = node("receiver", 2);
    let sender = spawn_sender(
        &kit,
        sender_node.clone(),
        TestHeartbeatClock::new(Duration::from_millis(10)),
        "sender",
    );

    sender
        .tell(HeartbeatSenderMsg::Init(cluster_state(
            sender_node,
            [receiver_node.clone()],
            [],
        )))
        .unwrap();
    sender
        .tell(HeartbeatSenderMsg::ExpectedFirstHeartbeat(
            receiver_node.clone(),
        ))
        .unwrap();

    let snapshot = request_snapshot(&kit, &sender);
    assert!(snapshot.monitored_receivers.contains(&receiver_node));
}

#[test]
fn sender_applies_cluster_membership_and_reachability_events() {
    let kit = ActorSystemTestKit::new("cluster-heartbeat-sender-events").unwrap();
    let sender_node = node("sender", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let sender = spawn_sender(
        &kit,
        sender_node.clone(),
        TestHeartbeatClock::new(Duration::from_millis(1)),
        "sender",
    );

    sender
        .tell(HeartbeatSenderMsg::Init(cluster_state(sender_node, [], [])))
        .unwrap();
    sender
        .tell(HeartbeatSenderMsg::ClusterEvent(ClusterEvent::Member(
            MemberEvent::Up(member(node_b.clone())),
        )))
        .unwrap();
    sender
        .tell(HeartbeatSenderMsg::ClusterEvent(
            ClusterEvent::Reachability(ReachabilityEvent::Unreachable(member(node_b.clone()))),
        ))
        .unwrap();
    sender
        .tell(HeartbeatSenderMsg::ClusterEvent(ClusterEvent::Member(
            MemberEvent::Up(member(node_c.clone())),
        )))
        .unwrap();

    let snapshot = request_snapshot(&kit, &sender);
    assert!(snapshot.active_receivers.contains(&node_b));
    assert!(snapshot.active_receivers.contains(&node_c));
}

fn spawn_sender(
    kit: &ActorSystemTestKit,
    self_node: UniqueAddress,
    clock: TestHeartbeatClock,
    name: &str,
) -> ActorRef<HeartbeatSenderMsg> {
    let settings = settings().with_automatic_ticks(false);
    kit.system()
        .spawn(
            name,
            Props::new(move || {
                HeartbeatSender::with_clock(self_node.clone(), settings, Arc::new(clock)).unwrap()
            }),
        )
        .unwrap()
}

fn request_snapshot(
    kit: &ActorSystemTestKit,
    sender: &ActorRef<HeartbeatSenderMsg>,
) -> HeartbeatSenderSnapshot {
    let probe = kit
        .create_probe::<HeartbeatSenderSnapshot>("snapshot")
        .unwrap();
    sender
        .tell(HeartbeatSenderMsg::SendSnapshot {
            reply_to: probe.actor_ref(),
        })
        .unwrap();
    probe.expect_msg(Duration::from_secs(1)).unwrap()
}

fn cluster_state(
    self_node: UniqueAddress,
    others: impl IntoIterator<Item = UniqueAddress>,
    unreachable: impl IntoIterator<Item = UniqueAddress>,
) -> CurrentClusterState {
    CurrentClusterState {
        members: std::iter::once(self_node)
            .chain(others)
            .map(member)
            .collect(),
        unreachable: unreachable.into_iter().map(member).collect(),
        seen_by: HashSet::new(),
        leader: None,
        role_leaders: HashMap::new(),
        member_tombstones: HashSet::new(),
    }
}

fn settings() -> HeartbeatSenderSettings {
    HeartbeatSenderSettings::new(
        3,
        DeadlineFailureDetectorSettings::new(
            Duration::from_millis(1_000),
            Duration::from_millis(3_000),
        )
        .unwrap(),
    )
}

fn member(unique_address: UniqueAddress) -> Member {
    Member::new(unique_address, Vec::new())
        .with_status(MemberStatus::Up)
        .with_up_number(1)
}

fn node(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(Address::local(system), uid)
}

#[derive(Clone)]
struct TestHeartbeatClock {
    now: Arc<Mutex<Duration>>,
}

impl TestHeartbeatClock {
    fn new(now: Duration) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    fn set(&self, now: Duration) {
        *self.now.lock().expect("test heartbeat clock poisoned") = now;
    }
}

impl HeartbeatClock for TestHeartbeatClock {
    fn now(&self) -> Duration {
        *self.now.lock().expect("test heartbeat clock poisoned")
    }
}
