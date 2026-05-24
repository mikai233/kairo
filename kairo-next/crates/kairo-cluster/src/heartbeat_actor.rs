use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use kairo_actor::{Actor, ActorRef, ActorResult, Context};

use crate::{
    ClusterEvent, CurrentClusterState, DeadlineFailureDetectorSettings, Heartbeat, HeartbeatRsp,
    HeartbeatSenderState, Member, MemberEvent, ReachabilityEvent, UniqueAddress,
};

const HEARTBEAT_TIMER_KEY: &str = "cluster-heartbeat";

pub trait HeartbeatClock: Send + Sync + 'static {
    fn now(&self) -> Duration;
}

#[derive(Debug)]
pub struct SystemHeartbeatClock {
    started_at: Instant,
}

impl SystemHeartbeatClock {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }
}

impl Default for SystemHeartbeatClock {
    fn default() -> Self {
        Self::new()
    }
}

impl HeartbeatClock for SystemHeartbeatClock {
    fn now(&self) -> Duration {
        self.started_at.elapsed()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatSenderSettings {
    pub monitored_by_nr_of_members: usize,
    pub failure_detector: DeadlineFailureDetectorSettings,
    pub periodic_tasks_initial_delay: Duration,
    pub heartbeat_expected_response_after: Duration,
    pub automatic_ticks: bool,
}

impl HeartbeatSenderSettings {
    pub fn new(
        monitored_by_nr_of_members: usize,
        failure_detector: DeadlineFailureDetectorSettings,
    ) -> Self {
        Self {
            monitored_by_nr_of_members,
            failure_detector,
            periodic_tasks_initial_delay: Duration::ZERO,
            heartbeat_expected_response_after: failure_detector.heartbeat_interval(),
            automatic_ticks: true,
        }
    }

    pub fn with_periodic_tasks_initial_delay(mut self, delay: Duration) -> Self {
        self.periodic_tasks_initial_delay = delay;
        self
    }

    pub fn with_heartbeat_expected_response_after(mut self, delay: Duration) -> Self {
        self.heartbeat_expected_response_after = delay;
        self
    }

    pub fn with_automatic_ticks(mut self, automatic_ticks: bool) -> Self {
        self.automatic_ticks = automatic_ticks;
        self
    }

    fn heartbeat_interval(&self) -> Duration {
        self.failure_detector.heartbeat_interval()
    }

    fn first_tick_delay(&self) -> Duration {
        self.periodic_tasks_initial_delay
            .max(self.heartbeat_interval())
    }
}

#[derive(Debug, Clone)]
pub enum HeartbeatReceiverMsg {
    Heartbeat {
        heartbeat: Heartbeat,
        reply_to: ActorRef<HeartbeatSenderMsg>,
    },
}

#[derive(Debug, Clone)]
pub enum HeartbeatSenderMsg {
    Init(CurrentClusterState),
    RegisterReceiver {
        node: UniqueAddress,
        receiver: ActorRef<HeartbeatReceiverMsg>,
    },
    UnregisterReceiver {
        node: UniqueAddress,
    },
    ClusterEvent(ClusterEvent),
    HeartbeatTick,
    HeartbeatResponse(HeartbeatRsp),
    ExpectedFirstHeartbeat(UniqueAddress),
    SendSnapshot {
        reply_to: ActorRef<HeartbeatSenderSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatSenderSnapshot {
    pub initialized: bool,
    pub sequence_nr: u64,
    pub active_receivers: HashSet<UniqueAddress>,
    pub monitored_receivers: HashSet<UniqueAddress>,
}

pub struct HeartbeatReceiver {
    self_node: UniqueAddress,
}

impl HeartbeatReceiver {
    pub fn new(self_node: UniqueAddress) -> Self {
        Self { self_node }
    }
}

impl Actor for HeartbeatReceiver {
    type Msg = HeartbeatReceiverMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            HeartbeatReceiverMsg::Heartbeat {
                heartbeat,
                reply_to,
            } => {
                let _ = reply_to.tell(HeartbeatSenderMsg::HeartbeatResponse(HeartbeatRsp {
                    from: self.self_node.clone(),
                    sequence_nr: heartbeat.sequence_nr,
                    creation_time_nanos: heartbeat.creation_time_nanos,
                }));
            }
        }
        Ok(())
    }
}

pub struct HeartbeatSender {
    self_node: UniqueAddress,
    settings: HeartbeatSenderSettings,
    state: HeartbeatSenderState,
    routes: HashMap<UniqueAddress, ActorRef<HeartbeatReceiverMsg>>,
    sequence_nr: u64,
    initialized: bool,
    clock: Arc<dyn HeartbeatClock>,
}

impl HeartbeatSender {
    pub fn new(
        self_node: UniqueAddress,
        settings: HeartbeatSenderSettings,
    ) -> Result<Self, crate::HeartbeatError> {
        Self::with_clock(self_node, settings, Arc::new(SystemHeartbeatClock::new()))
    }

    pub fn with_clock(
        self_node: UniqueAddress,
        settings: HeartbeatSenderSettings,
        clock: Arc<dyn HeartbeatClock>,
    ) -> Result<Self, crate::HeartbeatError> {
        let state = HeartbeatSenderState::new(
            self_node.clone(),
            settings.monitored_by_nr_of_members,
            settings.failure_detector,
        )?;
        Ok(Self {
            self_node,
            settings,
            state,
            routes: HashMap::new(),
            sequence_nr: 0,
            initialized: false,
            clock,
        })
    }

    fn init(&mut self, snapshot: CurrentClusterState) {
        let nodes = snapshot
            .members
            .into_iter()
            .map(|member| member.unique_address);
        let unreachable = snapshot
            .unreachable
            .into_iter()
            .map(|member| member.unique_address);
        self.state = self.state.init(nodes, unreachable);
        self.initialized = true;
    }

    fn register_receiver(&mut self, node: UniqueAddress, receiver: ActorRef<HeartbeatReceiverMsg>) {
        self.routes.insert(node, receiver);
    }

    fn unregister_receiver(&mut self, node: &UniqueAddress) {
        self.routes.remove(node);
    }

    fn handle_cluster_event(&mut self, event: ClusterEvent, now: Duration) {
        match event {
            ClusterEvent::Member(MemberEvent::Removed { member, .. }) => {
                self.remove_member(member, now);
            }
            ClusterEvent::Member(event) => {
                self.add_member(member_from_event(event), now);
            }
            ClusterEvent::Reachability(ReachabilityEvent::Unreachable(member)) => {
                self.state = self.state.unreachable_member(member.unique_address, now);
            }
            ClusterEvent::Reachability(ReachabilityEvent::Reachable(member)) => {
                self.state = self.state.reachable_member(&member.unique_address, now);
            }
            ClusterEvent::LeaderChanged { .. }
            | ClusterEvent::RoleLeaderChanged { .. }
            | ClusterEvent::SeenChanged { .. }
            | ClusterEvent::ReachabilityChanged { .. }
            | ClusterEvent::MemberTombstonesChanged { .. } => {}
        }
    }

    fn add_member(&mut self, member: Member, now: Duration) {
        if member.unique_address != self.self_node && !self.state.contains(&member.unique_address) {
            self.state = self.state.add_member(member.unique_address, now);
        }
    }

    fn remove_member(&mut self, member: Member, now: Duration) {
        if member.unique_address != self.self_node {
            self.state = self.state.remove_member(&member.unique_address, now);
            self.routes.remove(&member.unique_address);
        }
    }

    fn heartbeat(&mut self, ctx: &Context<HeartbeatSenderMsg>, now: Duration) {
        self.sequence_nr += 1;
        let heartbeat = Heartbeat {
            from: self.self_node.clone(),
            sequence_nr: self.sequence_nr,
            creation_time_nanos: nanos_u64(now),
        };

        for receiver in self.state.active_receivers() {
            if !self.state.failure_detector().is_monitoring(&receiver) {
                ctx.schedule_once_self(
                    self.settings.heartbeat_expected_response_after,
                    HeartbeatSenderMsg::ExpectedFirstHeartbeat(receiver.clone()),
                );
            }

            if let Some(route) = self.routes.get(&receiver) {
                let _ = route.tell(HeartbeatReceiverMsg::Heartbeat {
                    heartbeat: heartbeat.clone(),
                    reply_to: ctx.myself(),
                });
            }
        }
    }

    fn heartbeat_response(&mut self, response: HeartbeatRsp, now: Duration) {
        self.state = self.state.heartbeat_response(&response.from, now);
    }

    fn expected_first_heartbeat(&mut self, from: UniqueAddress, now: Duration) {
        self.state = self.state.trigger_expected_first_heartbeat(&from, now);
    }

    fn snapshot(&self) -> HeartbeatSenderSnapshot {
        let active_receivers = self.state.active_receivers();
        let monitored_receivers = active_receivers
            .iter()
            .filter(|node| self.state.failure_detector().is_monitoring(node))
            .cloned()
            .collect();
        HeartbeatSenderSnapshot {
            initialized: self.initialized,
            sequence_nr: self.sequence_nr,
            active_receivers,
            monitored_receivers,
        }
    }
}

impl Actor for HeartbeatSender {
    type Msg = HeartbeatSenderMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        if self.settings.automatic_ticks {
            ctx.start_timer_with_fixed_delay(
                HEARTBEAT_TIMER_KEY,
                self.settings.first_tick_delay(),
                self.settings.heartbeat_interval(),
                HeartbeatSenderMsg::HeartbeatTick,
            );
        }
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.state = self.state.reset_failure_detector();
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            HeartbeatSenderMsg::Init(snapshot) => self.init(snapshot),
            HeartbeatSenderMsg::RegisterReceiver { node, receiver } => {
                self.register_receiver(node, receiver)
            }
            HeartbeatSenderMsg::UnregisterReceiver { node } => self.unregister_receiver(&node),
            HeartbeatSenderMsg::ClusterEvent(event) if self.initialized => {
                self.handle_cluster_event(event, self.clock.now());
            }
            HeartbeatSenderMsg::ClusterEvent(_) => {}
            HeartbeatSenderMsg::HeartbeatTick if self.initialized => {
                self.heartbeat(ctx, self.clock.now());
            }
            HeartbeatSenderMsg::HeartbeatTick => {}
            HeartbeatSenderMsg::HeartbeatResponse(response) if self.initialized => {
                self.heartbeat_response(response, self.clock.now());
            }
            HeartbeatSenderMsg::HeartbeatResponse(_) => {}
            HeartbeatSenderMsg::ExpectedFirstHeartbeat(from) if self.initialized => {
                self.expected_first_heartbeat(from, self.clock.now());
            }
            HeartbeatSenderMsg::ExpectedFirstHeartbeat(_) => {}
            HeartbeatSenderMsg::SendSnapshot { reply_to } => {
                let _ = reply_to.tell(self.snapshot());
            }
        }
        Ok(())
    }
}

fn member_from_event(event: MemberEvent) -> Member {
    match event {
        MemberEvent::Joined(member)
        | MemberEvent::WeaklyUp(member)
        | MemberEvent::Up(member)
        | MemberEvent::Left(member)
        | MemberEvent::Exited(member)
        | MemberEvent::Downed(member) => member,
        MemberEvent::Removed { member, .. } => member,
    }
}

fn nanos_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use kairo_actor::{Address, Props};
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
                    HeartbeatSender::with_clock(self_node.clone(), settings, Arc::new(clock))
                        .unwrap()
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
}
