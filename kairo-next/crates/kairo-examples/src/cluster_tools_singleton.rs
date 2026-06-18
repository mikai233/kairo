use std::error::Error;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use kairo::actor::{Actor, ActorRef, ActorResult, ActorSystem, Address, Context, Props};
use kairo::cluster::{Member, MemberStatus, UniqueAddress};
use kairo::cluster_tools::{
    LocalSingletonManagerActor, LocalSingletonManagerMsg, LocalSingletonManagerSnapshot,
    SingletonManagerEffect, SingletonManagerState, SingletonOldestChange,
    SingletonOldestObservation, SingletonOldestTracker, SingletonScope,
};

use crate::reply::spawn_one_shot_reply;

const MANAGER_STATE_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterToolsSingletonObservation {
    pub first_node: String,
    pub second_node: String,
    pub first_started: bool,
    pub first_stopped: bool,
    pub second_started: bool,
    pub handover_requested: bool,
    pub handover_in_progress: bool,
    pub second_started_after_first_stopped: bool,
    pub first_state: SingletonManagerState,
    pub second_state: SingletonManagerState,
    pub first_singleton_path: Option<String>,
    pub second_singleton_path: Option<String>,
}

#[derive(Debug, Clone)]
enum ExampleSingletonMsg {
    Stop,
}

struct ExampleSingleton {
    started: ActorRef<String>,
    stopped: ActorRef<String>,
    label: String,
}

impl ExampleSingleton {
    fn new(started: ActorRef<String>, stopped: ActorRef<String>, label: impl Into<String>) -> Self {
        Self {
            started,
            stopped,
            label: label.into(),
        }
    }
}

impl Actor for ExampleSingleton {
    type Msg = ExampleSingletonMsg;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        let _ = self.started.tell(format!("{} started", self.label));
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        let _ = self.stopped.tell(format!("{} stopped", self.label));
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ExampleSingletonMsg::Stop => ctx.stop(ctx.myself())?,
        }
        Ok(())
    }
}

pub fn run_cluster_tools_singleton(
    system_name: &str,
) -> Result<ClusterToolsSingletonObservation, Box<dyn Error>> {
    let example = ClusterToolsSingletonExample::start(system_name)?;
    let observation = example.run(Duration::from_secs(1))?;
    example.shutdown(Duration::from_secs(1))?;
    Ok(observation)
}

pub struct ClusterToolsSingletonExample {
    system: ActorSystem,
}

impl ClusterToolsSingletonExample {
    pub fn start(system_name: &str) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            system: ActorSystem::builder(system_name).build()?,
        })
    }

    pub fn run(
        &self,
        timeout: Duration,
    ) -> Result<ClusterToolsSingletonObservation, Box<dyn Error>> {
        let node_a = node(self.system.name(), 1);
        let node_b = node(self.system.name(), 2);
        let manager_a = self.spawn_manager("a", &node_a)?;
        let manager_b = self.spawn_manager("b", &node_b)?;

        let initial_a = initial_observation(node_a.clone(), node_a.clone(), node_b.clone());
        let initial_b = initial_observation(node_b.clone(), node_a.clone(), node_b.clone());
        let effects_a = request_effects(
            &self.system,
            "singleton-a-initial",
            &manager_a.manager,
            |reply_to| LocalSingletonManagerMsg::ApplyInitialObservation {
                observation: initial_a,
                reply_to: Some(reply_to),
            },
            timeout,
        )?;
        let first_started = manager_a.started.recv_timeout(timeout)?;
        let first_started_at = Instant::now();
        let effects_b = request_effects(
            &self.system,
            "singleton-b-initial",
            &manager_b.manager,
            |reply_to| LocalSingletonManagerMsg::ApplyInitialObservation {
                observation: initial_b,
                reply_to: Some(reply_to),
            },
            timeout,
        )?;

        let takeover = request_effects(
            &self.system,
            "singleton-b-oldest",
            &manager_b.manager,
            |reply_to| LocalSingletonManagerMsg::ApplyOldestChange {
                change: SingletonOldestChange::OldestChanged(Some(node_b.clone())),
                reply_to: Some(reply_to),
            },
            timeout,
        )?;
        let handover = request_effects(
            &self.system,
            "singleton-a-handover",
            &manager_a.manager,
            |reply_to| LocalSingletonManagerMsg::HandOverToMe {
                from: node_b.clone(),
                reply_to: Some(reply_to),
            },
            timeout,
        )?;
        let first_stopped = manager_a.stopped.recv_timeout(timeout)?;
        let first_stopped_at = Instant::now();
        let first_snapshot = wait_for_manager_state(
            &self.system,
            "singleton-a-end",
            &manager_a.manager,
            timeout,
            |state| matches!(state, SingletonManagerState::End),
        )?;

        request_effects(
            &self.system,
            "singleton-b-progress",
            &manager_b.manager,
            |reply_to| LocalSingletonManagerMsg::HandOverInProgress {
                from: node_a.clone(),
                reply_to: Some(reply_to),
            },
            timeout,
        )?;
        let start_second = request_effects(
            &self.system,
            "singleton-b-done",
            &manager_b.manager,
            |reply_to| LocalSingletonManagerMsg::HandOverDone {
                from: node_a.clone(),
                reply_to: Some(reply_to),
            },
            timeout,
        )?;
        let second_started = manager_b.started.recv_timeout(timeout)?;
        let second_started_at = Instant::now();
        let second_snapshot = snapshot(
            &self.system,
            "singleton-b-final",
            &manager_b.manager,
            timeout,
        )?;

        Ok(ClusterToolsSingletonObservation {
            first_node: node_a.ordering_key(),
            second_node: node_b.ordering_key(),
            first_started: effects_a == vec![SingletonManagerEffect::StartSingleton]
                && first_started == "a started"
                && first_started_at <= first_stopped_at,
            first_stopped: first_stopped == "a stopped" && first_snapshot.singleton_path.is_none(),
            second_started: effects_b.is_empty()
                && start_second == vec![SingletonManagerEffect::StartSingleton]
                && second_started == "b started",
            handover_requested: takeover
                == vec![SingletonManagerEffect::SendHandOverToMe { to: node_a }],
            handover_in_progress: handover
                == vec![
                    SingletonManagerEffect::SendHandOverInProgress { to: node_b },
                    SingletonManagerEffect::StopSingleton,
                ],
            second_started_after_first_stopped: second_started_at > first_stopped_at,
            first_state: first_snapshot.state,
            second_state: second_snapshot.state,
            first_singleton_path: first_snapshot.singleton_path.map(|path| path.to_string()),
            second_singleton_path: second_snapshot.singleton_path.map(|path| path.to_string()),
        })
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), Box<dyn Error>> {
        self.system.terminate(timeout)?;
        Ok(())
    }

    fn spawn_manager(
        &self,
        label: &'static str,
        node: &UniqueAddress,
    ) -> Result<SingletonManagerHandle, Box<dyn Error>> {
        let (started_ref, started) =
            spawn_one_shot_reply::<String>(&self.system, format!("singleton-{label}-started"))?;
        let (stopped_ref, stopped) =
            spawn_one_shot_reply::<String>(&self.system, format!("singleton-{label}-stopped"))?;
        let manager = self.system.spawn(
            format!("singleton-manager-{label}"),
            LocalSingletonManagerActor::<ExampleSingleton>::props(
                node.clone(),
                "singleton",
                move || {
                    let started_ref = started_ref.clone();
                    let stopped_ref = stopped_ref.clone();
                    Props::new(move || {
                        ExampleSingleton::new(started_ref.clone(), stopped_ref.clone(), label)
                    })
                },
                ExampleSingletonMsg::Stop,
            ),
        )?;
        Ok(SingletonManagerHandle {
            manager,
            started,
            stopped,
        })
    }
}

struct SingletonManagerHandle {
    manager: ActorRef<LocalSingletonManagerMsg<ExampleSingletonMsg>>,
    started: mpsc::Receiver<String>,
    stopped: mpsc::Receiver<String>,
}

fn request_effects<F>(
    system: &ActorSystem,
    name: &str,
    manager: &ActorRef<LocalSingletonManagerMsg<ExampleSingletonMsg>>,
    message: F,
    timeout: Duration,
) -> Result<Vec<SingletonManagerEffect>, Box<dyn Error>>
where
    F: FnOnce(
        ActorRef<Vec<SingletonManagerEffect>>,
    ) -> LocalSingletonManagerMsg<ExampleSingletonMsg>,
{
    let (reply_to, replies) = spawn_one_shot_reply::<Vec<SingletonManagerEffect>>(system, name)?;
    manager.tell(message(reply_to))?;
    Ok(replies.recv_timeout(timeout)?)
}

fn snapshot(
    system: &ActorSystem,
    name: &str,
    manager: &ActorRef<LocalSingletonManagerMsg<ExampleSingletonMsg>>,
    timeout: Duration,
) -> Result<LocalSingletonManagerSnapshot, Box<dyn Error>> {
    let (reply_to, replies) = spawn_one_shot_reply::<LocalSingletonManagerSnapshot>(system, name)?;
    manager.tell(LocalSingletonManagerMsg::GetState { reply_to })?;
    Ok(replies.recv_timeout(timeout)?)
}

fn wait_for_manager_state<P>(
    system: &ActorSystem,
    name: &str,
    manager: &ActorRef<LocalSingletonManagerMsg<ExampleSingletonMsg>>,
    timeout: Duration,
    predicate: P,
) -> Result<LocalSingletonManagerSnapshot, Box<dyn Error>>
where
    P: Fn(&SingletonManagerState) -> bool,
{
    let deadline = Instant::now() + timeout;
    let mut attempt = 0;
    loop {
        let Some(remaining) = remaining_until(deadline) else {
            return Err("timed out waiting for singleton manager state".into());
        };
        let state = snapshot(system, &format!("{name}-{attempt}"), manager, remaining)?;
        if predicate(&state.state) {
            return Ok(state);
        }
        attempt += 1;
        if !sleep_until_next_poll(deadline) {
            return Err("timed out waiting for singleton manager state".into());
        }
    }
}

fn remaining_until(deadline: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    (!remaining.is_zero()).then_some(remaining)
}

fn sleep_until_next_poll(deadline: Instant) -> bool {
    let Some(remaining) = remaining_until(deadline) else {
        return false;
    };
    std::thread::sleep(MANAGER_STATE_POLL_INTERVAL.min(remaining));
    true
}

fn initial_observation(
    self_node: UniqueAddress,
    node_a: UniqueAddress,
    node_b: UniqueAddress,
) -> SingletonOldestObservation {
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        self_node,
        SingletonScope::all(),
        [
            member(node_a, MemberStatus::Up, 1),
            member(node_b, MemberStatus::Up, 2),
        ],
    );
    observation
}

fn node(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(Address::local(system), uid)
}

fn member(node: UniqueAddress, status: MemberStatus, up_number: u64) -> Member {
    Member::new(node, vec![])
        .with_status(status)
        .with_up_number(up_number)
}
