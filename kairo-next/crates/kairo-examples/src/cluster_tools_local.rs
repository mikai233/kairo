use std::collections::BTreeSet;
use std::error::Error;
use std::time::Duration;

use kairo::actor::{Actor, ActorRef, ActorResult, ActorSystem, Address, Context, Props};
use kairo::cluster::{Member, MemberStatus, UniqueAddress};
use kairo::cluster_tools::{
    CurrentTopics, LocalPubSubActor, LocalPubSubMsg, LocalSingletonManagerActor,
    LocalSingletonManagerMsg, LocalSingletonManagerSnapshot, PubSubSubscribeAck, PubSubTopicReport,
    SingletonManagerEffect, SingletonManagerState, SingletonOldestTracker, SingletonScope,
    TopicName, TopicPublishMode,
};

use crate::reply::spawn_one_shot_reply;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterToolsLocalObservation {
    pub topic: String,
    pub subscribed: bool,
    pub delivered_message: String,
    pub delivered_count: usize,
    pub current_topics: Vec<String>,
    pub singleton_started: bool,
    pub singleton_reply: String,
    pub singleton_running: bool,
    pub singleton_path: Option<String>,
}

#[derive(Debug, Clone)]
enum ExampleSingletonMsg {
    Stop,
    Ping { reply_to: ActorRef<String> },
}

struct ExampleSingleton {
    started: ActorRef<String>,
    stopped: ActorRef<String>,
}

impl ExampleSingleton {
    fn new(started: ActorRef<String>, stopped: ActorRef<String>) -> Self {
        Self { started, stopped }
    }
}

impl Actor for ExampleSingleton {
    type Msg = ExampleSingletonMsg;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        let _ = self.started.tell("started".to_string());
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        let _ = self.stopped.tell("stopped".to_string());
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ExampleSingletonMsg::Stop => ctx.stop(ctx.myself())?,
            ExampleSingletonMsg::Ping { reply_to } => {
                let _ = reply_to.tell("pong".to_string());
            }
        }
        Ok(())
    }
}

pub struct ClusterToolsLocalExample {
    system: ActorSystem,
}

impl ClusterToolsLocalExample {
    pub fn start(system_name: &str) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            system: ActorSystem::builder(system_name).build()?,
        })
    }

    pub fn run(&self, timeout: Duration) -> Result<ClusterToolsLocalObservation, Box<dyn Error>> {
        let topic = TopicName::new("orders");
        let (subscriber, received) =
            spawn_one_shot_reply::<String>(&self.system, "pubsub-subscriber")?;
        let pubsub = self
            .system
            .spawn("local-pubsub", Props::new(LocalPubSubActor::<String>::new))?;
        let (ack_ref, ack_rx) =
            spawn_one_shot_reply::<PubSubSubscribeAck>(&self.system, "pubsub-ack")?;
        let (report_ref, report_rx) =
            spawn_one_shot_reply::<PubSubTopicReport>(&self.system, "pubsub-report")?;
        let (topics_ref, topics_rx) =
            spawn_one_shot_reply::<CurrentTopics>(&self.system, "pubsub-topics")?;

        pubsub.tell(LocalPubSubMsg::Subscribe {
            topic: topic.clone(),
            subscriber,
            reply_to: Some(ack_ref),
        })?;
        let ack = ack_rx.recv_timeout(timeout)?;

        pubsub.tell(LocalPubSubMsg::GetTopics {
            reply_to: topics_ref,
        })?;
        let current_topics = topics_rx.recv_timeout(timeout)?.topics;

        pubsub.tell(LocalPubSubMsg::Publish {
            topic: topic.clone(),
            message: "created".to_string(),
            mode: TopicPublishMode::Broadcast,
            reply_to: Some(report_ref),
        })?;
        let delivered_message = received.recv_timeout(timeout)?;
        let report = report_rx.recv_timeout(timeout)?;

        let singleton = self.run_singleton(timeout)?;

        Ok(ClusterToolsLocalObservation {
            topic: topic.as_str().to_string(),
            subscribed: ack.changed,
            delivered_message,
            delivered_count: report.report.delivered,
            current_topics: topic_names(current_topics),
            singleton_started: singleton.started,
            singleton_reply: singleton.reply,
            singleton_running: singleton.running,
            singleton_path: singleton.path,
        })
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), Box<dyn Error>> {
        self.system.terminate(timeout)?;
        Ok(())
    }

    fn run_singleton(&self, timeout: Duration) -> Result<SingletonObservation, Box<dyn Error>> {
        let self_node = node(self.system.name(), 1);
        let (_tracker, observation) = SingletonOldestTracker::from_members(
            self_node.clone(),
            SingletonScope::all(),
            [member(self_node.clone(), MemberStatus::Up, 1)],
        );
        let (started_ref, started_rx) =
            spawn_one_shot_reply::<String>(&self.system, "singleton-started")?;
        let (stopped_ref, _stopped_rx) =
            spawn_one_shot_reply::<String>(&self.system, "singleton-stopped")?;
        let manager = self.system.spawn(
            "local-singleton-manager",
            LocalSingletonManagerActor::<ExampleSingleton>::props(
                self_node,
                "singleton",
                {
                    let started_ref = started_ref.clone();
                    let stopped_ref = stopped_ref.clone();
                    move || {
                        let started_ref = started_ref.clone();
                        let stopped_ref = stopped_ref.clone();
                        Props::new(move || ExampleSingleton::new(started_ref, stopped_ref))
                    }
                },
                ExampleSingletonMsg::Stop,
            ),
        )?;
        let (effects_ref, effects_rx) =
            spawn_one_shot_reply::<Vec<SingletonManagerEffect>>(&self.system, "singleton-effects")?;

        manager.tell(LocalSingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: Some(effects_ref),
        })?;
        let effects = effects_rx.recv_timeout(timeout)?;
        let started = started_rx.recv_timeout(timeout)? == "started";

        let (singleton_ref, singleton_rx) = spawn_one_shot_reply::<
            Option<ActorRef<ExampleSingletonMsg>>,
        >(&self.system, "singleton-ref")?;
        manager.tell(LocalSingletonManagerMsg::GetSingleton {
            reply_to: singleton_ref,
        })?;
        let singleton = singleton_rx
            .recv_timeout(timeout)?
            .ok_or("singleton child was not started")?;
        let (ping_ref, ping_rx) = spawn_one_shot_reply::<String>(&self.system, "singleton-ping")?;
        singleton.tell(ExampleSingletonMsg::Ping { reply_to: ping_ref })?;
        let reply = ping_rx.recv_timeout(timeout)?;

        let (state_ref, state_rx) =
            spawn_one_shot_reply::<LocalSingletonManagerSnapshot>(&self.system, "singleton-state")?;
        manager.tell(LocalSingletonManagerMsg::GetState {
            reply_to: state_ref,
        })?;
        let state = state_rx.recv_timeout(timeout)?;

        Ok(SingletonObservation {
            started: started && effects == vec![SingletonManagerEffect::StartSingleton],
            reply,
            running: matches!(
                state.state,
                SingletonManagerState::Oldest {
                    singleton_running: true,
                }
            ),
            path: state.singleton_path.map(|path| path.to_string()),
        })
    }
}

pub fn run_cluster_tools_local(
    system_name: &str,
) -> Result<ClusterToolsLocalObservation, Box<dyn Error>> {
    let example = ClusterToolsLocalExample::start(system_name)?;
    let observation = example.run(Duration::from_secs(1))?;
    example.shutdown(Duration::from_secs(1))?;
    Ok(observation)
}

struct SingletonObservation {
    started: bool,
    reply: String,
    running: bool,
    path: Option<String>,
}

fn node(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(Address::local(system), uid)
}

fn member(node: UniqueAddress, status: MemberStatus, up_number: u64) -> Member {
    Member::new(node, vec![])
        .with_status(status)
        .with_up_number(up_number)
}

fn topic_names(topics: BTreeSet<TopicName>) -> Vec<String> {
    topics
        .into_iter()
        .map(|topic| topic.as_str().to_string())
        .collect()
}
