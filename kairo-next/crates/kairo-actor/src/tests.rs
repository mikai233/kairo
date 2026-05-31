use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use crate::*;

mod asks;
mod coordinated_shutdown;
mod event_stream;
mod receive_timeout;
mod receptionist;
mod scheduler;
mod stash;
mod timers;

enum CounterMsg {
    Increment,
    Get(mpsc::Sender<usize>),
    Stop,
}

struct Counter {
    value: usize,
}

struct ChannelProbe<T> {
    observed: mpsc::Sender<T>,
}

impl<T: Send + 'static> Actor for ChannelProbe<T> {
    type Msg = T;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.observed
            .send(msg)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

impl Actor for Counter {
    type Msg = CounterMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            CounterMsg::Increment => self.value += 1,
            CounterMsg::Get(reply_to) => {
                reply_to
                    .send(self.value)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            CounterMsg::Stop => ctx.stop(ctx.myself())?,
        }
        Ok(())
    }
}

#[test]
fn spawned_actor_receives_messages_in_tell_order() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    counter.tell(CounterMsg::Increment).unwrap();
    counter.tell(CounterMsg::Increment).unwrap();
    counter.tell(CounterMsg::Get(reply_tx)).unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
}

#[test]
fn actor_system_builder_configures_dispatcher_throughput() {
    let system = ActorSystem::builder("test")
        .dispatcher_throughput(2)
        .build()
        .unwrap();

    assert_eq!(system.dispatcher_settings().throughput(), 2);
}

#[test]
fn actor_system_builder_rejects_zero_dispatcher_throughput() {
    let error = ActorSystem::builder("test")
        .dispatcher_throughput(0)
        .build()
        .unwrap_err();

    assert!(matches!(error, ActorError::InvalidThroughput));
}

fn send_to_recipient<R>(recipient: &R, message: CounterMsg)
where
    R: Recipient<CounterMsg>,
{
    recipient.tell(message).unwrap();
}

#[test]
fn actor_ref_and_ignore_ref_are_recipients() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();
    let ignore = IgnoreRef::new();
    let (reply_tx, reply_rx) = mpsc::channel();

    send_to_recipient(&counter, CounterMsg::Increment);
    send_to_recipient(&ignore, CounterMsg::Increment);
    counter.tell(CounterMsg::Get(reply_tx)).unwrap();

    assert_eq!(ignore.path().as_str(), "kairo://local/ignore");
    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
}

#[test]
fn duplicate_live_actor_name_is_rejected() {
    let system = ActorSystem::builder("test").build().unwrap();
    let _counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();

    let error = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap_err();

    assert!(matches!(error, ActorError::DuplicateName(name) if name == "counter"));
}

#[test]
fn user_actor_names_follow_path_element_rules() {
    let system = ActorSystem::builder("test").build().unwrap();
    let valid = system
        .spawn("worker-1_.*+:@&=,!~';%20", Props::new(|| Noop))
        .unwrap();

    assert!(valid.path().as_str().contains("/worker-1_.*+:@&=,!~';%20#"));

    for invalid in [
        "",
        "$reserved",
        "bad/name",
        "bad#name",
        "bad name",
        "naive?",
        "naiveä",
        "bad%",
        "bad%zz",
    ] {
        let error = system.spawn(invalid, Props::new(|| Noop)).unwrap_err();
        assert!(matches!(error, ActorError::InvalidName(name) if name == invalid));
    }
}

#[test]
fn stop_prevents_later_user_message_delivery() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();

    counter.tell(CounterMsg::Stop).unwrap();

    let mut rejected = None;
    for _ in 0..100 {
        match counter.tell(CounterMsg::Increment) {
            Ok(()) => thread::sleep(Duration::from_millis(5)),
            Err(error) => {
                rejected = Some(error);
                break;
            }
        }
    }

    let error = rejected.expect("message sent after stop should be rejected");
    assert_eq!(error.reason(), "actor is stopped");
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );

    let records = system.dead_letters().records();
    assert_eq!(records[0].recipient(), counter.path());
    assert_eq!(records[0].reason(), "actor is stopped");
}

#[test]
fn missing_actor_ref_sends_to_dead_letters() {
    let system = ActorSystem::builder("test").build().unwrap();
    let missing: ActorRef<CounterMsg> = system.missing_ref("kairo://test/user/missing#404");

    let error = missing.tell(CounterMsg::Increment).unwrap_err();

    assert_eq!(error.reason(), "actor does not exist");
    assert!(missing.is_stopped());
    assert!(missing.wait_for_stop(Duration::from_secs(1)));
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    let records = system.dead_letters().records();
    assert_eq!(records[0].recipient(), missing.path());
    assert_eq!(records[0].reason(), "actor does not exist");
}

#[test]
fn actor_system_resolves_live_local_ref_by_exact_typed_path() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();
    let path = counter.path().to_string();
    let resolved: ActorRef<CounterMsg> = system
        .resolve_local(&path)
        .expect("live local actor should resolve by typed path");
    let (reply_tx, reply_rx) = mpsc::channel();

    resolved.tell(CounterMsg::Increment).unwrap();
    counter.tell(CounterMsg::Get(reply_tx)).unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    assert!(system.resolve_local::<()>(&path).is_none());

    counter.tell(CounterMsg::Stop).unwrap();
    assert!(counter.wait_for_stop(Duration::from_secs(1)));
    assert!(system.resolve_local::<CounterMsg>(&path).is_none());

    let missing: ActorRef<CounterMsg> = system.resolve_local_or_missing(path);
    let error = missing.tell(CounterMsg::Increment).unwrap_err();

    assert_eq!(error.reason(), "actor does not exist");
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
}

struct StopProbe {
    stopped: mpsc::Sender<()>,
}

impl Actor for StopProbe {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.stopped
            .send(())
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

#[test]
fn actor_system_stop_wakes_idle_actor() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (stopped_tx, stopped_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "probe",
            Props::new(move || StopProbe {
                stopped: stopped_tx,
            }),
        )
        .unwrap();

    system.stop(&actor);

    stopped_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(actor.is_stopped());
}

#[test]
fn stopped_actor_name_can_be_reused_with_new_incarnation() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (first_stopped_tx, first_stopped_rx) = mpsc::channel();
    let first = system
        .spawn(
            "probe",
            Props::new(move || StopProbe {
                stopped: first_stopped_tx,
            }),
        )
        .unwrap();
    let first_path = first.path().clone();

    system.stop(&first);
    first_stopped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    let (second_stopped_tx, _second_stopped_rx) = mpsc::channel();
    let second = system
        .spawn(
            "probe",
            Props::new(move || StopProbe {
                stopped: second_stopped_tx,
            }),
        )
        .unwrap();

    assert_ne!(&first_path, second.path());
    assert!(first_path.as_str().contains("/user/probe#"));
    assert!(second.path().as_str().contains("/user/probe#"));
}

struct BlockingStart {
    release: mpsc::Receiver<()>,
    received: Arc<AtomicU64>,
}

impl Actor for BlockingStart {
    type Msg = ();

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.release
            .recv()
            .map_err(|error| ActorError::Message(error.to_string()))?;
        Ok(())
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        self.received.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[test]
fn system_stop_drains_queued_user_messages_to_dead_letters() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (release_tx, release_rx) = mpsc::channel();
    let received = Arc::new(AtomicU64::new(0));
    let actor = system
        .spawn(
            "blocked",
            Props::new({
                let received = Arc::clone(&received);
                move || BlockingStart {
                    release: release_rx,
                    received,
                }
            }),
        )
        .unwrap();

    actor.tell(()).unwrap();
    actor.tell(()).unwrap();
    system.stop(&actor);
    release_tx.send(()).unwrap();

    assert!(
        system
            .dead_letters()
            .wait_for_len(2, Duration::from_secs(1))
    );
    assert_eq!(received.load(Ordering::Relaxed), 0);
    assert_eq!(system.dead_letters().records()[0].recipient(), actor.path());
}

struct Noop;

impl Actor for Noop {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }
}

enum ParentMsg {
    SpawnNamed(mpsc::Sender<ActorPath>),
    SpawnAnonymous(mpsc::Sender<(ActorPath, ActorPath)>),
    SystemName(mpsc::Sender<String>),
    ParentPath(mpsc::Sender<ActorPath>),
    Children(mpsc::Sender<Vec<ActorPath>>),
    ChildNamed {
        name: String,
        reply_to: mpsc::Sender<Option<ActorPath>>,
    },
}

struct Parent;

impl Actor for Parent {
    type Msg = ParentMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ParentMsg::SpawnNamed(reply_to) => {
                let child = ctx.spawn("child", Props::new(|| Noop))?;
                reply_to
                    .send(child.path().clone())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ParentMsg::SpawnAnonymous(reply_to) => {
                let first = ctx.spawn_anonymous(Props::new(|| Noop))?;
                let second = ctx.spawn_anonymous(Props::new(|| Noop))?;
                reply_to
                    .send((first.path().clone(), second.path().clone()))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ParentMsg::SystemName(reply_to) => {
                reply_to
                    .send(ctx.system().name().to_string())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ParentMsg::ParentPath(reply_to) => {
                reply_to
                    .send(ctx.parent().path().clone())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ParentMsg::Children(reply_to) => {
                let paths = ctx
                    .children()
                    .into_iter()
                    .map(|child| child.path().clone())
                    .collect();
                reply_to
                    .send(paths)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ParentMsg::ChildNamed { name, reply_to } => {
                let path = ctx.child(&name).map(|child| child.path().clone());
                reply_to
                    .send(path)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[test]
fn context_spawn_places_children_under_parent_path() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system.spawn("parent", Props::new(|| Parent)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    parent.tell(ParentMsg::SpawnNamed(reply_tx)).unwrap();

    let child_path = reply_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(
        child_path
            .as_str()
            .starts_with(&format!("{}/child#", parent.path()))
    );
}

#[test]
fn context_spawn_anonymous_creates_unique_child_names() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system.spawn("parent", Props::new(|| Parent)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    parent.tell(ParentMsg::SpawnAnonymous(reply_tx)).unwrap();

    let (first, second) = reply_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_ne!(first, second);
    assert!(
        first
            .as_str()
            .starts_with(&format!("{}/$anon-", parent.path()))
    );
    assert!(
        second
            .as_str()
            .starts_with(&format!("{}/$anon-", parent.path()))
    );
}

#[test]
fn context_exposes_actor_system_handle() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system.spawn("parent", Props::new(|| Parent)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    parent.tell(ParentMsg::SystemName(reply_tx)).unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "test"
    );
}

#[test]
fn context_parent_points_to_user_root_for_top_level_actor() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system.spawn("parent", Props::new(|| Parent)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    parent.tell(ParentMsg::ParentPath(reply_tx)).unwrap();

    assert_eq!(
        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .as_str(),
        "kairo://test/user"
    );
}

#[test]
fn actor_path_exposes_structured_address_name_parent_and_uid() {
    let path = ActorPath::new("kairo://test/user/parent#7/child#9");

    assert_eq!(path.address().protocol(), "kairo");
    assert_eq!(path.address().system(), "test");
    assert_eq!(path.name(), Some("child"));
    assert_eq!(path.uid(), Some(9));
    assert_eq!(
        path.parent().unwrap().as_str(),
        "kairo://test/user/parent#7"
    );
}

#[test]
fn context_children_and_child_lookup_reflect_live_children() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system.spawn("parent", Props::new(|| Parent)).unwrap();
    let (spawn_tx, spawn_rx) = mpsc::channel();
    let (children_tx, children_rx) = mpsc::channel();
    let (child_tx, child_rx) = mpsc::channel();
    let (missing_tx, missing_rx) = mpsc::channel();

    parent.tell(ParentMsg::SpawnNamed(spawn_tx)).unwrap();
    let child_path = spawn_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent.tell(ParentMsg::Children(children_tx)).unwrap();
    let children = children_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent
        .tell(ParentMsg::ChildNamed {
            name: "child".to_string(),
            reply_to: child_tx,
        })
        .unwrap();
    parent
        .tell(ParentMsg::ChildNamed {
            name: "missing".to_string(),
            reply_to: missing_tx,
        })
        .unwrap();

    assert!(children.contains(&child_path));
    assert_eq!(
        child_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Some(child_path)
    );
    assert_eq!(
        missing_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        None
    );
}

enum ChildStopMsg {
    SpawnChild {
        stopped: mpsc::Sender<()>,
        reply_to: mpsc::Sender<ActorPath>,
    },
    StopChild {
        reply_to: mpsc::Sender<()>,
    },
    StopOther {
        other: ActorRef<()>,
        reply_to: mpsc::Sender<String>,
    },
    ChildPath(mpsc::Sender<Option<ActorPath>>),
    Ping(mpsc::Sender<&'static str>),
}

struct ChildStoppingParent {
    child: Option<ActorRef<()>>,
}

impl Actor for ChildStoppingParent {
    type Msg = ChildStopMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ChildStopMsg::SpawnChild { stopped, reply_to } => {
                let child = ctx.spawn("child", Props::new(move || StopProbe { stopped }))?;
                reply_to
                    .send(child.path().clone())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                self.child = Some(child);
            }
            ChildStopMsg::StopChild { reply_to } => {
                if let Some(child) = self.child.clone() {
                    ctx.stop(child)?;
                }
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ChildStopMsg::StopOther { other, reply_to } => {
                let error = ctx.stop(other).unwrap_err();
                reply_to
                    .send(error.to_string())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ChildStopMsg::ChildPath(reply_to) => {
                let path = ctx.child("child").map(|child| child.path().clone());
                reply_to
                    .send(path)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ChildStopMsg::Ping(reply_to) => {
                reply_to
                    .send("alive")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[test]
fn context_stop_can_stop_direct_child_without_stopping_parent() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn("parent", Props::new(|| ChildStoppingParent { child: None }))
        .unwrap();
    let (child_stopped_tx, child_stopped_rx) = mpsc::channel();
    let (spawn_tx, spawn_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let (child_lookup_tx, child_lookup_rx) = mpsc::channel();
    let (ping_tx, ping_rx) = mpsc::channel();

    parent
        .tell(ChildStopMsg::SpawnChild {
            stopped: child_stopped_tx,
            reply_to: spawn_tx,
        })
        .unwrap();
    let child_path = spawn_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent
        .tell(ChildStopMsg::StopChild { reply_to: stop_tx })
        .unwrap();
    stop_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    child_stopped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    parent.tell(ChildStopMsg::Ping(ping_tx)).unwrap();
    parent
        .tell(ChildStopMsg::ChildPath(child_lookup_tx))
        .unwrap();

    assert_eq!(
        ping_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "alive"
    );
    assert_eq!(
        child_lookup_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        None
    );
    assert!(
        child_path
            .as_str()
            .starts_with(&format!("{}/child#", parent.path()))
    );
}

#[test]
fn context_stop_rejects_actor_that_is_not_self_or_direct_child() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn("parent", Props::new(|| ChildStoppingParent { child: None }))
        .unwrap();
    let other = system.spawn("other", Props::new(|| Noop)).unwrap();
    let (error_tx, error_rx) = mpsc::channel();
    let (ping_tx, ping_rx) = mpsc::channel();

    parent
        .tell(ChildStopMsg::StopOther {
            other,
            reply_to: error_tx,
        })
        .unwrap();
    parent.tell(ChildStopMsg::Ping(ping_tx)).unwrap();

    let error = error_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(error.contains("is not self or a direct child"));
    assert_eq!(
        ping_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "alive"
    );
}

#[derive(Debug, PartialEq, Eq)]
enum StopEvent {
    Child,
    Parent,
}

struct LifecycleChild {
    events: mpsc::Sender<StopEvent>,
}

impl Actor for LifecycleChild {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.events
            .send(StopEvent::Child)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

struct LifecycleParent {
    ready: mpsc::Sender<()>,
    events: mpsc::Sender<StopEvent>,
}

impl Actor for LifecycleParent {
    type Msg = ();

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let events = self.events.clone();
        ctx.spawn(
            "child",
            Props::new(move || LifecycleChild {
                events: events.clone(),
            }),
        )?;
        self.ready
            .send(())
            .map_err(|error| ActorError::Message(error.to_string()))
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.events
            .send(StopEvent::Parent)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

#[test]
fn parent_stop_waits_for_children_before_stopped_hook() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (events_tx, events_rx) = mpsc::channel();
    let parent = system
        .spawn(
            "parent",
            Props::new(move || LifecycleParent {
                ready: ready_tx,
                events: events_tx,
            }),
        )
        .unwrap();
    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&parent);

    let first = events_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let second = events_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(first, StopEvent::Child);
    assert_eq!(second, StopEvent::Parent);
    assert!(parent.wait_for_stop(Duration::from_secs(1)));
}

struct SignalProbe {
    signals: mpsc::Sender<Signal>,
}

impl Actor for SignalProbe {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn signal(&mut self, _ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        self.signals
            .send(signal)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

#[test]
fn post_stop_signal_is_delivered_during_termination() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (signals_tx, signals_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "signal-probe",
            Props::new(move || SignalProbe {
                signals: signals_tx,
            }),
        )
        .unwrap();

    system.stop(&actor);

    assert_eq!(
        signals_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Signal::PostStop
    );
    assert!(actor.wait_for_stop(Duration::from_secs(1)));
}

enum SupervisionMsg {
    Increment,
    Fail,
    Panic,
    Get(mpsc::Sender<usize>),
}

struct SupervisionProbe {
    value: usize,
    restarted: Option<mpsc::Sender<()>>,
}

impl Actor for SupervisionProbe {
    type Msg = SupervisionMsg;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.value = 0;
        Ok(())
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            SupervisionMsg::Increment => {
                self.value += 1;
                Ok(())
            }
            SupervisionMsg::Fail => Err(ActorError::Message("boom".to_string())),
            SupervisionMsg::Panic => panic!("panic boom"),
            SupervisionMsg::Get(reply_to) => reply_to
                .send(self.value)
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }

    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::PreRestart => {
                if let Some(restarted) = &self.restarted {
                    restarted
                        .send(())
                        .map_err(|error| ActorError::Message(error.to_string()))?;
                }
                Ok(())
            }
            Signal::PostStop => self.stopped(ctx),
            Signal::Terminated(_) | Signal::ChildFailed { .. } => Ok(()),
        }
    }
}

enum StartupProbeMsg {
    GetStartCount(mpsc::Sender<u64>),
}

struct StartupProbe {
    starts: Arc<AtomicU64>,
    pre_restarts: Arc<AtomicU64>,
    fail_until: u64,
}

impl Actor for StartupProbe {
    type Msg = StartupProbeMsg;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        let start = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
        if start <= self.fail_until {
            Err(ActorError::Message(format!("startup boom {start}")))
        } else {
            Ok(())
        }
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            StartupProbeMsg::GetStartCount(reply_to) => reply_to
                .send(self.starts.load(Ordering::SeqCst))
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }

    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::PreRestart => {
                self.pre_restarts.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            Signal::PostStop => self.stopped(ctx),
            Signal::Terminated(_) | Signal::ChildFailed { .. } => Ok(()),
        }
    }
}

struct StartupPanicProbe {
    starts: Arc<AtomicU64>,
    panic_until: u64,
}

impl Actor for StartupPanicProbe {
    type Msg = StartupProbeMsg;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        let start = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
        if start <= self.panic_until {
            panic!("startup panic {start}");
        }
        Ok(())
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            StartupProbeMsg::GetStartCount(reply_to) => reply_to
                .send(self.starts.load(Ordering::SeqCst))
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }
}

#[test]
fn startup_failure_stops_actor_by_default() {
    let system = ActorSystem::builder("test").build().unwrap();
    let starts = Arc::new(AtomicU64::new(0));
    let actor = system
        .spawn(
            "startup-probe",
            Props::new({
                let starts = Arc::clone(&starts);
                move || StartupProbe {
                    starts,
                    pre_restarts: Arc::new(AtomicU64::new(0)),
                    fail_until: u64::MAX,
                }
            }),
        )
        .unwrap();

    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert!(
        actor
            .tell(StartupProbeMsg::GetStartCount(mpsc::channel().0))
            .is_err()
    );
}

#[test]
fn bounded_restart_supervision_retries_startup_failure() {
    let system = ActorSystem::builder("test").build().unwrap();
    let starts = Arc::new(AtomicU64::new(0));
    let pre_restarts = Arc::new(AtomicU64::new(0));
    let actor = system
        .spawn(
            "startup-probe",
            Props::restartable({
                let starts = Arc::clone(&starts);
                let pre_restarts = Arc::clone(&pre_restarts);
                move || StartupProbe {
                    starts: Arc::clone(&starts),
                    pre_restarts: Arc::clone(&pre_restarts),
                    fail_until: 1,
                }
            })
            .with_supervisor(SupervisorStrategy::restart_with_limit(
                2,
                Duration::from_secs(60),
            )),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(StartupProbeMsg::GetStartCount(reply_tx))
        .unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
    assert_eq!(pre_restarts.load(Ordering::SeqCst), 0);
    assert!(!actor.is_stopped());
}

#[test]
fn bounded_restart_supervision_stops_when_startup_limit_is_exceeded() {
    let system = ActorSystem::builder("test").build().unwrap();
    let starts = Arc::new(AtomicU64::new(0));
    let actor = system
        .spawn(
            "startup-probe",
            Props::restartable({
                let starts = Arc::clone(&starts);
                move || StartupProbe {
                    starts: Arc::clone(&starts),
                    pre_restarts: Arc::new(AtomicU64::new(0)),
                    fail_until: u64::MAX,
                }
            })
            .with_supervisor(SupervisorStrategy::restart_with_limit(
                2,
                Duration::from_secs(60),
            )),
        )
        .unwrap();

    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(starts.load(Ordering::SeqCst), 2);
}

#[test]
fn startup_panic_enters_bounded_restart_supervision() {
    let system = ActorSystem::builder("test").build().unwrap();
    let starts = Arc::new(AtomicU64::new(0));
    let actor = system
        .spawn(
            "startup-panic-probe",
            Props::restartable({
                let starts = Arc::clone(&starts);
                move || StartupPanicProbe {
                    starts: Arc::clone(&starts),
                    panic_until: 1,
                }
            })
            .with_supervisor(SupervisorStrategy::restart_with_limit(
                2,
                Duration::from_secs(60),
            )),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(StartupProbeMsg::GetStartCount(reply_tx))
        .unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
    assert!(!actor.is_stopped());
}

#[test]
fn default_supervision_stops_actor_on_failure() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "supervised",
            Props::new(|| SupervisionProbe {
                value: 0,
                restarted: None,
            }),
        )
        .unwrap();

    actor.tell(SupervisionMsg::Fail).unwrap();

    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    assert!(actor.tell(SupervisionMsg::Increment).is_err());
}

#[test]
fn default_supervision_stops_actor_on_receive_panic() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "supervised",
            Props::new(|| SupervisionProbe {
                value: 0,
                restarted: None,
            }),
        )
        .unwrap();

    actor.tell(SupervisionMsg::Panic).unwrap();

    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    assert!(actor.tell(SupervisionMsg::Increment).is_err());
}

#[test]
fn restart_supervision_rebuilds_actor_after_receive_panic() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (restarted_tx, restarted_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "supervised",
            Props::restartable(move || SupervisionProbe {
                value: 0,
                restarted: Some(restarted_tx.clone()),
            }),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor.tell(SupervisionMsg::Increment).unwrap();
    actor.tell(SupervisionMsg::Panic).unwrap();
    restarted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    actor.tell(SupervisionMsg::Get(reply_tx)).unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 0);
    assert!(!actor.is_stopped());
}

#[test]
fn resume_supervision_keeps_actor_state_after_failure() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "supervised",
            Props::new(|| SupervisionProbe {
                value: 0,
                restarted: None,
            })
            .with_supervisor(SupervisorStrategy::Resume),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor.tell(SupervisionMsg::Increment).unwrap();
    actor.tell(SupervisionMsg::Fail).unwrap();
    actor.tell(SupervisionMsg::Get(reply_tx)).unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    assert!(!actor.is_stopped());
}

#[test]
fn restart_supervision_rebuilds_actor_state_and_keeps_ref_path() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (restarted_tx, restarted_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "supervised",
            Props::restartable(move || SupervisionProbe {
                value: 0,
                restarted: Some(restarted_tx.clone()),
            }),
        )
        .unwrap();
    let path = actor.path().clone();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor.tell(SupervisionMsg::Increment).unwrap();
    actor.tell(SupervisionMsg::Fail).unwrap();
    restarted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    actor.tell(SupervisionMsg::Get(reply_tx)).unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 0);
    assert_eq!(actor.path(), &path);
    assert!(!actor.is_stopped());
}

#[test]
fn restart_supervision_stops_when_restart_limit_is_exceeded() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (restarted_tx, restarted_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "supervised",
            Props::restartable(move || SupervisionProbe {
                value: 0,
                restarted: Some(restarted_tx.clone()),
            })
            .with_supervisor(SupervisorStrategy::restart_with_limit(
                1,
                Duration::from_secs(60),
            )),
        )
        .unwrap();

    actor.tell(SupervisionMsg::Fail).unwrap();
    restarted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    actor.tell(SupervisionMsg::Fail).unwrap();

    assert!(actor.wait_for_stop(Duration::from_secs(1)));
}

#[test]
fn restart_supervision_limit_resets_after_time_window() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (restarted_tx, restarted_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "supervised",
            Props::restartable(move || SupervisionProbe {
                value: 0,
                restarted: Some(restarted_tx.clone()),
            })
            .with_supervisor(SupervisorStrategy::restart_with_limit(
                1,
                Duration::from_millis(25),
            )),
        )
        .unwrap();

    actor.tell(SupervisionMsg::Fail).unwrap();
    restarted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    thread::sleep(Duration::from_millis(60));
    actor.tell(SupervisionMsg::Fail).unwrap();
    restarted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    actor.tell(SupervisionMsg::Fail).unwrap();

    assert!(actor.wait_for_stop(Duration::from_secs(1)));
}

enum RestartParentMsg {
    SpawnChild {
        stopped: mpsc::Sender<()>,
        reply_to: mpsc::Sender<()>,
    },
    Fail,
    ChildCount(mpsc::Sender<usize>),
}

struct RestartParent;

impl Actor for RestartParent {
    type Msg = RestartParentMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RestartParentMsg::SpawnChild { stopped, reply_to } => {
                ctx.spawn("child", Props::new(move || StopProbe { stopped }))?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            RestartParentMsg::Fail => Err(ActorError::Message("boom".to_string())),
            RestartParentMsg::ChildCount(reply_to) => reply_to
                .send(ctx.children().len())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }
}

#[test]
fn restart_supervision_stops_children_by_default() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn("parent", Props::restartable(|| RestartParent))
        .unwrap();
    let (child_stopped_tx, child_stopped_rx) = mpsc::channel();
    let (spawned_tx, spawned_rx) = mpsc::channel();
    let (count_tx, count_rx) = mpsc::channel();

    parent
        .tell(RestartParentMsg::SpawnChild {
            stopped: child_stopped_tx,
            reply_to: spawned_tx,
        })
        .unwrap();
    spawned_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent.tell(RestartParentMsg::Fail).unwrap();
    child_stopped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    parent.tell(RestartParentMsg::ChildCount(count_tx)).unwrap();

    assert_eq!(count_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 0);
}

#[test]
fn restart_supervision_can_preserve_children() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn(
            "parent",
            Props::restartable(|| RestartParent)
                .with_supervisor(SupervisorStrategy::restart_preserving_children()),
        )
        .unwrap();
    let (child_stopped_tx, child_stopped_rx) = mpsc::channel();
    let (spawned_tx, spawned_rx) = mpsc::channel();
    let (count_tx, count_rx) = mpsc::channel();

    parent
        .tell(RestartParentMsg::SpawnChild {
            stopped: child_stopped_tx,
            reply_to: spawned_tx,
        })
        .unwrap();
    spawned_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent.tell(RestartParentMsg::Fail).unwrap();
    parent.tell(RestartParentMsg::ChildCount(count_tx)).unwrap();

    assert_eq!(count_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    assert!(child_stopped_rx.try_recv().is_err());
}

enum EscalatingChildMsg {
    Fail,
}

struct EscalatingChild {
    stopped: Option<mpsc::Sender<()>>,
}

impl Actor for EscalatingChild {
    type Msg = EscalatingChildMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            EscalatingChildMsg::Fail => Err(ActorError::Message("child boom".to_string())),
        }
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        if let Some(stopped) = &self.stopped {
            stopped
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }
}

enum EscalationParentMsg {
    SpawnChild {
        child_stopped: Option<mpsc::Sender<()>>,
        reply_to: mpsc::Sender<ActorRef<EscalatingChildMsg>>,
    },
    SpawnStartupFailingChild {
        starts: Arc<AtomicU64>,
        reply_to: mpsc::Sender<ActorRef<StartupProbeMsg>>,
    },
    Ping(mpsc::Sender<()>),
}

struct EscalationParent {
    restarted: Option<mpsc::Sender<()>>,
}

impl Actor for EscalationParent {
    type Msg = EscalationParentMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            EscalationParentMsg::SpawnChild {
                child_stopped,
                reply_to,
            } => {
                let child = ctx.spawn(
                    "child",
                    Props::new(move || EscalatingChild {
                        stopped: child_stopped,
                    })
                    .with_supervisor(SupervisorStrategy::Escalate),
                )?;
                reply_to
                    .send(child)
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            EscalationParentMsg::SpawnStartupFailingChild { starts, reply_to } => {
                let child = ctx.spawn(
                    "startup-failing-child",
                    Props::new(move || StartupProbe {
                        starts,
                        pre_restarts: Arc::new(AtomicU64::new(0)),
                        fail_until: u64::MAX,
                    })
                    .with_supervisor(SupervisorStrategy::Escalate),
                )?;
                reply_to
                    .send(child)
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            EscalationParentMsg::Ping(reply_to) => reply_to
                .send(())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }

    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::PreRestart => {
                if let Some(restarted) = &self.restarted {
                    restarted
                        .send(())
                        .map_err(|error| ActorError::Message(error.to_string()))?;
                }
                Ok(())
            }
            Signal::PostStop => self.stopped(ctx),
            Signal::Terminated(_) | Signal::ChildFailed { .. } => Ok(()),
        }
    }
}

#[test]
fn escalate_supervision_stops_parent_by_default() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn(
            "parent",
            Props::new(|| EscalationParent { restarted: None }),
        )
        .unwrap();
    let (child_tx, child_rx) = mpsc::channel();

    parent
        .tell(EscalationParentMsg::SpawnChild {
            child_stopped: None,
            reply_to: child_tx,
        })
        .unwrap();
    let child = child_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    child.tell(EscalatingChildMsg::Fail).unwrap();

    assert!(parent.wait_for_stop(Duration::from_secs(1)));
    assert!(child.wait_for_stop(Duration::from_secs(1)));
}

#[test]
fn escalate_supervision_restarts_parent_when_parent_strategy_restarts() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (restarted_tx, restarted_rx) = mpsc::channel();
    let parent = system
        .spawn(
            "parent",
            Props::restartable(move || EscalationParent {
                restarted: Some(restarted_tx.clone()),
            }),
        )
        .unwrap();
    let (child_tx, child_rx) = mpsc::channel();
    let (child_stopped_tx, child_stopped_rx) = mpsc::channel();

    parent
        .tell(EscalationParentMsg::SpawnChild {
            child_stopped: Some(child_stopped_tx),
            reply_to: child_tx,
        })
        .unwrap();
    let child = child_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    child.tell(EscalatingChildMsg::Fail).unwrap();
    restarted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    child_stopped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    let (ping_tx, ping_rx) = mpsc::channel();
    parent.tell(EscalationParentMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(!parent.is_stopped());
}

#[test]
fn startup_failure_escalates_to_parent_supervision() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn(
            "parent",
            Props::new(|| EscalationParent { restarted: None }),
        )
        .unwrap();
    let starts = Arc::new(AtomicU64::new(0));
    let (child_tx, child_rx) = mpsc::channel();

    parent
        .tell(EscalationParentMsg::SpawnStartupFailingChild {
            starts: Arc::clone(&starts),
            reply_to: child_tx,
        })
        .unwrap();
    let child = child_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert!(child.wait_for_stop(Duration::from_secs(1)));
    assert!(parent.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(starts.load(Ordering::SeqCst), 1);
}

enum BackoffChildMsg {
    Stop,
}

struct BackoffChild {
    generation: u64,
    started: mpsc::Sender<u64>,
}

impl Actor for BackoffChild {
    type Msg = BackoffChildMsg;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.started
            .send(self.generation)
            .map_err(|error| ActorError::Message(error.to_string()))
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            BackoffChildMsg::Stop => ctx.stop(ctx.myself())?,
        }
        Ok(())
    }
}

#[test]
fn backoff_supervisor_restarts_child_after_delay() {
    let manual = ManualScheduler::new();
    let system = ActorSystem::builder("test")
        .manual_scheduler(manual.clone())
        .build()
        .unwrap();
    let settings =
        BackoffSupervisorSettings::new(Duration::from_millis(50), Duration::from_millis(200))
            .unwrap();
    let next_generation = Arc::new(AtomicU64::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let child_factory = {
        let next_generation = Arc::clone(&next_generation);
        move || {
            let generation = next_generation.fetch_add(1, Ordering::Relaxed) + 1;
            let started = started_tx.clone();
            Props::new(move || BackoffChild {
                generation,
                started,
            })
        }
    };
    let supervisor = system
        .spawn(
            "backoff",
            BackoffSupervisor::<BackoffChild>::on_stop("child", child_factory, settings),
        )
        .unwrap();
    let (current_tx, current_rx) = mpsc::channel();
    let current_probe = system
        .spawn(
            "current-child-probe",
            Props::new(move || ChannelProbe {
                observed: current_tx,
            }),
        )
        .unwrap();
    let (count_tx, count_rx) = mpsc::channel();
    let count_probe = system
        .spawn(
            "restart-count-probe",
            Props::new(move || ChannelProbe { observed: count_tx }),
        )
        .unwrap();

    assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    supervisor
        .tell(BackoffSupervisorMsg::GetCurrentChild {
            reply_to: current_probe.clone(),
        })
        .unwrap();
    let first_child = current_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .child()
        .unwrap();
    let first_path = first_child.path().clone();

    first_child.tell(BackoffChildMsg::Stop).unwrap();
    assert!(first_child.wait_for_stop(Duration::from_secs(1)));

    let mut restart_count = None;
    for _ in 0..100 {
        supervisor
            .tell(BackoffSupervisorMsg::GetRestartCount {
                reply_to: count_probe.clone(),
            })
            .unwrap();
        let count = count_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .count();
        if count == 1 {
            restart_count = Some(count);
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(restart_count, Some(1));

    manual.advance(Duration::from_millis(49));
    assert!(started_rx.recv_timeout(Duration::from_millis(100)).is_err());

    manual.advance(Duration::from_millis(1));
    assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
    supervisor
        .tell(BackoffSupervisorMsg::GetCurrentChild {
            reply_to: current_probe,
        })
        .unwrap();
    let second_child = current_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .child()
        .unwrap();

    assert_ne!(second_child.path(), &first_path);
}

enum WatchProbeMsg {
    WatchTwice {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<()>,
    },
    WatchSelf {
        reply_to: mpsc::Sender<Result<(), ActorError>>,
    },
    WatchWithSelf {
        reply_to: mpsc::Sender<Result<(), ActorError>>,
    },
    WatchFailing {
        subject: ActorRef<SupervisionMsg>,
        reply_to: mpsc::Sender<()>,
    },
    WatchWith {
        subject: ActorRef<()>,
        registered: mpsc::Sender<()>,
        observed: mpsc::Sender<ActorPath>,
    },
    WatchThenWatchWith {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<Result<(), ActorError>>,
    },
    WatchWithThenWatch {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<Result<(), ActorError>>,
    },
    Observed(ActorPath),
    Unwatch {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<()>,
    },
}

struct WatchProbe {
    terminated: mpsc::Sender<ActorPath>,
    custom: Option<mpsc::Sender<ActorPath>>,
}

impl Actor for WatchProbe {
    type Msg = WatchProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            WatchProbeMsg::WatchTwice { subject, reply_to } => {
                ctx.watch(&subject)?;
                ctx.watch(&subject)?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatchProbeMsg::WatchSelf { reply_to } => reply_to
                .send(ctx.watch(&ctx.myself()))
                .map_err(|error| ActorError::Message(error.to_string()))?,
            WatchProbeMsg::WatchWithSelf { reply_to } => {
                let myself = ctx.myself();
                reply_to
                    .send(ctx.watch_with(&myself, WatchProbeMsg::Observed(myself.path().clone())))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatchProbeMsg::WatchFailing { subject, reply_to } => {
                ctx.watch(&subject)?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatchProbeMsg::WatchWith {
                subject,
                registered,
                observed,
            } => {
                let path = subject.path().clone();
                self.custom = Some(observed);
                ctx.watch_with(&subject, WatchProbeMsg::Observed(path))?;
                registered
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatchProbeMsg::WatchThenWatchWith { subject, reply_to } => {
                ctx.watch(&subject)?;
                reply_to
                    .send(ctx.watch_with(&subject, WatchProbeMsg::Observed(subject.path().clone())))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatchProbeMsg::WatchWithThenWatch { subject, reply_to } => {
                ctx.watch_with(&subject, WatchProbeMsg::Observed(subject.path().clone()))?;
                reply_to
                    .send(ctx.watch(&subject))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatchProbeMsg::Observed(path) => {
                if let Some(observed) = self.custom.take() {
                    observed
                        .send(path)
                        .map_err(|error| ActorError::Message(error.to_string()))?;
                }
            }
            WatchProbeMsg::Unwatch { subject, reply_to } => {
                ctx.watch(&subject)?;
                ctx.unwatch(&subject);
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }

    fn signal(&mut self, _ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        if let Signal::Terminated(actor) = signal {
            self.terminated
                .send(actor.path().clone())
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }
}

enum SignalFailureWatcherMsg {
    Watch {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<()>,
    },
    Ping(mpsc::Sender<()>),
}

struct SignalFailureWatcher {
    pre_restart: Option<mpsc::Sender<()>>,
}

impl Actor for SignalFailureWatcher {
    type Msg = SignalFailureWatcherMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            SignalFailureWatcherMsg::Watch { subject, reply_to } => {
                ctx.watch(&subject)?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            SignalFailureWatcherMsg::Ping(reply_to) => reply_to
                .send(())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }

    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::Terminated(_) => Err(ActorError::Message("signal boom".to_string())),
            Signal::PreRestart => {
                if let Some(pre_restart) = &self.pre_restart {
                    pre_restart
                        .send(())
                        .map_err(|error| ActorError::Message(error.to_string()))?;
                }
                Ok(())
            }
            Signal::PostStop => self.stopped(ctx),
            Signal::ChildFailed { .. } => Ok(()),
        }
    }
}

enum ParentWatchMsg {
    FailChild,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParentWatchSignal {
    Terminated(ActorPath),
    ChildFailed { path: ActorPath, reason: String },
}

struct ParentWatchProbe {
    observed: mpsc::Sender<ParentWatchSignal>,
    child: Option<ActorRef<SupervisionMsg>>,
}

impl Actor for ParentWatchProbe {
    type Msg = ParentWatchMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let child = ctx.spawn(
            "child",
            Props::new(|| SupervisionProbe {
                value: 0,
                restarted: None,
            }),
        )?;
        ctx.watch(&child)?;
        self.child = Some(child);
        Ok(())
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ParentWatchMsg::FailChild => {
                let child = self
                    .child
                    .as_ref()
                    .expect("child should be spawned before messages are processed");
                child
                    .tell(SupervisionMsg::Fail)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }

    fn signal(&mut self, _ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::ChildFailed { actor, reason } => {
                self.observed
                    .send(ParentWatchSignal::ChildFailed {
                        path: actor.path().clone(),
                        reason,
                    })
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            Signal::Terminated(actor) => {
                self.observed
                    .send(ParentWatchSignal::Terminated(actor.path().clone()))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            Signal::PreRestart | Signal::PostStop => {}
        }
        Ok(())
    }
}

#[test]
fn watch_delivers_terminated_signal_once() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    let (terminated_tx, terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || WatchProbe {
                terminated: terminated_tx,
                custom: None,
            }),
        )
        .unwrap();
    let (registered_tx, registered_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchTwice {
            subject: subject.clone(),
            reply_to: registered_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&subject);

    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject.path().clone()
    );
    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn watch_self_returns_explicit_error() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (terminated_tx, _terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || WatchProbe {
                terminated: terminated_tx,
                custom: None,
            }),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchSelf { reply_to: reply_tx })
        .unwrap();

    assert!(matches!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Err(ActorError::InvalidWatchTarget { actor }) if actor == watcher.path().to_string()
    ));
    assert!(!watcher.is_stopped());
}

#[test]
fn watch_with_self_returns_explicit_error() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (terminated_tx, _terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || WatchProbe {
                terminated: terminated_tx,
                custom: None,
            }),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchWithSelf { reply_to: reply_tx })
        .unwrap();

    assert!(matches!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Err(ActorError::InvalidWatchTarget { actor }) if actor == watcher.path().to_string()
    ));
    assert!(!watcher.is_stopped());
}

#[test]
fn watch_then_watch_with_requires_unwatch_first() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (subject_stopped_tx, _subject_stopped_rx) = mpsc::channel();
    let subject = system
        .spawn(
            "subject",
            Props::new(move || StopProbe {
                stopped: subject_stopped_tx,
            }),
        )
        .unwrap();
    let (terminated_tx, _terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || WatchProbe {
                terminated: terminated_tx,
                custom: None,
            }),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchThenWatchWith {
            subject: subject.clone(),
            reply_to: reply_tx,
        })
        .unwrap();

    assert!(matches!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Err(ActorError::AlreadyWatching { actor, watcher: current_watcher })
            if actor == subject.path().to_string()
                && current_watcher == watcher.path().to_string()
    ));
    assert!(!watcher.is_stopped());
}

#[test]
fn watch_with_then_watch_requires_unwatch_first() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (subject_stopped_tx, _subject_stopped_rx) = mpsc::channel();
    let subject = system
        .spawn(
            "subject",
            Props::new(move || StopProbe {
                stopped: subject_stopped_tx,
            }),
        )
        .unwrap();
    let (terminated_tx, _terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || WatchProbe {
                terminated: terminated_tx,
                custom: None,
            }),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchWithThenWatch {
            subject: subject.clone(),
            reply_to: reply_tx,
        })
        .unwrap();

    assert!(matches!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Err(ActorError::AlreadyWatching { actor, watcher: current_watcher })
            if actor == subject.path().to_string()
                && current_watcher == watcher.path().to_string()
    ));
    assert!(!watcher.is_stopped());
}

#[test]
fn signal_failure_stops_actor_by_default() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (subject_stopped_tx, _subject_stopped_rx) = mpsc::channel();
    let subject = system
        .spawn(
            "subject",
            Props::new(move || StopProbe {
                stopped: subject_stopped_tx,
            }),
        )
        .unwrap();
    let watcher = system
        .spawn(
            "signal-failure-watcher",
            Props::new(|| SignalFailureWatcher { pre_restart: None }),
        )
        .unwrap();
    let (registered_tx, registered_rx) = mpsc::channel();

    watcher
        .tell(SignalFailureWatcherMsg::Watch {
            subject: subject.clone(),
            reply_to: registered_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    system.stop(&subject);

    assert!(watcher.wait_for_stop(Duration::from_secs(1)));
}

#[test]
fn restart_supervision_rebuilds_actor_after_signal_failure() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (subject_stopped_tx, _subject_stopped_rx) = mpsc::channel();
    let subject = system
        .spawn(
            "subject",
            Props::new(move || StopProbe {
                stopped: subject_stopped_tx,
            }),
        )
        .unwrap();
    let (pre_restart_tx, pre_restart_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "signal-failure-watcher",
            Props::restartable(move || SignalFailureWatcher {
                pre_restart: Some(pre_restart_tx.clone()),
            }),
        )
        .unwrap();
    let (registered_tx, registered_rx) = mpsc::channel();
    let (ping_tx, ping_rx) = mpsc::channel();

    watcher
        .tell(SignalFailureWatcherMsg::Watch {
            subject: subject.clone(),
            reply_to: registered_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    system.stop(&subject);
    pre_restart_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    watcher
        .tell(SignalFailureWatcherMsg::Ping(ping_tx))
        .unwrap();

    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(!watcher.is_stopped());
}

#[test]
fn watch_with_delivers_custom_message_after_termination() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    let (terminated_tx, _terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || WatchProbe {
                terminated: terminated_tx,
                custom: None,
            }),
        )
        .unwrap();
    let (registered_tx, registered_rx) = mpsc::channel();
    let (observed_tx, observed_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchWith {
            subject: subject.clone(),
            registered: registered_tx,
            observed: observed_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&subject);

    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject.path().clone()
    );
}

#[test]
fn unwatch_suppresses_later_termination_signal() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    let (terminated_tx, terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || WatchProbe {
                terminated: terminated_tx,
                custom: None,
            }),
        )
        .unwrap();
    let (registered_tx, registered_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::Unwatch {
            subject: subject.clone(),
            reply_to: registered_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&subject);

    assert!(subject.wait_for_stop(Duration::from_secs(1)));
    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn parent_watch_receives_child_failed_when_child_stops_from_failure() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let parent = system
        .spawn(
            "parent",
            Props::new(move || ParentWatchProbe {
                observed: observed_tx,
                child: None,
            }),
        )
        .unwrap();

    parent.tell(ParentWatchMsg::FailChild).unwrap();

    let observed = observed_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let ParentWatchSignal::ChildFailed { path, reason } = observed else {
        panic!("expected child failure signal");
    };
    assert_eq!(path.name(), Some("child"));
    assert_eq!(reason, "boom");
}

#[test]
fn non_parent_watch_receives_plain_terminated_for_failed_actor() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system
        .spawn(
            "subject",
            Props::new(|| SupervisionProbe {
                value: 0,
                restarted: None,
            }),
        )
        .unwrap();
    let (terminated_tx, terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || WatchProbe {
                terminated: terminated_tx,
                custom: None,
            }),
        )
        .unwrap();
    let (registered_tx, registered_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchFailing {
            subject: subject.clone(),
            reply_to: registered_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    subject.tell(SupervisionMsg::Fail).unwrap();

    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject.path().clone()
    );
}

enum TaskProbeMsg {
    PipeNumber {
        reply_to: mpsc::Sender<i32>,
    },
    PipeFailure {
        reply_to: mpsc::Sender<&'static str>,
    },
    SpawnTask {
        reply_to: mpsc::Sender<&'static str>,
    },
    PipedNumber {
        value: i32,
        reply_to: mpsc::Sender<i32>,
    },
    PipedFailure {
        reason: &'static str,
        reply_to: mpsc::Sender<&'static str>,
    },
    TaskDone {
        reply_to: mpsc::Sender<&'static str>,
    },
}

struct TaskProbe;

impl Actor for TaskProbe {
    type Msg = TaskProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            TaskProbeMsg::PipeNumber { reply_to } => {
                ctx.pipe_to_self(
                    || Ok::<i32, &'static str>(41),
                    |result| TaskProbeMsg::PipedNumber {
                        value: result.expect("pipe task should succeed") + 1,
                        reply_to,
                    },
                )?;
            }
            TaskProbeMsg::PipeFailure { reply_to } => {
                ctx.pipe_to_self(
                    || Err::<i32, &'static str>("failed"),
                    |result| TaskProbeMsg::PipedFailure {
                        reason: result.expect_err("pipe task should fail"),
                        reply_to,
                    },
                )?;
            }
            TaskProbeMsg::SpawnTask { reply_to } => {
                ctx.spawn_task(move |myself| {
                    let _ = myself.tell(TaskProbeMsg::TaskDone { reply_to });
                })?;
            }
            TaskProbeMsg::PipedNumber { value, reply_to } => {
                reply_to
                    .send(value)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TaskProbeMsg::PipedFailure { reason, reply_to } => {
                reply_to
                    .send(reason)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TaskProbeMsg::TaskDone { reply_to } => {
                reply_to
                    .send("done")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[test]
fn pipe_to_self_delivers_success_result_through_mailbox() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("task", Props::new(|| TaskProbe)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(TaskProbeMsg::PipeNumber { reply_to: reply_tx })
        .unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 42);
}

#[test]
fn pipe_to_self_delivers_failure_result_through_mailbox() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("task", Props::new(|| TaskProbe)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(TaskProbeMsg::PipeFailure { reply_to: reply_tx })
        .unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "failed"
    );
}

#[test]
fn spawn_task_sends_back_through_actor_ref() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("task", Props::new(|| TaskProbe)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(TaskProbeMsg::SpawnTask { reply_to: reply_tx })
        .unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "done"
    );
}

struct ExternalProbeMsg {
    label: &'static str,
    reply_to: mpsc::Sender<(&'static str, usize)>,
}

enum AdapterProbeMsg {
    CreateAdapter(mpsc::Sender<ActorRef<ExternalProbeMsg>>),
    Adapted(ExternalProbeMsg),
}

struct AdapterProbe {
    adapted_count: usize,
}

impl Actor for AdapterProbe {
    type Msg = AdapterProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            AdapterProbeMsg::CreateAdapter(reply_to) => {
                let adapter = ctx.message_adapter(AdapterProbeMsg::Adapted)?;
                reply_to
                    .send(adapter)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            AdapterProbeMsg::Adapted(message) => {
                self.adapted_count += 1;
                message
                    .reply_to
                    .send((message.label, self.adapted_count))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[test]
fn message_adapter_maps_external_protocol_into_owner_mailbox() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn("adapter", Props::new(|| AdapterProbe { adapted_count: 0 }))
        .unwrap();
    let (adapter_tx, adapter_rx) = mpsc::channel();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(AdapterProbeMsg::CreateAdapter(adapter_tx))
        .unwrap();
    let adapter = adapter_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    adapter
        .tell(ExternalProbeMsg {
            label: "external",
            reply_to: reply_tx,
        })
        .unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("external", 1)
    );
    assert!(
        adapter
            .path()
            .as_str()
            .starts_with(&format!("{}/$adapter-", actor.path()))
    );
}

#[test]
fn message_adapter_rejects_after_owner_stops() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn("adapter", Props::new(|| AdapterProbe { adapted_count: 0 }))
        .unwrap();
    let (adapter_tx, adapter_rx) = mpsc::channel();
    let (reply_tx, _reply_rx) = mpsc::channel();

    actor
        .tell(AdapterProbeMsg::CreateAdapter(adapter_tx))
        .unwrap();
    let adapter = adapter_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    system.stop(&actor);
    assert!(actor.wait_for_stop(Duration::from_secs(1)));

    let error = adapter
        .tell(ExternalProbeMsg {
            label: "late",
            reply_to: reply_tx,
        })
        .unwrap_err();

    assert_eq!(error.reason(), "actor is stopped");
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    assert_eq!(
        system.dead_letters().records()[0].recipient(),
        adapter.path()
    );
}

#[test]
fn actor_system_terminate_stops_top_level_actors() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (stopped_tx, stopped_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "probe",
            Props::new(move || StopProbe {
                stopped: stopped_tx,
            }),
        )
        .unwrap();

    system.terminate(Duration::from_secs(1)).unwrap();

    stopped_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(actor.is_stopped());
    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    assert!(system.is_terminating());
    assert!(system.is_terminated());
}

#[test]
fn actor_system_terminate_rejects_later_spawns() {
    let system = ActorSystem::builder("test").build().unwrap();

    system.terminate(Duration::from_secs(1)).unwrap();
    let error = system.spawn("late", Props::new(|| Noop)).unwrap_err();

    assert!(matches!(error, ActorError::SystemTerminating));
}

#[test]
fn actor_system_terminate_times_out_waiting_for_blocked_actor_start() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (_release_tx, release_rx) = mpsc::channel();
    let received = Arc::new(AtomicU64::new(0));
    let _actor = system
        .spawn(
            "blocked",
            Props::new({
                let received = Arc::clone(&received);
                move || BlockingStart {
                    release: release_rx,
                    received,
                }
            }),
        )
        .unwrap();

    let error = system.terminate(Duration::from_millis(10)).unwrap_err();

    assert!(matches!(error, ActorError::TerminationTimeout));
    assert!(system.is_terminating());
    assert!(!system.is_terminated());
}
