use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use crate::*;

enum CounterMsg {
    Increment,
    Get(mpsc::Sender<usize>),
    Stop,
}

struct Counter {
    value: usize,
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

enum WatchProbeMsg {
    WatchTwice {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<()>,
    },
    WatchWith {
        subject: ActorRef<()>,
        registered: mpsc::Sender<()>,
        observed: mpsc::Sender<ActorPath>,
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

enum ScheduledMsg {
    Record(&'static str),
    ScheduleSelf {
        delay: Duration,
        reply_to: mpsc::Sender<&'static str>,
    },
    SelfFired(mpsc::Sender<&'static str>),
}

struct ScheduledProbe {
    observed: mpsc::Sender<&'static str>,
}

impl Actor for ScheduledProbe {
    type Msg = ScheduledMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ScheduledMsg::Record(label) => {
                self.observed
                    .send(label)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ScheduledMsg::ScheduleSelf { delay, reply_to } => {
                ctx.schedule_once_self(delay, ScheduledMsg::SelfFired(reply_to));
            }
            ScheduledMsg::SelfFired(reply_to) => {
                reply_to
                    .send("self")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[test]
fn actor_system_schedule_once_delivers_message_to_target() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "scheduled",
            Props::new(move || ScheduledProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();

    let cancellable = system.schedule_once(
        Duration::from_millis(10),
        actor,
        ScheduledMsg::Record("scheduled"),
    );

    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "scheduled"
    );
    assert!(cancellable.is_completed());
    assert!(!cancellable.cancel());
}

#[test]
fn cancellable_suppresses_scheduled_message() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "scheduled",
            Props::new(move || ScheduledProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();

    let cancellable = system.schedule_once(
        Duration::from_millis(100),
        actor,
        ScheduledMsg::Record("scheduled"),
    );

    assert!(cancellable.cancel());
    assert!(cancellable.is_cancelled());
    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(150))
            .is_err()
    );
}

#[test]
fn context_schedule_once_self_reenters_actor_mailbox() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, _observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "scheduled",
            Props::new(move || ScheduledProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(ScheduledMsg::ScheduleSelf {
            delay: Duration::from_millis(10),
            reply_to: reply_tx,
        })
        .unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "self"
    );
}

#[derive(Clone)]
enum TimerProbeMsg {
    StartSingle {
        reply_to: mpsc::Sender<(&'static str, bool)>,
    },
    StartThenCancel {
        fired: mpsc::Sender<&'static str>,
        ack: mpsc::Sender<()>,
    },
    Replace {
        fired: mpsc::Sender<&'static str>,
        ack: mpsc::Sender<()>,
    },
    StartRepeating {
        fired: mpsc::Sender<&'static str>,
        ack: mpsc::Sender<()>,
    },
    StartFixedRate {
        fired: mpsc::Sender<&'static str>,
        ack: mpsc::Sender<()>,
    },
    ReplaceRepeating {
        fired: mpsc::Sender<&'static str>,
        ack: mpsc::Sender<()>,
    },
    ReplaceFixedRate {
        fired: mpsc::Sender<&'static str>,
        ack: mpsc::Sender<()>,
    },
    StartThenStop {
        fired: mpsc::Sender<&'static str>,
        ack: mpsc::Sender<()>,
    },
    CancelKey {
        key: &'static str,
        ack: mpsc::Sender<()>,
    },
    Fired {
        key: &'static str,
        label: &'static str,
        reply_to: mpsc::Sender<(&'static str, bool)>,
    },
    FireLabel {
        label: &'static str,
        reply_to: mpsc::Sender<&'static str>,
    },
}

struct TimerProbe;

impl Actor for TimerProbe {
    type Msg = TimerProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            TimerProbeMsg::StartSingle { reply_to } => {
                ctx.start_single_timer(
                    "single",
                    Duration::from_millis(10),
                    TimerProbeMsg::Fired {
                        key: "single",
                        label: "single",
                        reply_to,
                    },
                );
            }
            TimerProbeMsg::StartThenCancel { fired, ack } => {
                ctx.start_single_timer(
                    "cancelled",
                    Duration::ZERO,
                    TimerProbeMsg::FireLabel {
                        label: "cancelled",
                        reply_to: fired,
                    },
                );
                ctx.cancel_timer("cancelled");
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TimerProbeMsg::Replace { fired, ack } => {
                ctx.start_single_timer(
                    "replace",
                    Duration::ZERO,
                    TimerProbeMsg::FireLabel {
                        label: "old",
                        reply_to: fired.clone(),
                    },
                );
                ctx.start_single_timer(
                    "replace",
                    Duration::from_millis(10),
                    TimerProbeMsg::FireLabel {
                        label: "new",
                        reply_to: fired,
                    },
                );
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TimerProbeMsg::StartRepeating { fired, ack } => {
                ctx.start_timer_with_fixed_delay(
                    "repeat",
                    Duration::ZERO,
                    Duration::from_millis(50),
                    TimerProbeMsg::FireLabel {
                        label: "repeat",
                        reply_to: fired,
                    },
                );
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TimerProbeMsg::StartFixedRate { fired, ack } => {
                ctx.start_timer_at_fixed_rate(
                    "rate",
                    Duration::ZERO,
                    Duration::from_millis(50),
                    TimerProbeMsg::FireLabel {
                        label: "rate",
                        reply_to: fired,
                    },
                );
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TimerProbeMsg::ReplaceRepeating { fired, ack } => {
                ctx.start_timer_with_fixed_delay(
                    "repeat-replace",
                    Duration::ZERO,
                    Duration::from_millis(50),
                    TimerProbeMsg::FireLabel {
                        label: "old",
                        reply_to: fired.clone(),
                    },
                );
                ctx.start_timer_with_fixed_delay(
                    "repeat-replace",
                    Duration::from_millis(50),
                    Duration::from_millis(50),
                    TimerProbeMsg::FireLabel {
                        label: "new",
                        reply_to: fired,
                    },
                );
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TimerProbeMsg::ReplaceFixedRate { fired, ack } => {
                ctx.start_timer_at_fixed_rate(
                    "rate-replace",
                    Duration::ZERO,
                    Duration::from_millis(50),
                    TimerProbeMsg::FireLabel {
                        label: "old",
                        reply_to: fired.clone(),
                    },
                );
                ctx.start_timer_at_fixed_rate(
                    "rate-replace",
                    Duration::from_millis(50),
                    Duration::from_millis(50),
                    TimerProbeMsg::FireLabel {
                        label: "new",
                        reply_to: fired,
                    },
                );
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TimerProbeMsg::StartThenStop { fired, ack } => {
                ctx.start_single_timer(
                    "stopped",
                    Duration::from_millis(50),
                    TimerProbeMsg::FireLabel {
                        label: "stopped",
                        reply_to: fired,
                    },
                );
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                ctx.stop(ctx.myself())?;
            }
            TimerProbeMsg::CancelKey { key, ack } => {
                ctx.cancel_timer(key);
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TimerProbeMsg::Fired {
                key,
                label,
                reply_to,
            } => {
                reply_to
                    .send((label, ctx.is_timer_active(key)))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TimerProbeMsg::FireLabel { label, reply_to } => {
                reply_to
                    .send(label)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[test]
fn start_single_timer_delivers_once_and_clears_active_key() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("timer", Props::new(|| TimerProbe)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::StartSingle { reply_to: reply_tx })
        .unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("single", false)
    );
    assert!(reply_rx.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn cancel_timer_suppresses_already_enqueued_timer_message() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("timer", Props::new(|| TimerProbe)).unwrap();
    let (fired_tx, fired_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::StartThenCancel {
            fired: fired_tx,
            ack: ack_tx,
        })
        .unwrap();
    ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert!(fired_rx.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn replacing_timer_suppresses_previous_generation() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("timer", Props::new(|| TimerProbe)).unwrap();
    let (fired_tx, fired_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::Replace {
            fired: fired_tx,
            ack: ack_tx,
        })
        .unwrap();
    ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(
        fired_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "new"
    );
    assert!(fired_rx.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn fixed_delay_timer_repeats_until_cancelled() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("timer", Props::new(|| TimerProbe)).unwrap();
    let (fired_tx, fired_rx) = mpsc::channel();
    let (start_tx, start_rx) = mpsc::channel();
    let (cancel_tx, cancel_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::StartRepeating {
            fired: fired_tx,
            ack: start_tx,
        })
        .unwrap();
    start_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(
        fired_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "repeat"
    );
    assert_eq!(
        fired_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "repeat"
    );

    actor
        .tell(TimerProbeMsg::CancelKey {
            key: "repeat",
            ack: cancel_tx,
        })
        .unwrap();
    cancel_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(fired_rx.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn replacing_fixed_delay_timer_suppresses_previous_generation() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("timer", Props::new(|| TimerProbe)).unwrap();
    let (fired_tx, fired_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();
    let (cancel_tx, cancel_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::ReplaceRepeating {
            fired: fired_tx,
            ack: ack_tx,
        })
        .unwrap();
    ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(
        fired_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "new"
    );
    actor
        .tell(TimerProbeMsg::CancelKey {
            key: "repeat-replace",
            ack: cancel_tx,
        })
        .unwrap();
    cancel_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(fired_rx.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn fixed_rate_timer_repeats_until_cancelled() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("timer", Props::new(|| TimerProbe)).unwrap();
    let (fired_tx, fired_rx) = mpsc::channel();
    let (start_tx, start_rx) = mpsc::channel();
    let (cancel_tx, cancel_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::StartFixedRate {
            fired: fired_tx,
            ack: start_tx,
        })
        .unwrap();
    start_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(
        fired_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "rate"
    );
    assert_eq!(
        fired_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "rate"
    );

    actor
        .tell(TimerProbeMsg::CancelKey {
            key: "rate",
            ack: cancel_tx,
        })
        .unwrap();
    cancel_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(fired_rx.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn replacing_fixed_rate_timer_suppresses_previous_generation() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("timer", Props::new(|| TimerProbe)).unwrap();
    let (fired_tx, fired_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();
    let (cancel_tx, cancel_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::ReplaceFixedRate {
            fired: fired_tx,
            ack: ack_tx,
        })
        .unwrap();
    ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(
        fired_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "new"
    );
    actor
        .tell(TimerProbeMsg::CancelKey {
            key: "rate-replace",
            ack: cancel_tx,
        })
        .unwrap();
    cancel_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(fired_rx.recv_timeout(Duration::from_millis(100)).is_err());
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

#[derive(Clone)]
enum EventStreamProbeMsg {
    Event(&'static str),
    Subscribe(mpsc::Sender<bool>),
    Unsubscribe(mpsc::Sender<bool>),
}

#[derive(Clone)]
struct OtherEvent;

struct EventStreamProbe {
    observed: mpsc::Sender<&'static str>,
}

impl Actor for EventStreamProbe {
    type Msg = EventStreamProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            EventStreamProbeMsg::Event(label) => {
                self.observed
                    .send(label)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            EventStreamProbeMsg::Subscribe(reply_to) => {
                let subscribed = ctx.event_stream().subscribe(ctx.myself());
                reply_to
                    .send(subscribed)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            EventStreamProbeMsg::Unsubscribe(reply_to) => {
                let unsubscribed = ctx.event_stream().unsubscribe(&ctx.myself());
                reply_to
                    .send(unsubscribed)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[test]
fn event_stream_publishes_to_typed_subscribers_once() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "events",
            Props::new(move || EventStreamProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();
    let (first_tx, first_rx) = mpsc::channel();
    let (second_tx, second_rx) = mpsc::channel();

    actor
        .tell(EventStreamProbeMsg::Subscribe(first_tx))
        .unwrap();
    actor
        .tell(EventStreamProbeMsg::Subscribe(second_tx))
        .unwrap();

    assert!(first_rx.recv_timeout(Duration::from_secs(1)).unwrap());
    assert!(!second_rx.recv_timeout(Duration::from_secs(1)).unwrap());

    system
        .event_stream()
        .publish(EventStreamProbeMsg::Event("event"));

    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "event"
    );
    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn event_stream_unsubscribe_stops_delivery() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "events",
            Props::new(move || EventStreamProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();
    let (subscribe_tx, subscribe_rx) = mpsc::channel();
    let (unsubscribe_tx, unsubscribe_rx) = mpsc::channel();

    actor
        .tell(EventStreamProbeMsg::Subscribe(subscribe_tx))
        .unwrap();
    subscribe_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    actor
        .tell(EventStreamProbeMsg::Unsubscribe(unsubscribe_tx))
        .unwrap();

    assert!(unsubscribe_rx.recv_timeout(Duration::from_secs(1)).unwrap());
    system
        .event_stream()
        .publish(EventStreamProbeMsg::Event("event"));

    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn event_stream_matches_exact_event_type() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "events",
            Props::new(move || EventStreamProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();
    let (subscribe_tx, subscribe_rx) = mpsc::channel();

    actor
        .tell(EventStreamProbeMsg::Subscribe(subscribe_tx))
        .unwrap();
    subscribe_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.event_stream().publish(OtherEvent);
    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn actor_stop_cancels_active_timers() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("timer", Props::new(|| TimerProbe)).unwrap();
    let (fired_tx, fired_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::StartThenStop {
            fired: fired_tx,
            ack: ack_tx,
        })
        .unwrap();
    ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    assert!(fired_rx.recv_timeout(Duration::from_millis(100)).is_err());
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
