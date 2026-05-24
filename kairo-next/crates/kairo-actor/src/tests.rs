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
            CounterMsg::Stop => ctx.stop(ctx.myself()),
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
