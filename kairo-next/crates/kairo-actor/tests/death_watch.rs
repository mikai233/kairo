use std::sync::mpsc;
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorPath, ActorRef, ActorResult, ActorSystem, Context, Props, Signal,
};

struct Noop;

impl Actor for Noop {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }
}

enum WatcherMsg {
    WatchStopped {
        subject: ActorRef<()>,
        registered: mpsc::Sender<()>,
    },
    WatchWithStopped {
        subject: ActorRef<()>,
        observed: mpsc::Sender<ActorPath>,
    },
    Observed(ActorPath),
}

struct Watcher {
    terminated: mpsc::Sender<ActorPath>,
    custom_observed: Option<mpsc::Sender<ActorPath>>,
}

impl Actor for Watcher {
    type Msg = WatcherMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            WatcherMsg::WatchStopped {
                subject,
                registered,
            } => {
                ctx.watch(&subject)?;
                registered
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatcherMsg::WatchWithStopped { subject, observed } => {
                let path = subject.path().clone();
                self.custom_observed = Some(observed);
                ctx.watch_with(&subject, WatcherMsg::Observed(path))?;
            }
            WatcherMsg::Observed(path) => {
                if let Some(observed) = self.custom_observed.take() {
                    observed
                        .send(path)
                        .map_err(|error| ActorError::Message(error.to_string()))?;
                }
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
fn watch_stopped_actor_delivers_terminated_immediately() {
    let system = ActorSystem::builder("death-watch").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

    let (terminated_tx, terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || Watcher {
                terminated: terminated_tx,
                custom_observed: None,
            }),
        )
        .unwrap();
    let (registered_tx, registered_rx) = mpsc::channel();

    watcher
        .tell(WatcherMsg::WatchStopped {
            subject: subject.clone(),
            registered: registered_tx,
        })
        .unwrap();

    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject.path().clone()
    );
}

#[test]
fn watch_with_stopped_actor_delivers_custom_message_immediately() {
    let system = ActorSystem::builder("death-watch").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

    let (terminated_tx, _terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || Watcher {
                terminated: terminated_tx,
                custom_observed: None,
            }),
        )
        .unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();

    watcher
        .tell(WatcherMsg::WatchWithStopped {
            subject: subject.clone(),
            observed: observed_tx,
        })
        .unwrap();

    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject.path().clone()
    );
}
