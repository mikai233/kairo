use super::supervision::{SupervisionMsg, SupervisionProbe};
use super::*;

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
    StopThenWatch {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<Result<(), ActorError>>,
    },
    StopThenWatchWith {
        subject: ActorRef<()>,
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
    WatchWithTwice {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<Result<(), ActorError>>,
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
    WatchThenUnwatchThenWatchWith {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<Result<(), ActorError>>,
        observed: mpsc::Sender<ActorPath>,
    },
    WatchStoppedThenUnwatch {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<()>,
    },
    WatchWithThenUnwatchThenWatch {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<Result<(), ActorError>>,
        observed: mpsc::Sender<ActorPath>,
    },
    WatchWithStoppedThenUnwatch {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<()>,
        observed: mpsc::Sender<ActorPath>,
    },
    Observed(ActorPath),
    Block {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    },
    Unwatch {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<()>,
    },
    UnwatchOnly {
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
            WatchProbeMsg::StopThenWatch { subject, reply_to } => {
                ctx.stop(ctx.myself())?;
                reply_to
                    .send(ctx.watch(&subject))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatchProbeMsg::StopThenWatchWith { subject, reply_to } => {
                ctx.stop(ctx.myself())?;
                reply_to
                    .send(ctx.watch_with(&subject, WatchProbeMsg::Observed(subject.path().clone())))
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
            WatchProbeMsg::WatchWithTwice {
                subject,
                reply_to,
                observed,
            } => {
                let result = (|| {
                    let path = subject.path().clone();
                    self.custom = Some(observed);
                    ctx.watch_with(&subject, WatchProbeMsg::Observed(path.clone()))?;
                    ctx.watch_with(&subject, WatchProbeMsg::Observed(path))
                })();
                reply_to
                    .send(result)
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
            WatchProbeMsg::WatchThenUnwatchThenWatchWith {
                subject,
                reply_to,
                observed,
            } => {
                let result = (|| {
                    ctx.watch(&subject)?;
                    ctx.unwatch(&subject);
                    let path = subject.path().clone();
                    self.custom = Some(observed);
                    ctx.watch_with(&subject, WatchProbeMsg::Observed(path))
                })();
                reply_to
                    .send(result)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatchProbeMsg::WatchStoppedThenUnwatch { subject, reply_to } => {
                ctx.watch(&subject)?;
                ctx.unwatch(&subject);
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatchProbeMsg::WatchWithThenUnwatchThenWatch {
                subject,
                reply_to,
                observed,
            } => {
                let result = (|| {
                    self.custom = Some(observed);
                    ctx.watch_with(&subject, WatchProbeMsg::Observed(subject.path().clone()))?;
                    ctx.unwatch(&subject);
                    ctx.watch(&subject)
                })();
                reply_to
                    .send(result)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatchProbeMsg::WatchWithStoppedThenUnwatch {
                subject,
                reply_to,
                observed,
            } => {
                self.custom = Some(observed);
                ctx.watch_with(&subject, WatchProbeMsg::Observed(subject.path().clone()))?;
                ctx.unwatch(&subject);
                reply_to
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
            WatchProbeMsg::Block { entered, release } => {
                entered
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                release
                    .recv()
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatchProbeMsg::Unwatch { subject, reply_to } => {
                ctx.watch(&subject)?;
                ctx.unwatch(&subject);
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            WatchProbeMsg::UnwatchOnly { subject, reply_to } => {
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

enum DeathPactProbeMsg {
    Watch {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<()>,
    },
    Ping(mpsc::Sender<()>),
}

struct DeathPactProbe;

impl Actor for DeathPactProbe {
    type Msg = DeathPactProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            DeathPactProbeMsg::Watch { subject, reply_to } => {
                ctx.watch(&subject)?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            DeathPactProbeMsg::Ping(reply_to) => reply_to
                .send(())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }
}

enum ChildDeathPactProbeMsg {
    FailChild,
    Ping(mpsc::Sender<()>),
}

struct ChildDeathPactProbe {
    child: Option<ActorRef<SupervisionMsg>>,
}

impl Actor for ChildDeathPactProbe {
    type Msg = ChildDeathPactProbeMsg;

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
            ChildDeathPactProbeMsg::FailChild => {
                if let Some(child) = &self.child {
                    child
                        .tell(SupervisionMsg::Fail)
                        .map_err(|error| ActorError::Message(error.reason().to_string()))?;
                }
                Ok(())
            }
            ChildDeathPactProbeMsg::Ping(reply_to) => reply_to
                .send(())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }
}

enum StoppingWatchWithMsg {
    Start(Box<StoppingWatchWithStart>),
    StartWithWatchedChild(Box<StoppingWatchedChildStart>),
    StartWithTwoWatchedChildren(Box<StoppingTwoWatchedChildrenStart>),
    SubjectTerminated,
}

struct StoppingWatchWithStart {
    subject: ActorRef<()>,
    entered_child_stop: mpsc::Sender<()>,
    release_child_stop: mpsc::Receiver<()>,
    reply_to: mpsc::Sender<()>,
}

struct StoppingWatchedChildStart {
    entered_child_stop: mpsc::Sender<()>,
    release_child_stop: mpsc::Receiver<()>,
    reply_to: mpsc::Sender<()>,
}

struct StoppingTwoWatchedChildrenStart {
    entered_first_child_stop: mpsc::Sender<()>,
    release_first_child_stop: mpsc::Receiver<()>,
    entered_second_child_stop: mpsc::Sender<()>,
    release_second_child_stop: mpsc::Receiver<()>,
    reply_to: mpsc::Sender<()>,
}

struct BlockingStopChild {
    entered_stop: mpsc::Sender<()>,
    release_stop: Option<mpsc::Receiver<()>>,
}

impl Actor for BlockingStopChild {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.entered_stop
            .send(())
            .map_err(|error| ActorError::Message(error.to_string()))?;
        if let Some(release_stop) = self.release_stop.take() {
            release_stop
                .recv()
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }
}

struct StoppingWatchWithProbe;

impl Actor for StoppingWatchWithProbe {
    type Msg = StoppingWatchWithMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            StoppingWatchWithMsg::Start(start) => {
                let StoppingWatchWithStart {
                    subject,
                    entered_child_stop,
                    release_child_stop,
                    reply_to,
                } = *start;
                ctx.spawn(
                    "child",
                    Props::new(move || BlockingStopChild {
                        entered_stop: entered_child_stop,
                        release_stop: Some(release_child_stop),
                    }),
                )?;
                ctx.watch_with(&subject, StoppingWatchWithMsg::SubjectTerminated)?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            StoppingWatchWithMsg::StartWithWatchedChild(start) => {
                let StoppingWatchedChildStart {
                    entered_child_stop,
                    release_child_stop,
                    reply_to,
                } = *start;
                let child = ctx.spawn(
                    "child",
                    Props::new(move || BlockingStopChild {
                        entered_stop: entered_child_stop,
                        release_stop: Some(release_child_stop),
                    }),
                )?;
                ctx.watch_with(&child, StoppingWatchWithMsg::SubjectTerminated)?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            StoppingWatchWithMsg::StartWithTwoWatchedChildren(start) => {
                let StoppingTwoWatchedChildrenStart {
                    entered_first_child_stop,
                    release_first_child_stop,
                    entered_second_child_stop,
                    release_second_child_stop,
                    reply_to,
                } = *start;
                let first_child = ctx.spawn(
                    "child-a",
                    Props::new(move || BlockingStopChild {
                        entered_stop: entered_first_child_stop,
                        release_stop: Some(release_first_child_stop),
                    }),
                )?;
                let second_child = ctx.spawn(
                    "child-b",
                    Props::new(move || BlockingStopChild {
                        entered_stop: entered_second_child_stop,
                        release_stop: Some(release_second_child_stop),
                    }),
                )?;
                ctx.watch_with(&first_child, StoppingWatchWithMsg::SubjectTerminated)?;
                ctx.watch_with(&second_child, StoppingWatchWithMsg::SubjectTerminated)?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            StoppingWatchWithMsg::SubjectTerminated => Ok(()),
        }
    }
}

enum RestartingWatchMsg {
    WatchWith {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<()>,
    },
    Fail,
    SubjectTerminated(ActorPath),
}

struct RestartingWatchProbe {
    observed: mpsc::Sender<ActorPath>,
    pre_restart: mpsc::Sender<()>,
}

impl Actor for RestartingWatchProbe {
    type Msg = RestartingWatchMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RestartingWatchMsg::WatchWith { subject, reply_to } => {
                let subject_path = subject.path().clone();
                ctx.watch_with(
                    &subject,
                    RestartingWatchMsg::SubjectTerminated(subject_path),
                )?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            RestartingWatchMsg::Fail => Err(ActorError::Message("boom".to_string())),
            RestartingWatchMsg::SubjectTerminated(path) => self
                .observed
                .send(path)
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }

    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::PreRestart => self
                .pre_restart
                .send(())
                .map_err(|error| ActorError::Message(error.to_string())),
            Signal::PostStop => self.stopped(ctx),
            Signal::Terminated(_) | Signal::ChildFailed { .. } => Ok(()),
        }
    }
}

enum StashingWatchWithMsg {
    WatchWith {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<()>,
    },
    StartStashing {
        reply_to: mpsc::Sender<()>,
    },
    Open,
    SubjectTerminated(ActorPath),
}

struct StashingWatchWithProbe {
    stashing: bool,
    observed: mpsc::Sender<ActorPath>,
}

impl Actor for StashingWatchWithProbe {
    type Msg = StashingWatchWithMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            StashingWatchWithMsg::WatchWith { subject, reply_to } => {
                let subject_path = subject.path().clone();
                ctx.watch_with(
                    &subject,
                    StashingWatchWithMsg::SubjectTerminated(subject_path),
                )?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            StashingWatchWithMsg::StartStashing { reply_to } => {
                self.stashing = true;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            StashingWatchWithMsg::Open => {
                self.stashing = false;
                ctx.unstash_all()
            }
            StashingWatchWithMsg::SubjectTerminated(path) if self.stashing => {
                ctx.stash(StashingWatchWithMsg::SubjectTerminated(path))
            }
            StashingWatchWithMsg::SubjectTerminated(path) => self
                .observed
                .send(path)
                .map_err(|error| ActorError::Message(error.to_string())),
        }
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
    SpawnStartupFailingChild,
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

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
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
            ParentWatchMsg::SpawnStartupFailingChild => {
                let child =
                    ctx.spawn("startup-failing-child", Props::new(|| StartupFailingChild))?;
                ctx.watch(&child)?;
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

struct StartupFailingChild;

impl Actor for StartupFailingChild {
    type Msg = ();

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        Err(ActorError::Message("startup boom".to_string()))
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }
}

#[test]
fn watch_rejects_registration_after_self_stop_is_requested() {
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
    let (reply_tx, reply_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::StopThenWatch {
            subject,
            reply_to: reply_tx,
        })
        .unwrap();

    assert!(matches!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Err(ActorError::ActorStopping { actor }) if actor == watcher.path().to_string()
    ));
    assert!(watcher.wait_for_stop(Duration::from_secs(1)));
}

#[test]
fn watch_with_rejects_registration_after_self_stop_is_requested() {
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
    let (reply_tx, reply_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::StopThenWatchWith {
            subject,
            reply_to: reply_tx,
        })
        .unwrap();

    assert!(matches!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Err(ActorError::ActorStopping { actor }) if actor == watcher.path().to_string()
    ));
    assert!(watcher.wait_for_stop(Duration::from_secs(1)));
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
fn watch_twice_delivers_one_terminated_signal_when_subject_already_stopped() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    let subject_path = subject.path().clone();
    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

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
            subject,
            reply_to: registered_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject_path
    );
    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn watch_with_twice_delivers_one_custom_message_when_subject_already_stopped() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    let subject_path = subject.path().clone();
    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

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
        .tell(WatchProbeMsg::WatchWithTwice {
            subject,
            reply_to: registered_tx,
            observed: observed_tx,
        })
        .unwrap();
    registered_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();

    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject_path
    );
    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
    assert!(!watcher.is_stopped());
}

#[test]
fn watch_can_requeue_already_stopped_notification_after_delivery() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    let subject_path = subject.path().clone();
    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

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
    let (first_registered_tx, first_registered_rx) = mpsc::channel();
    let (second_registered_tx, second_registered_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchTwice {
            subject: subject.clone(),
            reply_to: first_registered_tx,
        })
        .unwrap();
    first_registered_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject_path
    );

    watcher
        .tell(WatchProbeMsg::WatchTwice {
            subject: subject.clone(),
            reply_to: second_registered_tx,
        })
        .unwrap();
    second_registered_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject.path().clone()
    );
    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
    assert!(!watcher.is_stopped());
}

#[test]
fn watch_with_can_requeue_already_stopped_custom_message_after_delivery() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    let subject_path = subject.path().clone();
    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

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
    let (first_registered_tx, first_registered_rx) = mpsc::channel();
    let (first_observed_tx, first_observed_rx) = mpsc::channel();
    let (second_registered_tx, second_registered_rx) = mpsc::channel();
    let (second_observed_tx, second_observed_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchWith {
            subject: subject.clone(),
            registered: first_registered_tx,
            observed: first_observed_tx,
        })
        .unwrap();
    first_registered_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        first_observed_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        subject_path
    );

    watcher
        .tell(WatchProbeMsg::WatchWith {
            subject: subject.clone(),
            registered: second_registered_tx,
            observed: second_observed_tx,
        })
        .unwrap();
    second_registered_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    assert_eq!(
        second_observed_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        subject.path().clone()
    );
    assert!(
        second_observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
    assert!(!watcher.is_stopped());
}

#[test]
fn queued_already_stopped_watch_then_watch_with_requires_unwatch_first() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

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
fn queued_already_stopped_watch_with_then_watch_requires_unwatch_first() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

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
fn unwatch_discards_queued_terminated_signal_for_already_stopped_subject() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

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
        .tell(WatchProbeMsg::WatchStoppedThenUnwatch {
            subject,
            reply_to: registered_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "unwatch must remove the queued already-dead Terminated signal before delivery"
    );
    assert!(!watcher.is_stopped());
}

#[test]
fn unwatch_discards_queued_watch_with_message_for_already_stopped_subject() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

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
        .tell(WatchProbeMsg::WatchWithStoppedThenUnwatch {
            subject,
            reply_to: registered_tx,
            observed: observed_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "unwatch must remove the queued already-dead watch_with message before delivery"
    );
    assert!(!watcher.is_stopped());
}

#[test]
fn queued_unwatch_discards_live_terminated_signal_before_delivery() {
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
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (unwatched_tx, unwatched_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchTwice {
            subject: subject.clone(),
            reply_to: registered_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    watcher
        .tell(WatchProbeMsg::Block {
            entered: entered_tx,
            release: release_rx,
        })
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    watcher
        .tell(WatchProbeMsg::UnwatchOnly {
            subject: subject.clone(),
            reply_to: unwatched_tx,
        })
        .unwrap();

    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));
    release_tx.send(()).unwrap();
    unwatched_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "queued unwatch must clear a live-subject Terminated signal before delivery"
    );
    assert!(!watcher.is_stopped());
}

#[test]
fn queued_unwatch_discards_live_watch_with_message_before_delivery() {
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
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (unwatched_tx, unwatched_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchWith {
            subject: subject.clone(),
            registered: registered_tx,
            observed: observed_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    watcher
        .tell(WatchProbeMsg::Block {
            entered: entered_tx,
            release: release_rx,
        })
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    watcher
        .tell(WatchProbeMsg::UnwatchOnly {
            subject: subject.clone(),
            reply_to: unwatched_tx,
        })
        .unwrap();

    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));
    release_tx.send(()).unwrap();
    unwatched_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "queued unwatch must clear a live-subject watch_with message before delivery"
    );
    assert!(!watcher.is_stopped());
}

#[test]
fn watch_path_delivers_terminated_signal_once_when_notified() {
    let system = ActorSystem::builder("test").build().unwrap();
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
    let subject = ActorPath::new("kairo://remote@127.0.0.1:25520/user/target#99");

    system.watch_path(watcher.clone(), subject.clone()).unwrap();
    system.watch_path(watcher, subject.clone()).unwrap();
    system.notify_watched_path_terminated(&subject);
    system.notify_watched_path_terminated(&subject);

    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject
    );
    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn watch_path_delivers_terminated_for_each_subject_on_terminated_address() {
    let system = ActorSystem::builder("test").build().unwrap();
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
    let first = ActorPath::new("kairo://remote@127.0.0.1:25520/user/first#1");
    let second = ActorPath::new("kairo://remote@127.0.0.1:25520/user/second#2");
    let other = ActorPath::new("kairo://other@127.0.0.1:25521/user/other#3");

    system.watch_path(watcher.clone(), first.clone()).unwrap();
    system.watch_path(watcher.clone(), second.clone()).unwrap();
    system.watch_path(watcher, other.clone()).unwrap();
    system.notify_watched_address_terminated("kairo://remote@127.0.0.1:25520");
    system.notify_watched_address_terminated("kairo://remote@127.0.0.1:25520");

    let mut observed = [
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
    ];
    observed.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    assert_eq!(observed, [first, second]);
    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );

    system.notify_watched_path_terminated(&other);
    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        other
    );
}

#[test]
fn unwatch_path_discards_queued_address_terminated_signal_for_one_subject() {
    let system = ActorSystem::builder("test").build().unwrap();
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
    let first = ActorPath::new("kairo://remote@127.0.0.1:25520/user/first#1");
    let second = ActorPath::new("kairo://remote@127.0.0.1:25520/user/second#2");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    system.watch_path(watcher.clone(), first.clone()).unwrap();
    system.watch_path(watcher.clone(), second.clone()).unwrap();
    watcher
        .tell(WatchProbeMsg::Block {
            entered: entered_tx,
            release: release_rx,
        })
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.notify_watched_address_terminated("kairo://remote@127.0.0.1:25520");
    system.unwatch_path(watcher.path(), &first);
    release_tx.send(()).unwrap();

    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        second
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
fn live_watch_with_twice_requires_unwatch_first() {
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
    let (observed_tx, _observed_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchWithTwice {
            subject: subject.clone(),
            reply_to: reply_tx,
            observed: observed_tx,
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
fn watch_then_unwatch_then_watch_with_changes_notification() {
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
    let (reply_tx, reply_rx) = mpsc::channel();
    let (observed_tx, observed_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchThenUnwatchThenWatchWith {
            subject: subject.clone(),
            reply_to: reply_tx,
            observed: observed_tx,
        })
        .unwrap();
    reply_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();

    system.stop(&subject);

    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject.path().clone()
    );
    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn watch_with_then_unwatch_then_watch_changes_notification() {
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
    let (reply_tx, reply_rx) = mpsc::channel();
    let (observed_tx, observed_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchWithThenUnwatchThenWatch {
            subject: subject.clone(),
            reply_to: reply_tx,
            observed: observed_tx,
        })
        .unwrap();
    reply_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();

    system.stop(&subject);

    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject.path().clone()
    );
    assert!(
        observed_rx
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
fn watch_with_custom_termination_message_can_be_stashed_and_unstashed() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || StashingWatchWithProbe {
                stashing: false,
                observed: observed_tx,
            })
            .with_stash_capacity(1),
        )
        .unwrap();
    let (registered_tx, registered_rx) = mpsc::channel();
    let (stashing_tx, stashing_rx) = mpsc::channel();

    watcher
        .tell(StashingWatchWithMsg::WatchWith {
            subject: subject.clone(),
            reply_to: registered_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    watcher
        .tell(StashingWatchWithMsg::StartStashing {
            reply_to: stashing_tx,
        })
        .unwrap();
    stashing_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));
    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "custom watch_with termination message should stay stashed while the actor is closed"
    );

    watcher.tell(StashingWatchWithMsg::Open).unwrap();

    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        subject.path().clone()
    );
}

#[test]
fn watch_with_survives_unrelated_actor_restart() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (pre_restart_tx, pre_restart_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::restartable(move || RestartingWatchProbe {
                observed: observed_tx.clone(),
                pre_restart: pre_restart_tx.clone(),
            }),
        )
        .unwrap();
    let (registered_tx, registered_rx) = mpsc::channel();

    watcher
        .tell(RestartingWatchMsg::WatchWith {
            subject: subject.clone(),
            reply_to: registered_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    watcher.tell(RestartingWatchMsg::Fail).unwrap();
    pre_restart_rx.recv_timeout(Duration::from_secs(1)).unwrap();

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
fn stopped_watcher_is_removed_from_subject_watchers() {
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

    system.stop(&watcher);
    assert!(watcher.wait_for_stop(Duration::from_secs(1)));
    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
    assert!(
        !system
            .dead_letters()
            .wait_for_len(1, Duration::from_millis(100)),
        "stopped watcher should have been removed before subject termination"
    );
    assert!(system.dead_letters().is_empty());
}

#[test]
fn stopping_watcher_discards_queued_terminated_signal() {
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
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    watcher
        .tell(WatchProbeMsg::WatchTwice {
            subject: subject.clone(),
            reply_to: registered_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    watcher
        .tell(WatchProbeMsg::Block {
            entered: entered_tx,
            release: release_rx,
        })
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));
    system.stop(&watcher);
    release_tx.send(()).unwrap();
    assert!(watcher.wait_for_stop(Duration::from_secs(1)));

    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
    assert!(
        !system
            .dead_letters()
            .wait_for_len(1, Duration::from_millis(100)),
        "queued Terminated should be discarded when the watcher stops before delivery"
    );
    assert!(system.dead_letters().is_empty());
}

#[test]
fn default_terminated_signal_triggers_death_pact_stop() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    let watcher = system
        .spawn("watcher", Props::new(|| DeathPactProbe))
        .unwrap();
    let (registered_tx, registered_rx) = mpsc::channel();
    let (ping_tx, ping_rx) = mpsc::channel();

    watcher
        .tell(DeathPactProbeMsg::Watch {
            subject: subject.clone(),
            reply_to: registered_tx,
        })
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));
    assert!(
        watcher.wait_for_stop(Duration::from_secs(1)),
        "unhandled Terminated should stop the watching actor"
    );

    let error = watcher
        .tell(DeathPactProbeMsg::Ping(ping_tx))
        .expect_err("death-pact-stopped watcher should reject later messages");
    assert_eq!(error.reason(), "actor is stopped");
    assert!(ping_rx.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn default_child_failed_signal_triggers_death_pact_stop() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn("parent", Props::new(|| ChildDeathPactProbe { child: None }))
        .unwrap();
    let (ping_tx, ping_rx) = mpsc::channel();

    parent.tell(ChildDeathPactProbeMsg::FailChild).unwrap();
    assert!(
        parent.wait_for_stop(Duration::from_secs(1)),
        "unhandled ChildFailed should stop the watching parent"
    );

    let error = parent
        .tell(ChildDeathPactProbeMsg::Ping(ping_tx))
        .expect_err("death-pact-stopped parent should reject later messages");
    assert_eq!(error.reason(), "actor is stopped");
    assert!(ping_rx.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn stopping_watcher_is_removed_before_waiting_for_children() {
    let system = ActorSystem::builder("test").build().unwrap();
    let subject = system.spawn("subject", Props::new(|| Noop)).unwrap();
    let watcher = system
        .spawn("watcher", Props::new(|| StoppingWatchWithProbe))
        .unwrap();
    let (entered_child_stop_tx, entered_child_stop_rx) = mpsc::channel();
    let (release_child_stop_tx, release_child_stop_rx) = mpsc::channel();
    let (registered_tx, registered_rx) = mpsc::channel();

    watcher
        .tell(StoppingWatchWithMsg::Start(Box::new(
            StoppingWatchWithStart {
                subject: subject.clone(),
                entered_child_stop: entered_child_stop_tx,
                release_child_stop: release_child_stop_rx,
                reply_to: registered_tx,
            },
        )))
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&watcher);
    entered_child_stop_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    system.stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

    assert!(
        !system
            .dead_letters()
            .wait_for_len(1, Duration::from_millis(100)),
        "stopping watcher should unwatch subjects before child shutdown can block"
    );
    assert!(system.dead_letters().is_empty());

    release_child_stop_tx.send(()).unwrap();
    assert!(watcher.wait_for_stop(Duration::from_secs(1)));
}

#[test]
fn stopping_parent_unwatches_child_before_stopping_it() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn("parent", Props::new(|| StoppingWatchWithProbe))
        .unwrap();
    let (entered_child_stop_tx, entered_child_stop_rx) = mpsc::channel();
    let (release_child_stop_tx, release_child_stop_rx) = mpsc::channel();
    let (registered_tx, registered_rx) = mpsc::channel();

    parent
        .tell(StoppingWatchWithMsg::StartWithWatchedChild(Box::new(
            StoppingWatchedChildStart {
                entered_child_stop: entered_child_stop_tx,
                release_child_stop: release_child_stop_rx,
                reply_to: registered_tx,
            },
        )))
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&parent);
    entered_child_stop_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(
        !system
            .dead_letters()
            .wait_for_len(1, Duration::from_millis(100)),
        "parent stop should remove child watch before the child termination can enqueue a stale custom message"
    );
    assert!(system.dead_letters().is_empty());

    release_child_stop_tx.send(()).unwrap();
    assert!(parent.wait_for_stop(Duration::from_secs(1)));
}

#[test]
fn stopping_parent_unwatches_all_children_before_stopping_them() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn("parent", Props::new(|| StoppingWatchWithProbe))
        .unwrap();
    let (entered_first_tx, entered_first_rx) = mpsc::channel();
    let (release_first_tx, release_first_rx) = mpsc::channel();
    let (entered_second_tx, entered_second_rx) = mpsc::channel();
    let (release_second_tx, release_second_rx) = mpsc::channel();
    let (registered_tx, registered_rx) = mpsc::channel();

    parent
        .tell(StoppingWatchWithMsg::StartWithTwoWatchedChildren(Box::new(
            StoppingTwoWatchedChildrenStart {
                entered_first_child_stop: entered_first_tx,
                release_first_child_stop: release_first_rx,
                entered_second_child_stop: entered_second_tx,
                release_second_child_stop: release_second_rx,
                reply_to: registered_tx,
            },
        )))
        .unwrap();
    registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&parent);
    entered_first_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    entered_second_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(
        !system
            .dead_letters()
            .wait_for_len(1, Duration::from_millis(100)),
        "parent stop should remove all child watches before child termination can enqueue stale custom messages"
    );

    release_first_tx.send(()).unwrap();
    release_second_tx.send(()).unwrap();
    assert!(parent.wait_for_stop(Duration::from_secs(1)));
    assert!(
        !system
            .dead_letters()
            .wait_for_len(1, Duration::from_millis(100)),
        "released children should not enqueue stale custom watch messages after parent termination"
    );
    assert!(system.dead_letters().is_empty());
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
    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "parent watcher should not receive a duplicate plain Terminated after ChildFailed"
    );
}

#[test]
fn parent_watch_receives_child_failed_when_child_fails_startup() {
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

    parent
        .tell(ParentWatchMsg::SpawnStartupFailingChild)
        .unwrap();

    let observed = observed_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let ParentWatchSignal::ChildFailed { path, reason } = observed else {
        panic!("expected child failure signal");
    };
    assert_eq!(path.name(), Some("startup-failing-child"));
    assert_eq!(reason, "startup boom");
    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "parent watcher should not receive a duplicate plain Terminated after startup ChildFailed"
    );
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
