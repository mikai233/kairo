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
    WatchThenUnwatchThenWatchWith {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<Result<(), ActorError>>,
        observed: mpsc::Sender<ActorPath>,
    },
    WatchWithThenUnwatchThenWatch {
        subject: ActorRef<()>,
        reply_to: mpsc::Sender<Result<(), ActorError>>,
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

enum StoppingWatchWithMsg {
    Start(Box<StoppingWatchWithStart>),
    SubjectTerminated,
}

struct StoppingWatchWithStart {
    subject: ActorRef<()>,
    entered_child_stop: mpsc::Sender<()>,
    release_child_stop: mpsc::Receiver<()>,
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
