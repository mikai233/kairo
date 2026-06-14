use super::*;

pub(super) enum SupervisionMsg {
    Increment,
    Fail,
    Panic,
    Get(mpsc::Sender<usize>),
}

pub(super) struct SupervisionProbe {
    pub(super) value: usize,
    pub(super) restarted: Option<mpsc::Sender<()>>,
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

enum RestartStartupProbeMsg {
    Fail,
    GetStarts(mpsc::Sender<u64>),
}

struct RestartStartupProbe {
    starts: Arc<AtomicU64>,
    fail_restarted_start_until: u64,
}

impl Actor for RestartStartupProbe {
    type Msg = RestartStartupProbeMsg;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        let start = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
        if start > 1 && start <= self.fail_restarted_start_until {
            Err(ActorError::Message(format!("restart startup boom {start}")))
        } else {
            Ok(())
        }
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RestartStartupProbeMsg::Fail => Err(ActorError::Message("boom".to_string())),
            RestartStartupProbeMsg::GetStarts(reply_to) => reply_to
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
fn bounded_restart_supervision_retries_restarted_startup_failure() {
    let system = ActorSystem::builder("test").build().unwrap();
    let starts = Arc::new(AtomicU64::new(0));
    let actor = system
        .spawn(
            "restart-startup-probe",
            Props::restartable({
                let starts = Arc::clone(&starts);
                move || RestartStartupProbe {
                    starts: Arc::clone(&starts),
                    fail_restarted_start_until: 2,
                }
            })
            .with_supervisor(SupervisorStrategy::restart_with_limit(
                3,
                Duration::from_secs(60),
            )),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor.tell(RestartStartupProbeMsg::Fail).unwrap();
    actor
        .tell(RestartStartupProbeMsg::GetStarts(reply_tx))
        .unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 3);
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
    let (resolved_reply_tx, resolved_reply_rx) = mpsc::channel();

    actor.tell(SupervisionMsg::Increment).unwrap();
    actor.tell(SupervisionMsg::Fail).unwrap();
    restarted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    actor.tell(SupervisionMsg::Get(reply_tx)).unwrap();
    let resolved = system
        .resolve_local::<SupervisionMsg>(path.as_str())
        .unwrap();
    resolved
        .tell(SupervisionMsg::Get(resolved_reply_tx))
        .unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 0);
    assert_eq!(
        resolved_reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        0
    );
    assert_eq!(actor.path(), &path);
    assert_eq!(resolved.path(), &path);
    assert!(!actor.is_stopped());
}

enum RestartCleanupMsg {
    Fail,
    Ping(mpsc::Sender<()>),
}

struct RestartCleanupProbe {
    stopped: mpsc::Sender<&'static str>,
}

impl Actor for RestartCleanupProbe {
    type Msg = RestartCleanupMsg;

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.stopped
            .send("stopped")
            .map_err(|error| ActorError::Message(error.to_string()))
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RestartCleanupMsg::Fail => Err(ActorError::Message("boom".to_string())),
            RestartCleanupMsg::Ping(reply_to) => reply_to
                .send(())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }
}

#[test]
fn default_pre_restart_invokes_stopped_cleanup_hook() {
    let system = ActorSystem::builder("restart-default-cleanup")
        .build()
        .unwrap();
    let (stopped_tx, stopped_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "supervised",
            Props::restartable(move || RestartCleanupProbe {
                stopped: stopped_tx.clone(),
            }),
        )
        .unwrap();
    let (ping_tx, ping_rx) = mpsc::channel();

    actor.tell(RestartCleanupMsg::Fail).unwrap();
    assert_eq!(
        stopped_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "stopped"
    );

    actor.tell(RestartCleanupMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(!actor.is_stopped());
    system.terminate(Duration::from_secs(1)).unwrap();
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

#[derive(Debug, PartialEq, Eq)]
enum RestartOrderingEvent {
    PreRestartChildCount(usize),
    ChildStopped,
}

struct RestartOrderingChild {
    events: mpsc::Sender<RestartOrderingEvent>,
}

impl Actor for RestartOrderingChild {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.events
            .send(RestartOrderingEvent::ChildStopped)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

enum RestartOrderingMsg {
    SpawnChild(mpsc::Sender<()>),
    Fail,
    Ping(mpsc::Sender<()>),
}

struct RestartOrderingParent {
    events: mpsc::Sender<RestartOrderingEvent>,
}

impl Actor for RestartOrderingParent {
    type Msg = RestartOrderingMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RestartOrderingMsg::SpawnChild(reply_to) => {
                let events = self.events.clone();
                ctx.spawn(
                    "child",
                    Props::new(move || RestartOrderingChild {
                        events: events.clone(),
                    }),
                )?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            RestartOrderingMsg::Fail => Err(ActorError::Message("boom".to_string())),
            RestartOrderingMsg::Ping(reply_to) => reply_to
                .send(())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }

    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::PreRestart => self
                .events
                .send(RestartOrderingEvent::PreRestartChildCount(
                    ctx.children().len(),
                ))
                .map_err(|error| ActorError::Message(error.to_string())),
            Signal::PostStop => self.stopped(ctx),
            Signal::Terminated(_) | Signal::ChildFailed { .. } => Ok(()),
        }
    }
}

#[test]
fn restart_supervision_sends_pre_restart_before_stopping_children() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (events_tx, events_rx) = mpsc::channel();
    let parent = system
        .spawn(
            "parent",
            Props::restartable(move || RestartOrderingParent {
                events: events_tx.clone(),
            }),
        )
        .unwrap();
    let (spawned_tx, spawned_rx) = mpsc::channel();

    parent
        .tell(RestartOrderingMsg::SpawnChild(spawned_tx))
        .unwrap();
    spawned_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent.tell(RestartOrderingMsg::Fail).unwrap();
    let (ping_tx, ping_rx) = mpsc::channel();
    parent.tell(RestartOrderingMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RestartOrderingEvent::PreRestartChildCount(1)
    );
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RestartOrderingEvent::ChildStopped
    );
}

#[derive(Debug, PartialEq, Eq)]
enum RestartWatchEvent {
    ChildStopped,
    WatchDelivered,
}

struct RestartWatchedChild {
    events: mpsc::Sender<RestartWatchEvent>,
}

impl Actor for RestartWatchedChild {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.events
            .send(RestartWatchEvent::ChildStopped)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

enum RestartWatchMsg {
    SpawnAndWatchChild(mpsc::Sender<()>),
    Fail,
    ChildTerminated,
    Ping(mpsc::Sender<()>),
}

struct RestartWatchParent {
    events: mpsc::Sender<RestartWatchEvent>,
}

impl Actor for RestartWatchParent {
    type Msg = RestartWatchMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RestartWatchMsg::SpawnAndWatchChild(reply_to) => {
                let events = self.events.clone();
                let child = ctx.spawn(
                    "child",
                    Props::new(move || RestartWatchedChild {
                        events: events.clone(),
                    }),
                )?;
                ctx.watch_with(&child, RestartWatchMsg::ChildTerminated)?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            RestartWatchMsg::Fail => Err(ActorError::Message("boom".to_string())),
            RestartWatchMsg::ChildTerminated => self
                .events
                .send(RestartWatchEvent::WatchDelivered)
                .map_err(|error| ActorError::Message(error.to_string())),
            RestartWatchMsg::Ping(reply_to) => reply_to
                .send(())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }
}

#[test]
fn restart_supervision_unwatches_children_before_restart_stop() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (events_tx, events_rx) = mpsc::channel();
    let parent = system
        .spawn(
            "parent",
            Props::restartable(move || RestartWatchParent {
                events: events_tx.clone(),
            }),
        )
        .unwrap();
    let (spawned_tx, spawned_rx) = mpsc::channel();

    parent
        .tell(RestartWatchMsg::SpawnAndWatchChild(spawned_tx))
        .unwrap();
    spawned_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent.tell(RestartWatchMsg::Fail).unwrap();
    let (ping_tx, ping_rx) = mpsc::channel();
    parent.tell(RestartWatchMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (drain_tx, drain_rx) = mpsc::channel();
    parent.tell(RestartWatchMsg::Ping(drain_tx)).unwrap();
    drain_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RestartWatchEvent::ChildStopped
    );
    assert_eq!(events_rx.try_recv().unwrap_err(), mpsc::TryRecvError::Empty);
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
