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

struct NamedStopProbe {
    name: &'static str,
    stopped: mpsc::Sender<&'static str>,
}

impl Actor for NamedStopProbe {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.stopped
            .send(self.name)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
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

#[derive(Debug)]
struct PreRestartHelperResults {
    named_child_spawn: Result<(), String>,
    anonymous_child_spawn: Result<(), String>,
    spawn_task: Result<(), String>,
    pipe_to_self: Result<(), String>,
    adapter: Result<(), String>,
    ask: Result<(), String>,
    watch: Result<(), String>,
    watch_with: Result<(), String>,
    stash: Result<(), String>,
    unstash_all: Result<(), String>,
    schedule_once_self_cancelled: bool,
    single_timer_active: bool,
    fixed_delay_timer_active: bool,
    fixed_rate_timer_active: bool,
    receive_timeout: Option<Duration>,
}

enum PreRestartAskTargetMsg {
    Request { _reply_to: ActorRef<()> },
}

struct PreRestartAskTarget;

impl Actor for PreRestartAskTarget {
    type Msg = PreRestartAskTargetMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }
}

#[derive(Clone)]
enum PreRestartHelperMsg {
    Fail,
    Noop,
    Get(mpsc::Sender<&'static str>),
}

struct PreRestartHelperProbe {
    results: mpsc::Sender<PreRestartHelperResults>,
    ask_target: ActorRef<PreRestartAskTargetMsg>,
}

impl Actor for PreRestartHelperProbe {
    type Msg = PreRestartHelperMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            PreRestartHelperMsg::Fail => Err(ActorError::Message("boom".to_string())),
            PreRestartHelperMsg::Noop => Ok(()),
            PreRestartHelperMsg::Get(reply_to) => reply_to
                .send("live")
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }

    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::PreRestart => {
                let named_child_spawn = ctx
                    .spawn("late-child", Props::new(|| Noop))
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                let anonymous_child_spawn = ctx
                    .spawn_anonymous(Props::new(|| Noop))
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                let spawn_task = ctx
                    .spawn_task(|_| {})
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                let pipe_to_self = ctx
                    .pipe_to_self(|| Ok::<(), ()>(()), |_| PreRestartHelperMsg::Noop)
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                let adapter = ctx
                    .message_adapter(|_: u8| PreRestartHelperMsg::Noop)
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                let ask = ctx
                    .ask(
                        self.ask_target.clone(),
                        Duration::from_secs(1),
                        |reply_to| PreRestartAskTargetMsg::Request {
                            _reply_to: reply_to,
                        },
                        |_| PreRestartHelperMsg::Noop,
                    )
                    .map_err(|error| error.to_string());
                let watch = ctx
                    .watch(&self.ask_target)
                    .map_err(|error| error.to_string());
                let watch_with = ctx
                    .watch_with(&self.ask_target, PreRestartHelperMsg::Noop)
                    .map_err(|error| error.to_string());
                let stash = ctx
                    .stash(PreRestartHelperMsg::Noop)
                    .map_err(|error| error.to_string());
                let unstash_all = ctx.unstash_all().map_err(|error| error.to_string());
                let schedule_once_self_cancelled = ctx
                    .schedule_once_self(Duration::from_secs(1), PreRestartHelperMsg::Noop)
                    .is_cancelled();
                ctx.start_single_timer(
                    "late-single",
                    Duration::from_secs(1),
                    PreRestartHelperMsg::Noop,
                );
                let single_timer_active = ctx.is_timer_active("late-single");
                ctx.start_timer_with_fixed_delay(
                    "late-fixed-delay",
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                    PreRestartHelperMsg::Noop,
                );
                let fixed_delay_timer_active = ctx.is_timer_active("late-fixed-delay");
                ctx.start_timer_at_fixed_rate(
                    "late-fixed-rate",
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                    PreRestartHelperMsg::Noop,
                );
                let fixed_rate_timer_active = ctx.is_timer_active("late-fixed-rate");
                ctx.set_receive_timeout(Duration::from_secs(1), PreRestartHelperMsg::Noop);
                let receive_timeout = ctx.receive_timeout();
                self.results
                    .send(PreRestartHelperResults {
                        named_child_spawn,
                        anonymous_child_spawn,
                        spawn_task,
                        pipe_to_self,
                        adapter,
                        ask,
                        watch,
                        watch_with,
                        stash,
                        unstash_all,
                        schedule_once_self_cancelled,
                        single_timer_active,
                        fixed_delay_timer_active,
                        fixed_rate_timer_active,
                        receive_timeout,
                    })
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            Signal::PostStop => self.stopped(ctx),
            Signal::Terminated(_) | Signal::ChildFailed { .. } => Ok(()),
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

enum FailedRestartChildMsg {
    Fail,
    ChildCount(mpsc::Sender<usize>),
}

struct FailedRestartChildCleanupProbe {
    starts: Arc<AtomicU64>,
    survivor_stopped: mpsc::Sender<&'static str>,
    failed_attempt_stopped: mpsc::Sender<&'static str>,
}

impl Actor for FailedRestartChildCleanupProbe {
    type Msg = FailedRestartChildMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let start = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
        if start == 1 {
            let survivor_stopped = self.survivor_stopped.clone();
            ctx.spawn(
                "survivor",
                Props::new(move || NamedStopProbe {
                    name: "survivor",
                    stopped: survivor_stopped,
                }),
            )?;
            return Ok(());
        }

        if start == 2 {
            let failed_attempt_stopped = self.failed_attempt_stopped.clone();
            ctx.spawn(
                "failed-attempt",
                Props::new(move || NamedStopProbe {
                    name: "failed-attempt",
                    stopped: failed_attempt_stopped,
                }),
            )?;
            return Err(ActorError::Message(
                "replacement startup failed".to_string(),
            ));
        }

        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            FailedRestartChildMsg::Fail => Err(ActorError::Message("boom".to_string())),
            FailedRestartChildMsg::ChildCount(reply_to) => reply_to
                .send(ctx.children().len())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }
}

struct StartupChildFailureProbe {
    starts: Arc<AtomicU64>,
    child_stopped: mpsc::Sender<()>,
    restarted_child_count: mpsc::Sender<usize>,
}

impl Actor for StartupChildFailureProbe {
    type Msg = ();

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let start = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
        if start == 1 {
            let child_stopped = self.child_stopped.clone();
            ctx.spawn(
                "startup-child",
                Props::new(move || StopProbe {
                    stopped: child_stopped,
                }),
            )?;
            return Err(ActorError::Message("startup failed".to_string()));
        }

        self.restarted_child_count
            .send(ctx.children().len())
            .map_err(|error| ActorError::Message(error.to_string()))
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
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
fn preserving_startup_restart_stops_children_from_failed_start() {
    let system = ActorSystem::builder("test").build().unwrap();
    let starts = Arc::new(AtomicU64::new(0));
    let (child_stopped_tx, child_stopped_rx) = mpsc::channel();
    let (child_count_tx, child_count_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "startup-child-failure",
            Props::restartable({
                let starts = Arc::clone(&starts);
                move || StartupChildFailureProbe {
                    starts: Arc::clone(&starts),
                    child_stopped: child_stopped_tx.clone(),
                    restarted_child_count: child_count_tx.clone(),
                }
            })
            .with_supervisor(
                SupervisorStrategy::restart_with_limit_preserving_children(
                    2,
                    Duration::from_secs(60),
                ),
            ),
        )
        .unwrap();

    child_stopped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        child_count_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        0
    );
    assert_eq!(starts.load(Ordering::SeqCst), 2);
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
fn preserving_restart_stops_children_from_failed_replacement_start() {
    let system = ActorSystem::builder("test").build().unwrap();
    let starts = Arc::new(AtomicU64::new(0));
    let (survivor_stopped_tx, survivor_stopped_rx) = mpsc::channel();
    let (failed_attempt_stopped_tx, failed_attempt_stopped_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "restart-startup-child-cleanup",
            Props::restartable({
                let starts = Arc::clone(&starts);
                move || FailedRestartChildCleanupProbe {
                    starts: Arc::clone(&starts),
                    survivor_stopped: survivor_stopped_tx.clone(),
                    failed_attempt_stopped: failed_attempt_stopped_tx.clone(),
                }
            })
            .with_supervisor(
                SupervisorStrategy::restart_with_limit_preserving_children(
                    3,
                    Duration::from_secs(60),
                ),
            ),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor.tell(FailedRestartChildMsg::Fail).unwrap();
    actor
        .tell(FailedRestartChildMsg::ChildCount(reply_tx))
        .unwrap();

    let child_count = reply_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 3);
    assert_eq!(
        failed_attempt_stopped_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        "failed-attempt"
    );
    assert!(survivor_stopped_rx.try_recv().is_err());
    assert_eq!(child_count, 1);
    assert!(!actor.is_stopped());
}

#[test]
fn unbounded_preserving_restart_stops_all_children_when_replacement_start_fails() {
    let system = ActorSystem::builder("test").build().unwrap();
    let starts = Arc::new(AtomicU64::new(0));
    let (survivor_stopped_tx, survivor_stopped_rx) = mpsc::channel();
    let (failed_attempt_stopped_tx, failed_attempt_stopped_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "unbounded-restart-startup-child-cleanup",
            Props::restartable({
                let starts = Arc::clone(&starts);
                move || FailedRestartChildCleanupProbe {
                    starts: Arc::clone(&starts),
                    survivor_stopped: survivor_stopped_tx.clone(),
                    failed_attempt_stopped: failed_attempt_stopped_tx.clone(),
                }
            })
            .with_supervisor(SupervisorStrategy::restart_preserving_children()),
        )
        .unwrap();

    actor.tell(FailedRestartChildMsg::Fail).unwrap();

    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(
        failed_attempt_stopped_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        "failed-attempt"
    );
    assert_eq!(
        survivor_stopped_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        "survivor"
    );
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
fn resume_supervision_keeps_actor_state_after_receive_panic() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (pre_restart_tx, pre_restart_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "supervised",
            Props::new(move || SupervisionProbe {
                value: 0,
                restarted: Some(pre_restart_tx.clone()),
            })
            .with_supervisor(SupervisorStrategy::Resume),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor.tell(SupervisionMsg::Increment).unwrap();
    actor.tell(SupervisionMsg::Panic).unwrap();
    actor.tell(SupervisionMsg::Get(reply_tx)).unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    assert!(pre_restart_rx.try_recv().is_err());
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
fn pre_restart_rejects_late_helper_creation() {
    let system = ActorSystem::builder("restart-helper-cleanup")
        .build()
        .unwrap();
    let ask_target = system
        .spawn("pre-restart-ask-target", Props::new(|| PreRestartAskTarget))
        .unwrap();
    let (results_tx, results_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "pre-restart-helper",
            Props::restartable(move || PreRestartHelperProbe {
                results: results_tx.clone(),
                ask_target: ask_target.clone(),
            })
            .with_supervisor(SupervisorStrategy::Restart),
        )
        .unwrap();

    actor.tell(PreRestartHelperMsg::Fail).unwrap();

    let results = results_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let expected = format!("actor `{}` is stopping", actor.path());
    assert_eq!(
        results
            .named_child_spawn
            .expect_err("named child spawn should be rejected"),
        expected
    );
    assert_eq!(
        results
            .anonymous_child_spawn
            .expect_err("anonymous child spawn should be rejected"),
        expected
    );
    assert_eq!(
        results
            .spawn_task
            .expect_err("spawn_task should be rejected"),
        expected
    );
    assert_eq!(
        results
            .pipe_to_self
            .expect_err("pipe_to_self should be rejected"),
        expected
    );
    assert_eq!(
        results.adapter.expect_err("adapter should be rejected"),
        expected
    );
    assert_eq!(results.ask.expect_err("ask should be rejected"), expected);
    assert_eq!(
        results.watch.expect_err("watch should be rejected"),
        expected
    );
    assert_eq!(
        results
            .watch_with
            .expect_err("watch_with should be rejected"),
        expected
    );
    assert_eq!(
        results.stash.expect_err("stash should be rejected"),
        expected
    );
    assert_eq!(
        results
            .unstash_all
            .expect_err("unstash_all should be rejected"),
        expected
    );
    assert!(
        results.schedule_once_self_cancelled,
        "late self scheduling should return an already-cancelled handle"
    );
    assert!(
        !results.single_timer_active,
        "late single timers should not become active during PreRestart"
    );
    assert!(
        !results.fixed_delay_timer_active,
        "late fixed-delay timers should not become active during PreRestart"
    );
    assert!(
        !results.fixed_rate_timer_active,
        "late fixed-rate timers should not become active during PreRestart"
    );
    assert_eq!(
        results.receive_timeout, None,
        "late receive timeouts should not be armed during PreRestart"
    );

    let (reply_tx, reply_rx) = mpsc::channel();
    actor.tell(PreRestartHelperMsg::Get(reply_tx)).unwrap();
    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "live"
    );
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
    SpawnCounterChild {
        reply_to: mpsc::Sender<ActorRef<CounterMsg>>,
    },
    SpawnRestartableChild {
        restarted: mpsc::Sender<()>,
        reply_to: mpsc::Sender<ActorRef<SupervisionMsg>>,
    },
    Fail,
    ChildCount(mpsc::Sender<usize>),
    ChildPath(mpsc::Sender<Option<ActorPath>>),
    SpawnDuplicateChild(mpsc::Sender<Result<(), String>>),
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
            RestartParentMsg::SpawnCounterChild { reply_to } => {
                let child = ctx.spawn("counter-child", Props::new(|| Counter { value: 0 }))?;
                reply_to
                    .send(child)
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            RestartParentMsg::SpawnRestartableChild {
                restarted,
                reply_to,
            } => {
                let child = ctx.spawn(
                    "restartable-child",
                    Props::restartable(move || SupervisionProbe {
                        value: 0,
                        restarted: Some(restarted.clone()),
                    }),
                )?;
                reply_to
                    .send(child)
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            RestartParentMsg::Fail => Err(ActorError::Message("boom".to_string())),
            RestartParentMsg::ChildCount(reply_to) => reply_to
                .send(ctx.children().len())
                .map_err(|error| ActorError::Message(error.to_string())),
            RestartParentMsg::ChildPath(reply_to) => reply_to
                .send(ctx.child("child").map(|child| child.path().clone()))
                .map_err(|error| ActorError::Message(error.to_string())),
            RestartParentMsg::SpawnDuplicateChild(reply_to) => {
                let result = ctx
                    .spawn("child", Props::new(|| Noop))
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                reply_to
                    .send(result)
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
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
enum RestartRecreateOrderEvent {
    Built(u64),
    PreRestart,
    ChildStopped,
}

struct RestartRecreateOrderChild {
    events: mpsc::Sender<RestartRecreateOrderEvent>,
}

impl Actor for RestartRecreateOrderChild {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.events
            .send(RestartRecreateOrderEvent::ChildStopped)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

enum RestartRecreateOrderMsg {
    SpawnChild(mpsc::Sender<()>),
    Fail,
}

struct RestartRecreateOrderParent {
    events: mpsc::Sender<RestartRecreateOrderEvent>,
}

impl Actor for RestartRecreateOrderParent {
    type Msg = RestartRecreateOrderMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RestartRecreateOrderMsg::SpawnChild(reply_to) => {
                let events = self.events.clone();
                ctx.spawn(
                    "child",
                    Props::new(move || RestartRecreateOrderChild {
                        events: events.clone(),
                    }),
                )?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            RestartRecreateOrderMsg::Fail => Err(ActorError::Message("boom".to_string())),
        }
    }

    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::PreRestart => self
                .events
                .send(RestartRecreateOrderEvent::PreRestart)
                .map_err(|error| ActorError::Message(error.to_string())),
            Signal::PostStop => self.stopped(ctx),
            Signal::Terminated(_) | Signal::ChildFailed { .. } => Ok(()),
        }
    }
}

#[test]
fn restart_supervision_builds_replacement_after_pre_restart_and_child_stop() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (events_tx, events_rx) = mpsc::channel();
    let builds = Arc::new(AtomicU64::new(0));
    let parent = system
        .spawn(
            "parent",
            Props::restartable({
                let events = events_tx.clone();
                let builds = Arc::clone(&builds);
                move || {
                    let generation = builds.fetch_add(1, Ordering::SeqCst) + 1;
                    events
                        .send(RestartRecreateOrderEvent::Built(generation))
                        .expect("restart recreate order receiver should be open");
                    RestartRecreateOrderParent {
                        events: events.clone(),
                    }
                }
            }),
        )
        .unwrap();
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RestartRecreateOrderEvent::Built(1)
    );
    let (spawned_tx, spawned_rx) = mpsc::channel();

    parent
        .tell(RestartRecreateOrderMsg::SpawnChild(spawned_tx))
        .unwrap();
    spawned_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent.tell(RestartRecreateOrderMsg::Fail).unwrap();

    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RestartRecreateOrderEvent::PreRestart
    );
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RestartRecreateOrderEvent::ChildStopped
    );
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RestartRecreateOrderEvent::Built(2)
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
    SpawnAndWatchChildRef(mpsc::Sender<ActorRef<()>>),
    SpawnAndWatchTwoChildren(mpsc::Sender<()>),
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
            RestartWatchMsg::SpawnAndWatchChildRef(reply_to) => {
                let events = self.events.clone();
                let child = ctx.spawn(
                    "child",
                    Props::new(move || RestartWatchedChild {
                        events: events.clone(),
                    }),
                )?;
                ctx.watch_with(&child, RestartWatchMsg::ChildTerminated)?;
                reply_to
                    .send(child)
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            RestartWatchMsg::SpawnAndWatchTwoChildren(reply_to) => {
                for name in ["child-a", "child-b"] {
                    let events = self.events.clone();
                    let child = ctx.spawn(
                        name,
                        Props::new(move || RestartWatchedChild {
                            events: events.clone(),
                        }),
                    )?;
                    ctx.watch_with(&child, RestartWatchMsg::ChildTerminated)?;
                }
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
fn restart_supervision_waits_for_all_restart_stopped_children_without_watch_messages() {
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
        .tell(RestartWatchMsg::SpawnAndWatchTwoChildren(spawned_tx))
        .unwrap();
    spawned_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent.tell(RestartWatchMsg::Fail).unwrap();
    let (ping_tx, ping_rx) = mpsc::channel();
    parent.tell(RestartWatchMsg::Ping(ping_tx)).unwrap();

    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RestartWatchEvent::ChildStopped
    );
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RestartWatchEvent::ChildStopped
    );
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (drain_tx, drain_rx) = mpsc::channel();
    parent.tell(RestartWatchMsg::Ping(drain_tx)).unwrap();
    drain_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(events_rx.try_recv().unwrap_err(), mpsc::TryRecvError::Empty);
}

#[test]
fn restart_preserving_children_keeps_child_watch_registration() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (events_tx, events_rx) = mpsc::channel();
    let parent = system
        .spawn(
            "parent",
            Props::restartable(move || RestartWatchParent {
                events: events_tx.clone(),
            })
            .with_supervisor(SupervisorStrategy::restart_preserving_children()),
        )
        .unwrap();
    let (spawned_tx, spawned_rx) = mpsc::channel();

    parent
        .tell(RestartWatchMsg::SpawnAndWatchChildRef(spawned_tx))
        .unwrap();
    let child = spawned_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent.tell(RestartWatchMsg::Fail).unwrap();
    let (ping_tx, ping_rx) = mpsc::channel();
    parent.tell(RestartWatchMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&child);
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RestartWatchEvent::ChildStopped
    );
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RestartWatchEvent::WatchDelivered
    );
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

#[test]
fn restart_preserving_children_keeps_non_restartable_child_live() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn(
            "parent",
            Props::restartable(|| RestartParent)
                .with_supervisor(SupervisorStrategy::restart_preserving_children()),
        )
        .unwrap();
    let (child_tx, child_rx) = mpsc::channel();

    parent
        .tell(RestartParentMsg::SpawnCounterChild { reply_to: child_tx })
        .unwrap();
    let child = child_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    child.tell(CounterMsg::Increment).unwrap();

    parent.tell(RestartParentMsg::Fail).unwrap();
    let (count_tx, count_rx) = mpsc::channel();
    parent.tell(RestartParentMsg::ChildCount(count_tx)).unwrap();
    assert_eq!(count_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);

    child.tell(CounterMsg::Increment).unwrap();
    let (value_tx, value_rx) = mpsc::channel();
    child.tell(CounterMsg::Get(value_tx)).unwrap();
    assert_eq!(value_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
}

#[test]
fn restart_preserving_children_keeps_child_lookup_and_name_reserved() {
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

    parent
        .tell(RestartParentMsg::SpawnChild {
            stopped: child_stopped_tx,
            reply_to: spawned_tx,
        })
        .unwrap();
    spawned_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (before_path_tx, before_path_rx) = mpsc::channel();
    parent
        .tell(RestartParentMsg::ChildPath(before_path_tx))
        .unwrap();
    let child_path = before_path_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .expect("child should be visible before restart");

    parent.tell(RestartParentMsg::Fail).unwrap();
    let (after_path_tx, after_path_rx) = mpsc::channel();
    parent
        .tell(RestartParentMsg::ChildPath(after_path_tx))
        .unwrap();
    assert_eq!(
        after_path_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .expect("preserved child should stay visible after parent restart"),
        child_path
    );

    let (duplicate_tx, duplicate_rx) = mpsc::channel();
    parent
        .tell(RestartParentMsg::SpawnDuplicateChild(duplicate_tx))
        .unwrap();
    assert_eq!(
        duplicate_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .expect_err("preserved child should reserve its name"),
        "actor `child` already exists"
    );
    assert!(child_stopped_rx.try_recv().is_err());
}

#[test]
fn restart_preserving_children_restarts_restartable_child_after_parent_failure() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn(
            "parent",
            Props::restartable(|| RestartParent)
                .with_supervisor(SupervisorStrategy::restart_preserving_children()),
        )
        .unwrap();
    let (child_restarted_tx, child_restarted_rx) = mpsc::channel();
    let (child_tx, child_rx) = mpsc::channel();

    parent
        .tell(RestartParentMsg::SpawnRestartableChild {
            restarted: child_restarted_tx,
            reply_to: child_tx,
        })
        .unwrap();
    let child = child_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let child_path = child.path().clone();
    child.tell(SupervisionMsg::Increment).unwrap();
    let (before_tx, before_rx) = mpsc::channel();
    child.tell(SupervisionMsg::Get(before_tx)).unwrap();
    assert_eq!(before_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);

    parent.tell(RestartParentMsg::Fail).unwrap();
    let (count_tx, count_rx) = mpsc::channel();
    parent.tell(RestartParentMsg::ChildCount(count_tx)).unwrap();
    assert_eq!(count_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    child_restarted_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    let (after_tx, after_rx) = mpsc::channel();
    child.tell(SupervisionMsg::Get(after_tx)).unwrap();
    assert_eq!(after_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 0);
    assert_eq!(child.path(), &child_path);
}

#[test]
fn bounded_restart_supervision_can_preserve_children_until_limit_is_exceeded() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn(
            "parent",
            Props::restartable(|| RestartParent).with_supervisor(
                SupervisorStrategy::restart_with_limit_preserving_children(
                    2,
                    Duration::from_secs(10),
                ),
            ),
        )
        .unwrap();
    let (child_stopped_tx, child_stopped_rx) = mpsc::channel();
    let (spawned_tx, spawned_rx) = mpsc::channel();

    parent
        .tell(RestartParentMsg::SpawnChild {
            stopped: child_stopped_tx,
            reply_to: spawned_tx,
        })
        .unwrap();
    spawned_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    for _ in 0..2 {
        let (count_tx, count_rx) = mpsc::channel();
        parent.tell(RestartParentMsg::Fail).unwrap();
        parent.tell(RestartParentMsg::ChildCount(count_tx)).unwrap();
        assert_eq!(count_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
        assert!(child_stopped_rx.try_recv().is_err());
    }

    parent.tell(RestartParentMsg::Fail).unwrap();

    assert!(parent.wait_for_stop(Duration::from_secs(1)));
    child_stopped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
}

#[test]
fn bounded_child_preserving_restart_limit_resets_after_time_window() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn(
            "parent",
            Props::restartable(|| RestartParent).with_supervisor(
                SupervisorStrategy::restart_with_limit_preserving_children(
                    1,
                    Duration::from_millis(25),
                ),
            ),
        )
        .unwrap();
    let (child_stopped_tx, child_stopped_rx) = mpsc::channel();
    let (spawned_tx, spawned_rx) = mpsc::channel();

    parent
        .tell(RestartParentMsg::SpawnChild {
            stopped: child_stopped_tx,
            reply_to: spawned_tx,
        })
        .unwrap();
    spawned_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent.tell(RestartParentMsg::Fail).unwrap();
    let (first_count_tx, first_count_rx) = mpsc::channel();
    parent
        .tell(RestartParentMsg::ChildCount(first_count_tx))
        .unwrap();
    assert_eq!(
        first_count_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        1
    );
    assert!(child_stopped_rx.try_recv().is_err());

    thread::sleep(Duration::from_millis(60));
    parent.tell(RestartParentMsg::Fail).unwrap();
    let (second_count_tx, second_count_rx) = mpsc::channel();
    parent
        .tell(RestartParentMsg::ChildCount(second_count_tx))
        .unwrap();
    assert_eq!(
        second_count_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        1
    );
    assert!(child_stopped_rx.try_recv().is_err());

    parent.tell(RestartParentMsg::Fail).unwrap();

    assert!(parent.wait_for_stop(Duration::from_secs(1)));
    child_stopped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
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
    SpawnSibling {
        stopped: mpsc::Sender<()>,
        reply_to: mpsc::Sender<ActorRef<()>>,
    },
    SpawnBlockingSibling {
        entered_stop: mpsc::Sender<()>,
        release_stop: mpsc::Receiver<()>,
        reply_to: mpsc::Sender<ActorRef<()>>,
    },
    SpawnRestartableSibling {
        restarted: mpsc::Sender<()>,
        reply_to: mpsc::Sender<ActorRef<SupervisionMsg>>,
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
            EscalationParentMsg::SpawnSibling { stopped, reply_to } => {
                let sibling = ctx.spawn("sibling", Props::new(move || StopProbe { stopped }))?;
                reply_to
                    .send(sibling)
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            EscalationParentMsg::SpawnBlockingSibling {
                entered_stop,
                release_stop,
                reply_to,
            } => {
                let sibling = ctx.spawn(
                    "blocking-sibling",
                    Props::new(move || EscalationBlockingSibling {
                        entered_stop,
                        release_stop: Some(release_stop),
                    }),
                )?;
                reply_to
                    .send(sibling)
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            EscalationParentMsg::SpawnRestartableSibling {
                restarted,
                reply_to,
            } => {
                let sibling = ctx.spawn(
                    "restartable-sibling",
                    Props::restartable(move || SupervisionProbe {
                        value: 0,
                        restarted: Some(restarted.clone()),
                    }),
                )?;
                reply_to
                    .send(sibling)
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

struct EscalationBlockingSibling {
    entered_stop: mpsc::Sender<()>,
    release_stop: Option<mpsc::Receiver<()>>,
}

impl Actor for EscalationBlockingSibling {
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
                .recv_timeout(Duration::from_secs(1))
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
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
fn escalated_parent_stop_waits_for_already_stopping_sibling() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn(
            "parent",
            Props::new(|| EscalationParent { restarted: None }),
        )
        .unwrap();
    let (entered_stop_tx, entered_stop_rx) = mpsc::channel();
    let (release_stop_tx, release_stop_rx) = mpsc::channel();
    let (sibling_tx, sibling_rx) = mpsc::channel();
    let (child_tx, child_rx) = mpsc::channel();

    parent
        .tell(EscalationParentMsg::SpawnBlockingSibling {
            entered_stop: entered_stop_tx,
            release_stop: release_stop_rx,
            reply_to: sibling_tx,
        })
        .unwrap();
    let sibling = sibling_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    parent
        .tell(EscalationParentMsg::SpawnChild {
            child_stopped: None,
            reply_to: child_tx,
        })
        .unwrap();
    let child = child_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&sibling);
    entered_stop_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    child.tell(EscalatingChildMsg::Fail).unwrap();
    assert!(child.wait_for_stop(Duration::from_secs(1)));

    let (ping_tx, ping_rx) = mpsc::channel();
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if parent
            .tell(EscalationParentMsg::Ping(ping_tx.clone()))
            .is_err()
        {
            break;
        }
        assert!(
            ping_rx.try_recv().is_err(),
            "parent must not process user messages queued behind an escalated stop"
        );
        thread::sleep(Duration::from_millis(5));
    }
    assert!(
        parent
            .tell(EscalationParentMsg::Ping(ping_tx.clone()))
            .is_err(),
        "parent should reject user messages after escalated stop has begun"
    );
    assert!(
        ping_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "parent must not process user messages once escalated stop has begun"
    );
    assert!(
        !parent.wait_for_stop(Duration::from_millis(50)),
        "parent must not terminate while an already stopping sibling is still blocked"
    );

    release_stop_tx.send(()).unwrap();
    assert!(sibling.wait_for_stop(Duration::from_secs(1)));
    assert!(parent.wait_for_stop(Duration::from_secs(1)));
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
fn escalated_parent_restart_waits_for_already_stopping_sibling() {
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
    let (entered_stop_tx, entered_stop_rx) = mpsc::channel();
    let (release_stop_tx, release_stop_rx) = mpsc::channel();
    let (sibling_tx, sibling_rx) = mpsc::channel();
    let (child_tx, child_rx) = mpsc::channel();

    parent
        .tell(EscalationParentMsg::SpawnBlockingSibling {
            entered_stop: entered_stop_tx,
            release_stop: release_stop_rx,
            reply_to: sibling_tx,
        })
        .unwrap();
    let sibling = sibling_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    parent
        .tell(EscalationParentMsg::SpawnChild {
            child_stopped: None,
            reply_to: child_tx,
        })
        .unwrap();
    let child = child_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&sibling);
    entered_stop_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    child.tell(EscalatingChildMsg::Fail).unwrap();
    restarted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(child.wait_for_stop(Duration::from_secs(1)));

    let (ping_tx, ping_rx) = mpsc::channel();
    parent
        .tell(EscalationParentMsg::Ping(ping_tx.clone()))
        .unwrap();
    assert!(
        ping_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "parent must not process user messages queued behind an escalated restart"
    );
    assert!(
        !sibling.wait_for_stop(Duration::from_millis(50)),
        "sibling must remain blocked until its stop hook completes"
    );

    release_stop_tx.send(()).unwrap();
    assert!(sibling.wait_for_stop(Duration::from_secs(1)));
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(!parent.is_stopped());
}

#[test]
fn escalating_child_preserving_parent_restart_keeps_sibling_alive() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (restarted_tx, restarted_rx) = mpsc::channel();
    let parent = system
        .spawn(
            "parent",
            Props::restartable(move || EscalationParent {
                restarted: Some(restarted_tx.clone()),
            })
            .with_supervisor(SupervisorStrategy::restart_preserving_children()),
        )
        .unwrap();
    let (sibling_stopped_tx, sibling_stopped_rx) = mpsc::channel();
    let (sibling_tx, sibling_rx) = mpsc::channel();
    let (child_tx, child_rx) = mpsc::channel();

    parent
        .tell(EscalationParentMsg::SpawnSibling {
            stopped: sibling_stopped_tx,
            reply_to: sibling_tx,
        })
        .unwrap();
    let sibling = sibling_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    parent
        .tell(EscalationParentMsg::SpawnChild {
            child_stopped: None,
            reply_to: child_tx,
        })
        .unwrap();
    let child = child_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    child.tell(EscalatingChildMsg::Fail).unwrap();
    restarted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (ping_tx, ping_rx) = mpsc::channel();
    parent.tell(EscalationParentMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    sibling.tell(()).unwrap();
    assert!(sibling_stopped_rx.try_recv().is_err());
    system.stop(&sibling);
    sibling_stopped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
}

#[test]
fn preserving_parent_restart_restarts_restartable_surviving_children() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (parent_restarted_tx, parent_restarted_rx) = mpsc::channel();
    let parent = system
        .spawn(
            "parent",
            Props::restartable(move || EscalationParent {
                restarted: Some(parent_restarted_tx.clone()),
            })
            .with_supervisor(SupervisorStrategy::restart_preserving_children()),
        )
        .unwrap();
    let (sibling_restarted_tx, sibling_restarted_rx) = mpsc::channel();
    let (sibling_tx, sibling_rx) = mpsc::channel();
    let (child_tx, child_rx) = mpsc::channel();

    parent
        .tell(EscalationParentMsg::SpawnRestartableSibling {
            restarted: sibling_restarted_tx,
            reply_to: sibling_tx,
        })
        .unwrap();
    let sibling = sibling_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let sibling_path = sibling.path().clone();
    sibling.tell(SupervisionMsg::Increment).unwrap();
    let (before_tx, before_rx) = mpsc::channel();
    sibling.tell(SupervisionMsg::Get(before_tx)).unwrap();
    assert_eq!(before_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);

    parent
        .tell(EscalationParentMsg::SpawnChild {
            child_stopped: None,
            reply_to: child_tx,
        })
        .unwrap();
    let child = child_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    child.tell(EscalatingChildMsg::Fail).unwrap();

    parent_restarted_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    sibling_restarted_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    let (after_tx, after_rx) = mpsc::channel();
    sibling.tell(SupervisionMsg::Get(after_tx)).unwrap();
    assert_eq!(after_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 0);
    assert_eq!(sibling.path(), &sibling_path);
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

#[test]
fn startup_failure_escalation_restarts_restartable_parent() {
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
    restarted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    let (ping_tx, ping_rx) = mpsc::channel();
    parent.tell(EscalationParentMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(!parent.is_stopped());
}
