use super::*;

#[derive(Clone)]
enum TimerProbeMsg {
    StartSingle {
        reply_to: mpsc::Sender<(&'static str, bool)>,
    },
    StartSingleWithAck {
        fired: mpsc::Sender<&'static str>,
        ack: mpsc::Sender<bool>,
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
    StartZeroFixedDelay {
        fired: mpsc::Sender<&'static str>,
        active: mpsc::Sender<bool>,
    },
    StartZeroFixedRate {
        fired: mpsc::Sender<&'static str>,
        active: mpsc::Sender<bool>,
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
    Fail,
    Ping(mpsc::Sender<()>),
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
            TimerProbeMsg::StartSingleWithAck { fired, ack } => {
                ctx.start_single_timer(
                    "resume-single",
                    Duration::from_secs(1),
                    TimerProbeMsg::FireLabel {
                        label: "resume-single",
                        reply_to: fired,
                    },
                );
                ack.send(ctx.is_timer_active("resume-single"))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
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
            TimerProbeMsg::StartZeroFixedDelay { fired, active } => {
                ctx.start_timer_with_fixed_delay(
                    "zero-repeat",
                    Duration::ZERO,
                    Duration::ZERO,
                    TimerProbeMsg::FireLabel {
                        label: "zero-repeat",
                        reply_to: fired,
                    },
                );
                active
                    .send(ctx.is_timer_active("zero-repeat"))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TimerProbeMsg::StartZeroFixedRate { fired, active } => {
                ctx.start_timer_at_fixed_rate(
                    "zero-rate",
                    Duration::ZERO,
                    Duration::ZERO,
                    TimerProbeMsg::FireLabel {
                        label: "zero-rate",
                        reply_to: fired,
                    },
                );
                active
                    .send(ctx.is_timer_active("zero-rate"))
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
            TimerProbeMsg::Fail => return Err(ActorError::Message("boom".to_string())),
            TimerProbeMsg::Ping(reply_to) => {
                reply_to
                    .send(())
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
fn single_timer_survives_owner_resume_supervision() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let actor = system
        .spawn(
            "timer",
            Props::new(|| TimerProbe).with_supervisor(SupervisorStrategy::Resume),
        )
        .unwrap();
    let (fired_tx, fired_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::StartSingleWithAck {
            fired: fired_tx,
            ack: ack_tx,
        })
        .unwrap();
    assert!(ack_rx.recv_timeout(Duration::from_secs(1)).unwrap());

    actor.tell(TimerProbeMsg::Fail).unwrap();
    let (ping_tx, ping_rx) = mpsc::channel();
    actor.tell(TimerProbeMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    scheduler.advance(Duration::from_secs(1));
    assert_eq!(
        fired_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "resume-single"
    );
    assert!(fired_rx.recv_timeout(Duration::from_millis(100)).is_err());
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
fn zero_fixed_delay_timer_does_not_start_or_leave_active_key() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("timer", Props::new(|| TimerProbe)).unwrap();
    let (fired_tx, fired_rx) = mpsc::channel();
    let (active_tx, active_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::StartZeroFixedDelay {
            fired: fired_tx,
            active: active_tx,
        })
        .unwrap();

    assert!(!active_rx.recv_timeout(Duration::from_secs(1)).unwrap());
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
fn zero_fixed_rate_timer_does_not_start_or_leave_active_key() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("timer", Props::new(|| TimerProbe)).unwrap();
    let (fired_tx, fired_rx) = mpsc::channel();
    let (active_tx, active_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::StartZeroFixedRate {
            fired: fired_tx,
            active: active_tx,
        })
        .unwrap();

    assert!(!active_rx.recv_timeout(Duration::from_secs(1)).unwrap());
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
fn direct_actor_stop_cancels_user_actor_timers() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test-user-timer-direct-stop")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let actor = system.spawn("timer", Props::new(|| TimerProbe)).unwrap();

    assert_direct_actor_stop_cancels_active_timers(&system, &scheduler, actor);
}

#[test]
fn direct_actor_stop_cancels_system_actor_timers() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test-system-timer-direct-stop")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let actor = system
        .spawn_system("system-timer", Props::new(|| TimerProbe))
        .unwrap();

    assert_direct_actor_stop_cancels_active_timers(&system, &scheduler, actor);
}

fn assert_direct_actor_stop_cancels_active_timers(
    system: &ActorSystem,
    scheduler: &ManualScheduler,
    actor: ActorRef<TimerProbeMsg>,
) {
    let (fired_tx, fired_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::StartSingleWithAck {
            fired: fired_tx,
            ack: ack_tx,
        })
        .unwrap();
    assert!(ack_rx.recv_timeout(Duration::from_secs(1)).unwrap());

    system.stop(&actor);
    assert!(actor.wait_for_stop(Duration::from_secs(1)));

    scheduler.advance(Duration::from_secs(1));
    assert!(fired_rx.recv_timeout(Duration::from_millis(20)).is_err());
    assert!(
        system.dead_letters().is_empty(),
        "cancelled owner timers after direct stop must not publish late dead letters: {:?}",
        system.dead_letters().records()
    );
}

#[test]
fn actor_system_terminate_cancels_user_actor_timers() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test-user-timer-system-stop")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let actor = system.spawn("timer", Props::new(|| TimerProbe)).unwrap();

    assert_actor_system_terminate_cancels_active_timers(system, scheduler, actor);
}

#[test]
fn actor_system_terminate_cancels_system_actor_timers() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test-system-timer-system-stop")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let actor = system
        .spawn_system("system-timer", Props::new(|| TimerProbe))
        .unwrap();

    assert_actor_system_terminate_cancels_active_timers(system, scheduler, actor);
}

fn assert_actor_system_terminate_cancels_active_timers(
    system: ActorSystem,
    scheduler: ManualScheduler,
    actor: ActorRef<TimerProbeMsg>,
) {
    let (fired_tx, fired_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();

    actor
        .tell(TimerProbeMsg::StartSingleWithAck {
            fired: fired_tx,
            ack: ack_tx,
        })
        .unwrap();
    assert!(ack_rx.recv_timeout(Duration::from_secs(1)).unwrap());

    system.terminate(Duration::from_secs(1)).unwrap();
    assert!(actor.is_stopped());
    assert!(system.is_terminated());

    scheduler.advance(Duration::from_secs(1));
    assert!(fired_rx.recv_timeout(Duration::from_millis(20)).is_err());
    assert!(
        system.dead_letters().is_empty(),
        "cancelled owner timers must not publish late dead letters: {:?}",
        system.dead_letters().records()
    );
}
