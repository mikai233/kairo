use super::*;

fn wait_for_manual_pending(scheduler: &ManualScheduler, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if scheduler.pending_count() == expected {
            return;
        }
        thread::yield_now();
    }
    assert_eq!(scheduler.pending_count(), expected);
}

fn wait_for_manual_deadline(scheduler: &ManualScheduler, expected: Duration) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if scheduler.next_deadline() == Some(expected) {
            return;
        }
        thread::yield_now();
    }
    assert_eq!(scheduler.next_deadline(), Some(expected));
}

#[derive(Clone)]
enum ReceiveTimeoutProbeMsg {
    Arm {
        observed: mpsc::Sender<&'static str>,
        ack: mpsc::Sender<Option<Duration>>,
    },
    Touch {
        ack: mpsc::Sender<()>,
    },
    Cancel {
        ack: mpsc::Sender<()>,
    },
    Timeout {
        observed: mpsc::Sender<&'static str>,
    },
    Fail,
}

struct ReceiveTimeoutProbe;

impl Actor for ReceiveTimeoutProbe {
    type Msg = ReceiveTimeoutProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ReceiveTimeoutProbeMsg::Arm { observed, ack } => {
                ctx.set_receive_timeout(
                    Duration::from_secs(1),
                    ReceiveTimeoutProbeMsg::Timeout { observed },
                );
                ack.send(ctx.receive_timeout())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ReceiveTimeoutProbeMsg::Touch { ack } => {
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ReceiveTimeoutProbeMsg::Cancel { ack } => {
                ctx.cancel_receive_timeout();
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ReceiveTimeoutProbeMsg::Timeout { observed } => {
                observed
                    .send("timeout")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ReceiveTimeoutProbeMsg::Fail => return Err(ActorError::Message("boom".to_string())),
        }
        Ok(())
    }
}

#[derive(Clone)]
enum StartedReceiveTimeoutMsg {
    Timeout,
}

struct StartedReceiveTimeoutProbe {
    observed: mpsc::Sender<&'static str>,
}

impl Actor for StartedReceiveTimeoutProbe {
    type Msg = StartedReceiveTimeoutMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        ctx.set_receive_timeout(Duration::from_secs(1), StartedReceiveTimeoutMsg::Timeout);
        Ok(())
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            StartedReceiveTimeoutMsg::Timeout => {
                self.observed
                    .send("timeout")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[test]
fn receive_timeout_can_be_armed_from_started() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let _actor = system
        .spawn(
            "receive-timeout",
            Props::new(move || StartedReceiveTimeoutProbe {
                observed: observed_tx.clone(),
            }),
        )
        .unwrap();

    wait_for_manual_pending(&scheduler, 1);
    scheduler.advance(Duration::from_secs(1));
    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "timeout"
    );
}

#[test]
fn receive_timeout_resets_after_influencing_messages_and_repeats() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let actor = system
        .spawn("receive-timeout", Props::new(|| ReceiveTimeoutProbe))
        .unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (arm_tx, arm_rx) = mpsc::channel();

    actor
        .tell(ReceiveTimeoutProbeMsg::Arm {
            observed: observed_tx,
            ack: arm_tx,
        })
        .unwrap();
    assert_eq!(
        arm_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Some(Duration::from_secs(1))
    );
    wait_for_manual_deadline(&scheduler, Duration::from_secs(1));

    scheduler.advance(Duration::from_millis(999));
    assert!(observed_rx.recv_timeout(Duration::from_millis(20)).is_err());

    let (touch_tx, touch_rx) = mpsc::channel();
    actor
        .tell(ReceiveTimeoutProbeMsg::Touch { ack: touch_tx })
        .unwrap();
    touch_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    wait_for_manual_pending(&scheduler, 1);

    scheduler.advance(Duration::from_millis(1));
    assert!(observed_rx.recv_timeout(Duration::from_millis(20)).is_err());
    scheduler.advance(Duration::from_millis(999));
    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "timeout"
    );
    wait_for_manual_pending(&scheduler, 1);

    scheduler.advance(Duration::from_secs(1));
    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "timeout"
    );
}

#[test]
fn receive_timeout_reschedules_after_owner_resume_supervision() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let actor = system
        .spawn(
            "receive-timeout",
            Props::new(|| ReceiveTimeoutProbe).with_supervisor(SupervisorStrategy::Resume),
        )
        .unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (arm_tx, arm_rx) = mpsc::channel();

    actor
        .tell(ReceiveTimeoutProbeMsg::Arm {
            observed: observed_tx,
            ack: arm_tx,
        })
        .unwrap();
    assert_eq!(
        arm_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Some(Duration::from_secs(1))
    );
    wait_for_manual_deadline(&scheduler, Duration::from_secs(1));

    scheduler.advance(Duration::from_millis(999));
    assert!(observed_rx.recv_timeout(Duration::from_millis(20)).is_err());

    actor.tell(ReceiveTimeoutProbeMsg::Fail).unwrap();
    wait_for_manual_deadline(&scheduler, Duration::from_millis(1999));
    assert!(!actor.is_stopped());

    scheduler.advance(Duration::from_millis(999));
    assert_eq!(scheduler.now(), Duration::from_millis(1998));
    assert_eq!(scheduler.next_deadline(), Some(Duration::from_millis(1999)));
    assert!(observed_rx.recv_timeout(Duration::from_millis(20)).is_err());
    assert!(!actor.is_stopped());

    scheduler.advance(Duration::from_millis(1));
    assert_eq!(scheduler.now(), Duration::from_millis(1999));
    assert_ne!(scheduler.next_deadline(), Some(Duration::from_millis(1999)));
    assert!(!actor.is_stopped());
    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "timeout"
    );
}

#[test]
fn cancel_receive_timeout_suppresses_later_delivery() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let actor = system
        .spawn("receive-timeout", Props::new(|| ReceiveTimeoutProbe))
        .unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (arm_tx, arm_rx) = mpsc::channel();

    actor
        .tell(ReceiveTimeoutProbeMsg::Arm {
            observed: observed_tx,
            ack: arm_tx,
        })
        .unwrap();
    arm_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let (cancel_tx, cancel_rx) = mpsc::channel();
    actor
        .tell(ReceiveTimeoutProbeMsg::Cancel { ack: cancel_tx })
        .unwrap();
    cancel_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    scheduler.advance(Duration::from_secs(1));
    assert!(observed_rx.recv_timeout(Duration::from_millis(20)).is_err());
}

#[test]
fn actor_stop_cancels_active_receive_timeout() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test-receive-timeout-direct-stop")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let actor = system
        .spawn("receive-timeout", Props::new(|| ReceiveTimeoutProbe))
        .unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (arm_tx, arm_rx) = mpsc::channel();

    actor
        .tell(ReceiveTimeoutProbeMsg::Arm {
            observed: observed_tx,
            ack: arm_tx,
        })
        .unwrap();
    assert_eq!(
        arm_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Some(Duration::from_secs(1))
    );
    wait_for_manual_deadline(&scheduler, Duration::from_secs(1));

    system.stop(&actor);
    assert!(actor.wait_for_stop(Duration::from_secs(1)));

    scheduler.advance(Duration::from_secs(1));
    assert!(observed_rx.recv_timeout(Duration::from_millis(20)).is_err());
    assert!(
        system.dead_letters().is_empty(),
        "cancelled receive timeout after actor stop must not publish late dead letters: {:?}",
        system.dead_letters().records()
    );
}

#[test]
fn actor_system_terminate_cancels_user_actor_receive_timeout() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test-user-receive-timeout-system-stop")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let actor = system
        .spawn("receive-timeout", Props::new(|| ReceiveTimeoutProbe))
        .unwrap();

    assert_actor_system_terminate_cancels_receive_timeout(system, scheduler, actor);
}

#[test]
fn actor_system_terminate_cancels_system_actor_receive_timeout() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test-system-receive-timeout-system-stop")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let actor = system
        .spawn_system("system-receive-timeout", Props::new(|| ReceiveTimeoutProbe))
        .unwrap();

    assert_actor_system_terminate_cancels_receive_timeout(system, scheduler, actor);
}

fn assert_actor_system_terminate_cancels_receive_timeout(
    system: ActorSystem,
    scheduler: ManualScheduler,
    actor: ActorRef<ReceiveTimeoutProbeMsg>,
) {
    let (observed_tx, observed_rx) = mpsc::channel();
    let (arm_tx, arm_rx) = mpsc::channel();

    actor
        .tell(ReceiveTimeoutProbeMsg::Arm {
            observed: observed_tx,
            ack: arm_tx,
        })
        .unwrap();
    assert_eq!(
        arm_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Some(Duration::from_secs(1))
    );
    wait_for_manual_deadline(&scheduler, Duration::from_secs(1));

    system.terminate(Duration::from_secs(1)).unwrap();
    assert!(actor.is_stopped());
    assert!(system.is_terminated());

    scheduler.advance(Duration::from_secs(1));
    assert!(observed_rx.recv_timeout(Duration::from_millis(20)).is_err());
    assert!(
        system.dead_letters().is_empty(),
        "cancelled receive timeouts must not publish late dead letters: {:?}",
        system.dead_letters().records()
    );
}
