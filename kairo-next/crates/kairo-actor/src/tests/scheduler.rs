use super::*;

enum ScheduledMsg {
    Record(&'static str),
    ScheduleSelf {
        delay: Duration,
        reply_to: mpsc::Sender<&'static str>,
    },
    ScheduleSelfWithAck {
        delay: Duration,
        reply_to: mpsc::Sender<&'static str>,
        ack: mpsc::Sender<()>,
    },
    SelfFired(mpsc::Sender<&'static str>),
    Fail,
    Ping(mpsc::Sender<()>),
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
            ScheduledMsg::ScheduleSelfWithAck {
                delay,
                reply_to,
                ack,
            } => {
                ctx.schedule_once_self(delay, ScheduledMsg::SelfFired(reply_to));
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ScheduledMsg::SelfFired(reply_to) => {
                reply_to
                    .send("self")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ScheduledMsg::Fail => return Err(ActorError::Message("boom".to_string())),
            ScheduledMsg::Ping(reply_to) => {
                reply_to
                    .send(())
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

#[test]
fn schedule_once_self_survives_owner_resume_supervision() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let (observed_tx, _observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "scheduled",
            Props::new(move || ScheduledProbe {
                observed: observed_tx,
            })
            .with_supervisor(SupervisorStrategy::Resume),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();

    actor
        .tell(ScheduledMsg::ScheduleSelfWithAck {
            delay: Duration::from_secs(1),
            reply_to: reply_tx,
            ack: ack_tx,
        })
        .unwrap();
    ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    actor.tell(ScheduledMsg::Fail).unwrap();
    let (ping_tx, ping_rx) = mpsc::channel();
    actor.tell(ScheduledMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    scheduler.advance(Duration::from_secs(1));
    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "self"
    );
}

#[test]
fn schedule_once_self_survives_owner_restart_supervision() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let (observed_tx, _observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "scheduled",
            Props::restartable(move || ScheduledProbe {
                observed: observed_tx.clone(),
            })
            .with_supervisor(SupervisorStrategy::Restart),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();

    actor
        .tell(ScheduledMsg::ScheduleSelfWithAck {
            delay: Duration::from_secs(1),
            reply_to: reply_tx,
            ack: ack_tx,
        })
        .unwrap();
    ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    actor.tell(ScheduledMsg::Fail).unwrap();
    let (ping_tx, ping_rx) = mpsc::channel();
    actor.tell(ScheduledMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    scheduler.advance(Duration::from_secs(1));
    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "self"
    );
}

#[test]
fn schedule_once_self_after_owner_stop_goes_to_dead_letters() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
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
    let (ack_tx, ack_rx) = mpsc::channel();

    actor
        .tell(ScheduledMsg::ScheduleSelfWithAck {
            delay: Duration::from_secs(1),
            reply_to: reply_tx,
            ack: ack_tx,
        })
        .unwrap();
    ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&actor);
    assert!(actor.wait_for_stop(Duration::from_secs(1)));

    scheduler.advance(Duration::from_secs(1));
    assert!(reply_rx.recv_timeout(Duration::from_millis(50)).is_err());
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );

    let records = system.dead_letters().records();
    assert_eq!(records[0].recipient(), actor.path());
    assert_eq!(records[0].reason(), "actor is stopped");
}

#[test]
fn actor_system_schedule_once_after_termination_is_cancelled() {
    let scheduler = ManualScheduler::new();
    let system = ActorSystem::builder("test")
        .manual_scheduler(scheduler.clone())
        .build()
        .unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "scheduled",
            Props::new(move || ScheduledProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();

    system.terminate(Duration::from_secs(1)).unwrap();
    let cancellable =
        system.schedule_once(Duration::from_secs(1), actor, ScheduledMsg::Record("late"));

    assert!(cancellable.is_cancelled());
    assert_eq!(scheduler.pending_count(), 0);
    scheduler.advance(Duration::from_secs(1));
    assert!(observed_rx.recv_timeout(Duration::ZERO).is_err());
    assert!(system.dead_letters().is_empty());
}
