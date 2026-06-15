use super::*;

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
