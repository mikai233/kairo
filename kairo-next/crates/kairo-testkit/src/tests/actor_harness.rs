use super::*;

enum HarnessMsg {
    Ping(ActorRef<&'static str>),
    StartTimer {
        reply_to: ActorRef<&'static str>,
        ack: mpsc::Sender<()>,
    },
    TimerFired(ActorRef<&'static str>),
}

struct HarnessActor;

impl Actor for HarnessActor {
    type Msg = HarnessMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            HarnessMsg::Ping(reply_to) => reply_to
                .tell("pong")
                .map_err(|error| ActorError::Message(error.to_string())),
            HarnessMsg::StartTimer { reply_to, ack } => {
                ctx.schedule_once_self(Duration::from_secs(1), HarnessMsg::TimerFired(reply_to));
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                Ok(())
            }
            HarnessMsg::TimerFired(reply_to) => reply_to
                .tell("tick")
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }
}

#[test]
fn actor_harness_spawns_actor_and_creates_probe() {
    let harness = ActorHarness::spawn(
        "actor-harness-probe",
        "subject",
        Props::new(|| HarnessActor),
    )
    .expect("harness should spawn actor");
    let probe = harness
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");

    harness
        .tell(HarnessMsg::Ping(probe.actor_ref()))
        .expect("tell should enqueue");

    assert_eq!(probe.expect_msg(Duration::from_millis(50)).unwrap(), "pong");
    harness
        .shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn actor_harness_expect_stopped_waits_for_subject_stop() {
    let harness = ActorHarness::spawn("actor-harness-stop", "subject", Props::new(|| HarnessActor))
        .expect("harness should spawn actor");

    harness.stop();

    harness
        .expect_stopped(Duration::from_secs(1))
        .expect("subject should stop");
    harness
        .shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn actor_harness_manual_time_drives_subject_scheduler() {
    let (harness, time) = ActorHarness::with_manual_time(
        "actor-harness-manual-time",
        "subject",
        Props::new(|| HarnessActor),
    )
    .expect("harness should spawn actor with manual time");
    let probe = harness
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");
    let (ack_tx, ack_rx) = mpsc::channel();

    harness
        .tell(HarnessMsg::StartTimer {
            reply_to: probe.actor_ref(),
            ack: ack_tx,
        })
        .expect("timer start should enqueue");
    ack_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("timer should be scheduled");

    time.expect_no_msg_for(Duration::from_millis(999), &[&probe])
        .expect("probe should stay quiet before timer deadline");
    time.advance(Duration::from_millis(1));

    assert_eq!(probe.expect_msg(Duration::from_millis(50)).unwrap(), "tick");
    harness
        .shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}
