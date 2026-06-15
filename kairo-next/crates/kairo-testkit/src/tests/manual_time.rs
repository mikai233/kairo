use super::*;

#[test]
fn manual_time_delivers_due_messages_in_advance_order() {
    let kit = ActorSystemTestKit::new("manual-time").expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");
    let time = ManualTime::default();

    time.schedule_once(Duration::from_secs(2), probe.actor_ref(), "second");
    time.schedule_once(Duration::from_secs(1), probe.actor_ref(), "first");

    time.advance(Duration::from_secs(1));
    assert_eq!(
        probe.expect_msg(Duration::from_millis(50)).unwrap(),
        "first"
    );
    assert_eq!(probe.expect_no_msg(Duration::ZERO), Ok(()));

    time.advance(Duration::from_secs(1));
    assert_eq!(
        probe.expect_msg(Duration::from_millis(50)).unwrap(),
        "second"
    );
    assert_eq!(time.pending_count(), 0);
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn manual_time_cancel_suppresses_delivery() {
    let kit = ActorSystemTestKit::new("manual-time-cancel").expect("system should build");
    let probe = kit.create_probe::<u8>("probe").expect("probe should spawn");
    let time = ManualTime::default();

    let handle = time.schedule_once(Duration::from_secs(1), probe.actor_ref(), 1);

    assert!(handle.cancel());
    time.advance(Duration::from_secs(1));

    assert!(handle.is_cancelled());
    assert_eq!(probe.expect_no_msg(Duration::ZERO), Ok(()));
    assert_eq!(time.pending_count(), 0);
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn manual_time_can_drive_actor_system_schedule_once() {
    let (kit, time) =
        ActorSystemTestKit::with_manual_time("manual-time-system").expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");

    kit.system()
        .schedule_once(Duration::from_secs(1), probe.actor_ref(), "scheduled");

    assert_eq!(probe.expect_no_msg(Duration::ZERO), Ok(()));
    time.advance(Duration::from_secs(1));
    assert_eq!(
        probe.expect_msg(Duration::from_millis(50)).unwrap(),
        "scheduled"
    );
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn manual_time_expect_no_msg_for_advances_and_checks_probe() {
    let kit = ActorSystemTestKit::new("manual-time-expect-no-msg").expect("system should build");
    let first = kit
        .create_probe::<&'static str>("first")
        .expect("first probe should spawn");
    let second = kit
        .create_probe::<&'static str>("second")
        .expect("second probe should spawn");
    let time = ManualTime::default();

    time.schedule_once(Duration::from_secs(1), first.actor_ref(), "first");
    time.schedule_once(Duration::from_secs(2), second.actor_ref(), "second");

    time.expect_no_msg_for(Duration::from_millis(999), &[&first, &second])
        .expect("no probe should receive before the first deadline");
    assert_eq!(time.now(), Duration::from_millis(999));

    time.advance(Duration::from_millis(1));
    assert_eq!(
        first.expect_msg(Duration::from_millis(50)).unwrap(),
        "first"
    );
    assert_eq!(second.expect_no_msg(Duration::ZERO), Ok(()));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn manual_time_expect_no_msg_for_accepts_heterogeneous_probes() {
    let kit = ActorSystemTestKit::new("manual-time-expect-no-msg-heterogeneous")
        .expect("system should build");
    let text = kit
        .create_probe::<&'static str>("text")
        .expect("text probe should spawn");
    let number = kit
        .create_probe::<u8>("number")
        .expect("number probe should spawn");
    let time = ManualTime::default();

    time.schedule_once(Duration::from_secs(1), text.actor_ref(), "text");
    time.schedule_once(Duration::from_secs(2), number.actor_ref(), 2);

    time.expect_no_msg_for(Duration::from_millis(999), &[&text, &number])
        .expect("no probe should receive before the first deadline");

    time.advance(Duration::from_millis(1));
    assert_eq!(text.expect_msg(Duration::from_millis(50)).unwrap(), "text");
    assert_eq!(number.expect_no_msg(Duration::ZERO), Ok(()));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn manual_time_expect_no_msg_for_reports_due_probe_message() {
    let kit =
        ActorSystemTestKit::new("manual-time-expect-no-msg-failure").expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");
    let time = ManualTime::default();

    time.schedule_once(Duration::from_secs(1), probe.actor_ref(), "due");

    let error = time
        .expect_no_msg_for(Duration::from_secs(1), &[&probe])
        .expect_err("due message should fail no-message expectation");
    assert!(matches!(
        error,
        ProbeError::UnexpectedMessage { expected, .. } if expected == "no message"
    ));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn manual_time_can_drive_actor_timers() {
    let (kit, time) =
        ActorSystemTestKit::with_manual_time("manual-time-timer").expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");
    let actor = kit
        .system()
        .spawn("timer", Props::new(|| ManualTimerProbe))
        .expect("timer actor should spawn");
    let (ack_tx, ack_rx) = mpsc::channel();

    actor
        .tell(ManualTimerMsg::Start {
            reply_to: probe.actor_ref(),
            ack: ack_tx,
        })
        .expect("start should enqueue");
    ack_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("timer should be scheduled");

    assert_eq!(probe.expect_no_msg(Duration::ZERO), Ok(()));
    time.advance(Duration::from_secs(1));
    assert_eq!(probe.expect_msg(Duration::from_millis(50)).unwrap(), "tick");
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn manual_time_can_drive_repeated_actor_timers_until_cancelled() {
    let (kit, time) =
        ActorSystemTestKit::with_manual_time("manual-time-repeated").expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");
    let actor = kit
        .system()
        .spawn("timer", Props::new(|| ManualTimerProbe))
        .expect("timer actor should spawn");
    let (start_tx, start_rx) = mpsc::channel();

    actor
        .tell(ManualTimerMsg::StartRepeated {
            reply_to: probe.actor_ref(),
            ack: start_tx,
        })
        .expect("start should enqueue");
    start_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("timer should be scheduled");

    time.advance(Duration::from_secs(1));
    assert_eq!(probe.expect_msg(Duration::from_millis(50)).unwrap(), "tick");
    time.advance(Duration::from_secs(1));
    assert_eq!(probe.expect_msg(Duration::from_millis(50)).unwrap(), "tick");

    let (cancel_tx, cancel_rx) = mpsc::channel();
    actor
        .tell(ManualTimerMsg::Cancel { ack: cancel_tx })
        .expect("cancel should enqueue");
    cancel_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("timer should be cancelled");
    time.advance(Duration::from_secs(1));
    assert_eq!(probe.expect_no_msg(Duration::ZERO), Ok(()));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn manual_time_can_drive_fixed_rate_actor_timers() {
    let (kit, time) = ActorSystemTestKit::with_manual_time("manual-time-fixed-rate")
        .expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");
    let actor = kit
        .system()
        .spawn("timer", Props::new(|| ManualTimerProbe))
        .expect("timer actor should spawn");
    let (start_tx, start_rx) = mpsc::channel();

    actor
        .tell(ManualTimerMsg::StartFixedRate {
            reply_to: probe.actor_ref(),
            ack: start_tx,
        })
        .expect("start should enqueue");
    start_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("timer should be scheduled");

    time.advance(Duration::from_secs(2));
    assert_eq!(probe.expect_msg(Duration::from_millis(50)).unwrap(), "tick");
    assert_eq!(probe.expect_msg(Duration::from_millis(50)).unwrap(), "tick");
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn manual_time_can_drive_actor_receive_timeout() {
    let (kit, time) = ActorSystemTestKit::with_manual_time("manual-time-receive-timeout")
        .expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");
    let actor = kit
        .system()
        .spawn("receive-timeout", Props::new(|| ManualReceiveTimeoutProbe))
        .expect("receive-timeout actor should spawn");
    let (ack_tx, ack_rx) = mpsc::channel();

    actor
        .tell(ManualReceiveTimeoutMsg::Arm {
            reply_to: probe.actor_ref(),
            ack: ack_tx,
        })
        .expect("receive timeout should arm");
    ack_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive timeout should be scheduled");
    wait_for_manual_time_pending(&time, 1);

    time.advance(Duration::from_secs(1));
    wait_for_manual_time_pending(&time, 1);
    assert_eq!(probe.expect_msg(Duration::from_secs(1)).unwrap(), "idle");
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

fn wait_for_manual_time_pending(time: &ManualTime, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if time.pending_count() == expected {
            return;
        }
        thread::yield_now();
    }
    assert_eq!(time.pending_count(), expected);
}

#[derive(Clone)]
enum ManualTimerMsg {
    Start {
        reply_to: ActorRef<&'static str>,
        ack: mpsc::Sender<()>,
    },
    StartRepeated {
        reply_to: ActorRef<&'static str>,
        ack: mpsc::Sender<()>,
    },
    StartFixedRate {
        reply_to: ActorRef<&'static str>,
        ack: mpsc::Sender<()>,
    },
    Cancel {
        ack: mpsc::Sender<()>,
    },
    Fired(ActorRef<&'static str>),
}

struct ManualTimerProbe;

impl Actor for ManualTimerProbe {
    type Msg = ManualTimerMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ManualTimerMsg::Start { reply_to, ack } => {
                ctx.start_single_timer(
                    "manual",
                    Duration::from_secs(1),
                    ManualTimerMsg::Fired(reply_to),
                );
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ManualTimerMsg::StartRepeated { reply_to, ack } => {
                ctx.start_timer_with_fixed_delay(
                    "manual-repeated",
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                    ManualTimerMsg::Fired(reply_to),
                );
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ManualTimerMsg::StartFixedRate { reply_to, ack } => {
                ctx.start_timer_at_fixed_rate(
                    "manual-repeated",
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                    ManualTimerMsg::Fired(reply_to),
                );
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ManualTimerMsg::Cancel { ack } => {
                ctx.cancel_timer("manual-repeated");
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ManualTimerMsg::Fired(reply_to) => {
                reply_to
                    .tell("tick")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
enum ManualReceiveTimeoutMsg {
    Arm {
        reply_to: ActorRef<&'static str>,
        ack: mpsc::Sender<()>,
    },
    Idle(ActorRef<&'static str>),
}

struct ManualReceiveTimeoutProbe;

impl Actor for ManualReceiveTimeoutProbe {
    type Msg = ManualReceiveTimeoutMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ManualReceiveTimeoutMsg::Arm { reply_to, ack } => {
                ctx.set_receive_timeout(
                    Duration::from_secs(1),
                    ManualReceiveTimeoutMsg::Idle(reply_to),
                );
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ManualReceiveTimeoutMsg::Idle(reply_to) => {
                reply_to
                    .tell("idle")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}
