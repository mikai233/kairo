use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, AnyActorRef, Context, Props};

use crate::{ActorSystemTestKit, ManualTime, ProbeError, TestProbe};

#[test]
fn test_probe_receives_typed_messages() {
    let kit = ActorSystemTestKit::new("test-probe").expect("system should build");
    let probe = kit
        .create_probe::<String>("probe")
        .expect("probe should spawn");

    probe
        .actor_ref()
        .tell("hello".to_string())
        .expect("probe tell should enqueue");

    assert_eq!(
        probe.expect_msg(Duration::from_millis(50)).unwrap(),
        "hello"
    );
    assert_eq!(probe.expect_no_msg(Duration::ZERO), Ok(()));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_reports_expectation_mismatch() {
    let kit = ActorSystemTestKit::new("test-probe-mismatch").expect("system should build");
    let probe = TestProbe::<u32>::spawn(kit.system(), "probe").expect("probe should spawn");

    probe
        .actor_ref()
        .tell(7)
        .expect("probe tell should enqueue");

    let error = probe
        .expect_msg_eq(8, Duration::from_millis(50))
        .expect_err("probe should report unexpected message");
    assert!(matches!(
        error,
        ProbeError::UnexpectedMessage {
            expected,
            actual
        } if expected == "8" && actual == "7"
    ));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_watch_with_receives_custom_termination_message() {
    let kit = ActorSystemTestKit::new("test-probe-watch-with").expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");
    let subject = kit
        .system()
        .spawn("subject", Props::new(|| UnitActor))
        .expect("subject should spawn");

    probe
        .watch_with(&subject, "terminated")
        .expect("watch should register");
    kit.system().stop(&subject);

    assert_eq!(
        probe.expect_msg(Duration::from_millis(50)).unwrap(),
        "terminated"
    );
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_expect_terminated_checks_expected_actor() {
    let kit = ActorSystemTestKit::new("test-probe-expect-terminated").expect("system should build");
    let probe = kit
        .create_probe::<AnyActorRef>("probe")
        .expect("probe should spawn");
    let subject = kit
        .system()
        .spawn("subject", Props::new(|| UnitActor))
        .expect("subject should spawn");

    kit.system().stop(&subject);

    assert_eq!(
        probe
            .expect_terminated(&subject, Duration::from_millis(50))
            .unwrap(),
        subject.as_any()
    );
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_expect_terminated_reports_unexpected_actor() {
    let kit =
        ActorSystemTestKit::new("test-probe-unexpected-terminated").expect("system should build");
    let probe = kit
        .create_probe::<AnyActorRef>("probe")
        .expect("probe should spawn");
    let expected = kit
        .system()
        .spawn("expected", Props::new(|| UnitActor))
        .expect("expected should spawn");
    let other = kit
        .system()
        .spawn("other", Props::new(|| UnitActor))
        .expect("other should spawn");

    probe
        .watch_terminated(&other)
        .expect("other watch should register");
    kit.system().stop(&other);

    let error = probe
        .expect_terminated(&expected, Duration::from_millis(50))
        .expect_err("probe should report unexpected terminated actor");
    assert!(matches!(
        error,
        ProbeError::UnexpectedMessage {
            expected: expected_path,
            actual
        } if expected_path == expected.path().to_string() && actual == other.path().to_string()
    ));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

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
    assert_eq!(probe.expect_msg(Duration::from_millis(50)).unwrap(), "idle");
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

fn wait_for_manual_time_pending(time: &ManualTime, expected: usize) {
    for _ in 0..100 {
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

struct UnitActor;

impl Actor for UnitActor {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }
}

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
