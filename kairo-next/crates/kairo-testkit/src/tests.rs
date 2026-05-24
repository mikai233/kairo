use std::time::Duration;

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
fn manual_time_delivers_due_messages_in_advance_order() {
    let kit = ActorSystemTestKit::new("manual-time").expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");
    let mut time = ManualTime::default();

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
    let mut time = ManualTime::default();

    let handle = time.schedule_once(Duration::from_secs(1), probe.actor_ref(), 1);

    assert!(handle.cancel());
    time.advance(Duration::from_secs(1));

    assert!(handle.is_cancelled());
    assert_eq!(probe.expect_no_msg(Duration::ZERO), Ok(()));
    assert_eq!(time.pending_count(), 0);
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}
