use super::*;

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
fn test_probe_expect_msg_matching_returns_matching_message() {
    let kit = ActorSystemTestKit::new("test-probe-matching").expect("system should build");
    let probe = kit
        .create_probe::<String>("probe")
        .expect("probe should spawn");

    probe
        .actor_ref()
        .tell("cluster-member-up".to_string())
        .expect("probe tell should enqueue");

    let message = probe
        .expect_msg_matching(Duration::from_millis(50), |message| {
            message.starts_with("cluster-")
        })
        .expect("probe should return matching message");
    assert_eq!(message, "cluster-member-up");
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_expect_msg_matching_reports_mismatch() {
    let kit = ActorSystemTestKit::new("test-probe-matching-failure").expect("system should build");
    let probe = kit
        .create_probe::<u32>("probe")
        .expect("probe should spawn");

    probe
        .actor_ref()
        .tell(41)
        .expect("probe tell should enqueue");

    let error = probe
        .expect_msg_matching(Duration::from_millis(50), |message| *message == 42)
        .expect_err("probe should report mismatch");
    assert_eq!(
        error,
        ProbeError::UnexpectedMessage {
            expected: "message matching predicate".to_string(),
            actual: "41".to_string(),
        }
    );
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_await_assert_retries_probe_assertion_until_success() {
    let kit = ActorSystemTestKit::new("test-probe-await-assert").expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");
    let mut attempts = 0;

    probe
        .actor_ref()
        .tell("ready")
        .expect("probe tell should enqueue");

    let message = probe
        .await_assert(
            Duration::from_millis(50),
            Duration::from_millis(1),
            |probe| {
                attempts += 1;
                if attempts == 1 {
                    Err(ProbeError::Timeout(Duration::ZERO))
                } else {
                    probe.expect_msg(Duration::from_millis(50))
                }
            },
        )
        .expect("probe assertion should eventually succeed");

    assert_eq!(attempts, 2);
    assert_eq!(message, "ready");
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_await_assert_reports_last_probe_error() {
    let kit =
        ActorSystemTestKit::new("test-probe-await-assert-timeout").expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");

    let error = probe
        .await_assert(Duration::ZERO, Duration::from_millis(1), |probe| {
            probe.expect_msg(Duration::ZERO)
        })
        .expect_err("probe assertion should time out");

    assert_eq!(error.attempts(), 1);
    assert_eq!(error.last_error(), &ProbeError::Timeout(Duration::ZERO));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_receive_messages_collects_fixed_count_under_deadline() {
    let kit = ActorSystemTestKit::new("test-probe-receive-messages").expect("system should build");
    let probe = kit.create_probe::<u8>("probe").expect("probe should spawn");

    for message in [1, 2, 3] {
        probe
            .actor_ref()
            .tell(message)
            .expect("probe tell should enqueue");
    }

    assert_eq!(
        probe
            .receive_messages(3, Duration::from_millis(50))
            .expect("probe should receive all messages"),
        vec![1, 2, 3]
    );
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_receive_messages_reports_partial_timeout() {
    let kit = ActorSystemTestKit::new("test-probe-receive-messages-timeout")
        .expect("system should build");
    let probe = kit.create_probe::<u8>("probe").expect("probe should spawn");

    probe
        .actor_ref()
        .tell(1)
        .expect("probe tell should enqueue");

    let error = probe
        .receive_messages(2, Duration::from_millis(5))
        .expect_err("probe should report partial timeout");

    assert!(matches!(
        error,
        ProbeError::ReceiveMessagesTimeout {
            expected: 2,
            received: 1,
            ..
        }
    ));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_receive_messages_zero_count_does_not_consume() {
    let kit =
        ActorSystemTestKit::new("test-probe-receive-messages-zero").expect("system should build");
    let probe = kit.create_probe::<u8>("probe").expect("probe should spawn");

    probe
        .actor_ref()
        .tell(7)
        .expect("probe tell should enqueue");

    assert_eq!(
        probe
            .receive_messages(0, Duration::ZERO)
            .expect("zero count should succeed"),
        Vec::<u8>::new()
    );
    assert_eq!(probe.expect_msg(Duration::from_millis(50)).unwrap(), 7);
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
fn test_probe_unwatch_suppresses_custom_termination_message() {
    let kit = ActorSystemTestKit::new("test-probe-unwatch").expect("system should build");
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
    probe.unwatch(&subject);
    kit.system().stop(&subject);

    assert_eq!(probe.expect_no_msg(Duration::from_millis(50)), Ok(()));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_watch_receives_plain_termination_signal() {
    let kit = ActorSystemTestKit::new("test-probe-watch-signal").expect("system should build");
    let probe = kit
        .create_probe::<Signal>("probe")
        .expect("probe should spawn");
    let subject = kit
        .system()
        .spawn("subject", Props::new(|| UnitActor))
        .expect("subject should spawn");

    probe.watch(&subject).expect("watch should register");
    kit.system().stop(&subject);

    assert_eq!(
        probe.expect_msg(Duration::from_millis(50)).unwrap(),
        Signal::Terminated(subject.as_any())
    );
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_expect_plain_terminated_observes_already_stopped_actor() {
    let kit =
        ActorSystemTestKit::new("test-probe-watch-signal-stopped").expect("system should build");
    let probe = kit
        .create_probe::<Signal>("probe")
        .expect("probe should spawn");
    let subject = kit
        .system()
        .spawn("subject", Props::new(|| UnitActor))
        .expect("subject should spawn");

    kit.system().stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

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
fn test_probe_expect_plain_terminated_within_uses_shared_deadline() {
    let kit =
        ActorSystemTestKit::new("test-probe-watch-signal-within").expect("system should build");
    let probe = kit
        .create_probe::<Signal>("probe")
        .expect("probe should spawn");
    let subject = kit
        .system()
        .spawn("subject", Props::new(|| UnitActor))
        .expect("subject should spawn");

    let observed = probe
        .within(Duration::from_millis(50), |probe, scope| {
            kit.system().stop(&subject);
            probe.expect_terminated_within(&subject, scope)
        })
        .expect("termination should be observed inside shared deadline");

    assert_eq!(observed, subject.as_any());
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_expect_plain_terminated_reports_unexpected_actor() {
    let kit =
        ActorSystemTestKit::new("test-probe-watch-signal-unexpected").expect("system should build");
    let probe = kit
        .create_probe::<Signal>("probe")
        .expect("probe should spawn");
    let expected = kit
        .system()
        .spawn("expected", Props::new(|| UnitActor))
        .expect("expected should spawn");
    let other = kit
        .system()
        .spawn("other", Props::new(|| UnitActor))
        .expect("other should spawn");

    probe.watch(&other).expect("other watch should register");
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
fn test_probe_watch_terminated_receives_erased_actor_ref() {
    let kit = ActorSystemTestKit::new("test-probe-watch-terminated").expect("system should build");
    let probe = kit
        .create_probe::<AnyActorRef>("probe")
        .expect("probe should spawn");
    let subject = kit
        .system()
        .spawn("subject", Props::new(|| UnitActor))
        .expect("subject should spawn");

    probe
        .watch_terminated(&subject)
        .expect("watch should register");
    kit.system().stop(&subject);

    assert_eq!(
        probe.expect_msg(Duration::from_millis(50)).unwrap(),
        subject.as_any()
    );
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_expect_terminated_observes_already_stopped_actor() {
    let kit = ActorSystemTestKit::new("test-probe-expect-terminated-stopped")
        .expect("system should build");
    let probe = kit
        .create_probe::<AnyActorRef>("probe")
        .expect("probe should spawn");
    let subject = kit
        .system()
        .spawn("subject", Props::new(|| UnitActor))
        .expect("subject should spawn");

    kit.system().stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));

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
fn test_probe_expect_terminated_within_uses_shared_deadline() {
    let kit = ActorSystemTestKit::new("test-probe-expect-terminated-within")
        .expect("system should build");
    let probe = kit
        .create_probe::<AnyActorRef>("probe")
        .expect("probe should spawn");
    let subject = kit
        .system()
        .spawn("subject", Props::new(|| UnitActor))
        .expect("subject should spawn");

    let observed = probe
        .within(Duration::from_millis(50), |probe, scope| {
            kit.system().stop(&subject);
            probe.expect_terminated_within(&subject, scope)
        })
        .expect("termination should be observed inside shared deadline");

    assert_eq!(observed, subject.as_any());
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_expect_terminated_within_reports_unexpected_actor() {
    let kit = ActorSystemTestKit::new("test-probe-unexpected-terminated-within")
        .expect("system should build");
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
        .within(Duration::from_millis(50), |probe, scope| {
            probe.expect_terminated_within(&expected, scope)
        })
        .expect_err("probe should report unexpected terminated actor");
    assert!(matches!(
        error,
        WithinError::Assertion(ProbeError::UnexpectedMessage {
            expected: expected_path,
            actual
        }) if expected_path == expected.path().to_string() && actual == other.path().to_string()
    ));
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
fn test_probe_stop_notifies_watchers() {
    let kit = ActorSystemTestKit::new("test-probe-stop").expect("system should build");
    let subject = kit
        .create_probe::<&'static str>("subject")
        .expect("subject probe should spawn");
    let watcher = kit
        .create_probe::<AnyActorRef>("watcher")
        .expect("watcher probe should spawn");

    watcher
        .watch_terminated(&subject.actor_ref())
        .expect("probe watch should register");
    subject.stop();

    assert_eq!(
        watcher.expect_msg(Duration::from_millis(50)).unwrap(),
        subject.actor_ref().as_any()
    );
    assert_eq!(subject.expect_stopped(Duration::from_secs(1)), Ok(()));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_expect_stopped_reports_timeout() {
    let kit = ActorSystemTestKit::new("test-probe-stop-timeout").expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");

    let error = probe
        .expect_stopped(Duration::from_millis(5))
        .expect_err("live probe should not report stopped");

    assert!(matches!(
        error,
        ProbeError::StopTimeout { actor, .. } if actor == probe.actor_ref().path().to_string()
    ));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

struct UnitActor;

impl Actor for UnitActor {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }
}
