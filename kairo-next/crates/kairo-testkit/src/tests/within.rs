use super::*;

#[test]
fn within_passes_remaining_deadline_to_assertion() {
    let value = run_within(Duration::from_secs(1), |scope| {
        assert!(scope.remaining() <= scope.timeout());
        assert!(!scope.is_elapsed());
        Ok::<_, &'static str>(scope.remaining())
    })
    .expect("within block should complete");

    assert!(value <= Duration::from_secs(1));
}

#[test]
fn within_reports_assertion_error() {
    let error = run_within(Duration::from_secs(1), |_scope| {
        Err::<(), _>("assertion failed")
    })
    .expect_err("within should report assertion failure");

    assert_eq!(error, WithinError::Assertion("assertion failed"));
}

#[test]
fn within_reports_elapsed_block() {
    let error = run_within(Duration::ZERO, |_scope| Ok::<_, &'static str>(()))
        .expect_err("zero timeout should be elapsed after assertion");

    assert!(matches!(
        error,
        WithinError::Timeout {
            timeout,
            elapsed: _
        } if timeout == Duration::ZERO
    ));
}

#[test]
fn within_scoped_await_assert_retries_until_success() {
    let mut attempts = 0;

    let value = run_within(Duration::from_millis(50), |scope| {
        scope.await_assert(Duration::from_millis(1), || {
            attempts += 1;
            if attempts < 3 {
                Err("not ready")
            } else {
                Ok(attempts)
            }
        })
    })
    .expect("await assertion should complete inside within deadline");

    assert_eq!(value, 3);
    assert_eq!(attempts, 3);
}

#[test]
fn within_scoped_await_assert_reports_last_error_under_shared_deadline() {
    let mut attempts = 0;

    let error = run_within(Duration::ZERO, |scope| {
        scope.await_assert(Duration::from_millis(1), || {
            attempts += 1;
            Err::<(), _>("still waiting")
        })
    })
    .expect_err("await assertion should time out under within deadline");

    assert_eq!(attempts, 1);
    match error {
        WithinError::Assertion(error) => {
            assert_eq!(error.attempts(), 1);
            assert_eq!(error.last_error(), &"still waiting");
        }
        WithinError::Timeout { .. } => panic!("expected await assertion error"),
    }
}

#[test]
fn test_probe_within_uses_one_shared_deadline() {
    let kit = ActorSystemTestKit::new("test-probe-within").expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");

    probe
        .actor_ref()
        .tell("first")
        .expect("first tell should enqueue");
    probe
        .actor_ref()
        .tell("second")
        .expect("second tell should enqueue");

    let messages = probe
        .within(Duration::from_secs(1), |probe, scope| {
            let first = probe.expect_msg(scope.remaining())?;
            let second = probe.expect_msg(scope.remaining())?;
            Ok::<_, ProbeError>(vec![first, second])
        })
        .expect("probe assertions should complete within one deadline");

    assert_eq!(messages, vec!["first", "second"]);
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_within_helpers_use_shared_deadline() {
    let kit = ActorSystemTestKit::new("test-probe-within-helpers").expect("system should build");
    let probe = kit
        .create_probe::<&'static str>("probe")
        .expect("probe should spawn");

    for message in ["first", "second", "third", "done"] {
        probe
            .actor_ref()
            .tell(message)
            .expect("probe tell should enqueue");
    }

    let messages = probe
        .within(Duration::from_secs(1), |probe, scope| {
            let first = probe.expect_msg_eq_within("first", scope)?;
            let second =
                probe.expect_msg_matching_within(scope, |message| message.starts_with("sec"))?;
            let third = probe.receive_messages_within(1, scope)?;
            let mut fished = probe.fish_for_message_within(scope, |message| {
                if *message == "done" {
                    FishingOutcome::Complete
                } else {
                    FishingOutcome::Continue
                }
            })?;
            let done = fished
                .pop()
                .expect("fishing should include terminal message");
            Ok::<_, ProbeError>(vec![first, second, third[0], done])
        })
        .expect("probe helpers should complete under the shared deadline");

    assert_eq!(messages, vec!["first", "second", "third", "done"]);
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}
