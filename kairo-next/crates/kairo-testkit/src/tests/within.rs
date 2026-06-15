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
