use super::*;

#[test]
fn await_assert_retries_until_assertion_succeeds() {
    let mut attempts = 0;

    let value = await_assert(Duration::from_millis(50), Duration::from_millis(1), || {
        attempts += 1;
        if attempts < 3 {
            Err("not yet")
        } else {
            Ok(attempts)
        }
    })
    .expect("assertion should eventually succeed");

    assert_eq!(value, 3);
    assert_eq!(attempts, 3);
}

#[test]
fn await_assert_reports_last_error_after_timeout() {
    let mut attempts = 0;

    let error = await_assert(Duration::ZERO, Duration::from_millis(1), || {
        attempts += 1;
        Err::<(), _>("still failing")
    })
    .expect_err("assertion should time out");

    assert_eq!(attempts, 1);
    assert_eq!(error.attempts(), 1);
    assert_eq!(error.last_error(), &"still failing");
}
