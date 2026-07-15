use super::*;

#[test]
fn test_probe_fish_for_message_collects_until_complete() {
    let kit = ActorSystemTestKit::new("test-probe-fish-complete").expect("system should build");
    let probe = kit.create_probe::<u8>("probe").expect("probe should spawn");

    for message in [1, 2, 3] {
        probe
            .actor_ref()
            .tell(message)
            .expect("probe tell should enqueue");
    }

    let seen = probe
        .fish_for_message(Duration::from_millis(50), |message| match message {
            3 => FishingOutcome::Complete,
            _ => FishingOutcome::Continue,
        })
        .expect("fishing should complete");

    assert_eq!(seen, vec![1, 2, 3]);
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_fish_for_message_can_ignore_messages() {
    let kit = ActorSystemTestKit::new("test-probe-fish-ignore").expect("system should build");
    let probe = kit.create_probe::<u8>("probe").expect("probe should spawn");

    for message in [1, 2, 3] {
        probe
            .actor_ref()
            .tell(message)
            .expect("probe tell should enqueue");
    }

    let seen = probe
        .fish_for_message(Duration::from_millis(50), |message| match message {
            1 => FishingOutcome::ContinueAndIgnore,
            3 => FishingOutcome::Complete,
            _ => FishingOutcome::Continue,
        })
        .expect("fishing should complete");

    assert_eq!(seen, vec![2, 3]);
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_fish_for_message_reports_failure() {
    let kit = ActorSystemTestKit::new("test-probe-fish-fail").expect("system should build");
    let probe = kit.create_probe::<u8>("probe").expect("probe should spawn");

    probe
        .actor_ref()
        .tell(1)
        .expect("probe tell should enqueue");

    let error = probe
        .fish_for_message(Duration::from_millis(50), |_| {
            FishingOutcome::Fail("bad message".to_string())
        })
        .expect_err("fishing should fail");

    assert_eq!(error, ProbeError::FishingFailed("bad message".to_string()));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_fish_for_message_reports_collected_timeout_count() {
    let kit = ActorSystemTestKit::new("test-probe-fish-timeout").expect("system should build");
    let probe = kit.create_probe::<u8>("probe").expect("probe should spawn");

    probe
        .actor_ref()
        .tell(1)
        .expect("probe tell should enqueue");

    let error = probe
        .fish_for_message(Duration::from_millis(50), |_| FishingOutcome::Continue)
        .expect_err("fishing should time out");

    assert!(matches!(
        error,
        ProbeError::FishTimeout {
            seen: 1,
            ignored: 0,
            ..
        }
    ));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn test_probe_fish_for_message_reports_ignored_timeout_count() {
    let kit =
        ActorSystemTestKit::new("test-probe-fish-timeout-ignored").expect("system should build");
    let probe = kit.create_probe::<u8>("probe").expect("probe should spawn");

    for message in [1, 2] {
        probe
            .actor_ref()
            .tell(message)
            .expect("probe tell should enqueue");
    }

    let error = probe
        .fish_for_message(Duration::from_millis(50), |message| match message {
            1 => FishingOutcome::ContinueAndIgnore,
            _ => FishingOutcome::Continue,
        })
        .expect_err("fishing should time out");

    assert!(matches!(
        error,
        ProbeError::FishTimeout {
            seen: 1,
            ignored: 1,
            ..
        }
    ));
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}
