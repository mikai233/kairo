use super::*;

#[test]
fn singleton_manager_starts_immediately_when_self_is_safe_oldest() {
    let node_a = node("a", 1);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let mut manager = SingletonManagerRuntime::new(node_a);

    assert_eq!(
        manager.apply_initial_observation(observation),
        vec![SingletonManagerEffect::StartSingleton]
    );
    assert_eq!(
        manager.state(),
        &SingletonManagerState::Oldest {
            singleton_running: true,
        }
    );
}

#[test]
fn singleton_manager_requests_handover_before_becoming_oldest() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let mut manager = SingletonManagerRuntime::new(node_b.clone());
    assert!(manager.apply_initial_observation(observation).is_empty());

    assert_eq!(
        manager.apply_oldest_change(SingletonOldestChange::OldestChanged(Some(node_b.clone()))),
        vec![SingletonManagerEffect::SendHandOverToMe { to: node_a.clone() }]
    );
    assert_eq!(
        manager.state(),
        &SingletonManagerState::BecomingOldest {
            previous_oldest: vec![node_a.clone()],
            handover_started: false,
        }
    );

    assert!(manager.hand_over_in_progress(&node_a).is_empty());
    assert_eq!(
        manager.state(),
        &SingletonManagerState::BecomingOldest {
            previous_oldest: vec![node_a.clone()],
            handover_started: true,
        }
    );
    assert_eq!(
        manager.hand_over_done(&node_a),
        vec![SingletonManagerEffect::StartSingleton]
    );
    assert_eq!(
        manager.state(),
        &SingletonManagerState::Oldest {
            singleton_running: true,
        }
    );
}

#[test]
fn singleton_manager_starts_when_previous_oldest_is_removed() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Leaving, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let mut manager = SingletonManagerRuntime::new(node_b);
    assert!(manager.apply_initial_observation(observation).is_empty());

    assert!(manager.mark_removed(node_a).is_empty());
    assert_eq!(
        manager.apply_oldest_change(SingletonOldestChange::OldestChanged(Some(node("b", 2)))),
        vec![SingletonManagerEffect::StartSingleton]
    );
    assert_eq!(
        manager.state(),
        &SingletonManagerState::Oldest {
            singleton_running: true,
        }
    );
}

#[test]
fn singleton_manager_hands_over_when_oldest_changes_away() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let mut manager = SingletonManagerRuntime::new(node_a.clone());
    manager.apply_initial_observation(observation);

    assert_eq!(
        manager.apply_oldest_change(SingletonOldestChange::OldestChanged(Some(node_b.clone()))),
        vec![SingletonManagerEffect::SendTakeOverFromMe { to: node_b.clone() }]
    );
    assert_eq!(
        manager.hand_over_to_me(node_b.clone()),
        vec![
            SingletonManagerEffect::SendHandOverInProgress { to: node_b.clone() },
            SingletonManagerEffect::StopSingleton,
        ]
    );
    assert_eq!(
        manager.state(),
        &SingletonManagerState::HandingOver {
            singleton_running: true,
            handover_to: Some(node_b.clone()),
        }
    );

    assert_eq!(
        manager.singleton_terminated(),
        vec![SingletonManagerEffect::SendHandOverDone { to: node_b }]
    );
    assert_eq!(manager.state(), &SingletonManagerState::End);
}

#[test]
fn singleton_manager_hands_over_to_none_when_new_oldest_is_removed() {
    let node_a = node("new-oldest-removed-old", 1);
    let node_b = node("new-oldest-removed-new", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let mut manager = SingletonManagerRuntime::new(node_a);
    manager.apply_initial_observation(observation);

    assert_eq!(
        manager.apply_oldest_change(SingletonOldestChange::OldestChanged(Some(node_b.clone()))),
        vec![SingletonManagerEffect::SendTakeOverFromMe { to: node_b.clone() }]
    );
    assert_eq!(
        manager.mark_removed(node_b.clone()),
        vec![SingletonManagerEffect::StopSingleton]
    );
    assert_eq!(
        manager.state(),
        &SingletonManagerState::HandingOver {
            singleton_running: true,
            handover_to: None,
        }
    );
    assert!(manager.removed_members().contains(&node_b));

    assert!(manager.singleton_terminated().is_empty());
    assert_eq!(
        manager.state(),
        &SingletonManagerState::Younger {
            previous_oldest: Vec::new(),
        }
    );
}

#[test]
fn singleton_manager_retries_takeover_while_was_oldest() {
    let node_a = node("takeover-retry-old", 1);
    let node_b = node("takeover-retry-new", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let mut manager = SingletonManagerRuntime::new(node_a);
    manager.apply_initial_observation(observation);

    assert_eq!(
        manager.apply_oldest_change(SingletonOldestChange::OldestChanged(Some(node_b.clone()))),
        vec![SingletonManagerEffect::SendTakeOverFromMe { to: node_b.clone() }]
    );
    assert_eq!(
        manager.take_over_retry(),
        vec![SingletonManagerEffect::SendTakeOverFromMe { to: node_b.clone() }]
    );

    assert_eq!(
        manager.hand_over_to_me(node_b.clone()),
        vec![
            SingletonManagerEffect::SendHandOverInProgress { to: node_b },
            SingletonManagerEffect::StopSingleton,
        ]
    );
    assert!(manager.take_over_retry().is_empty());
}

#[test]
fn singleton_manager_responds_to_takeover_only_when_becoming_or_oldest() {
    let node_a = node("takeover-a", 1);
    let node_b = node("takeover-b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let mut manager = SingletonManagerRuntime::new(node_b.clone());
    assert!(manager.apply_initial_observation(observation).is_empty());
    assert!(manager.take_over_from_me(node_a.clone()).is_empty());

    assert_eq!(
        manager.apply_oldest_change(SingletonOldestChange::OldestChanged(Some(node_b))),
        vec![SingletonManagerEffect::SendHandOverToMe { to: node_a.clone() }]
    );
    assert_eq!(
        manager.take_over_from_me(node_a.clone()),
        vec![SingletonManagerEffect::SendHandOverToMe { to: node_a }]
    );
}

#[test]
fn singleton_manager_retries_handover_until_previous_oldest_confirms_progress() {
    let node_a = node("handover-retry-a", 1);
    let node_b = node("handover-retry-b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let mut manager = SingletonManagerRuntime::new(node_b.clone());
    assert!(manager.apply_initial_observation(observation).is_empty());
    assert_eq!(
        manager.apply_oldest_change(SingletonOldestChange::OldestChanged(Some(node_b))),
        vec![SingletonManagerEffect::SendHandOverToMe { to: node_a.clone() }]
    );

    assert_eq!(
        manager.hand_over_retry(),
        vec![SingletonManagerEffect::SendHandOverToMe { to: node_a.clone() }]
    );

    assert!(manager.hand_over_in_progress(&node_a).is_empty());
    assert!(manager.hand_over_retry().is_empty());
}

#[test]
fn singleton_manager_retry_is_ignored_outside_becoming_oldest() {
    let node_a = node("handover-retry-oldest", 1);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let mut manager = SingletonManagerRuntime::new(node_a);
    assert_eq!(
        manager.apply_initial_observation(observation),
        vec![SingletonManagerEffect::StartSingleton]
    );

    assert!(manager.hand_over_retry().is_empty());
}

#[test]
fn singleton_manager_actor_applies_initial_observation_in_mailbox_turn() {
    let node_a = node("singleton-actor-a", 1);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let kit = ActorSystemTestKit::new("singleton-manager-actor-initial").unwrap();
    let manager = kit
        .system()
        .spawn(
            "singleton-manager",
            SingletonManagerActor::props(node_a.clone()),
        )
        .unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let state = kit
        .create_probe::<SingletonManagerSnapshot>("state")
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::StartSingleton],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap(),
        SingletonManagerSnapshot {
            self_node: node_a,
            state: SingletonManagerState::Oldest {
                singleton_running: true,
            },
            removed_members: Vec::new(),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_manager_actor_retries_handover_in_mailbox_turn() {
    let node_a = node("singleton-actor-retry-a", 1);
    let node_b = node("singleton-actor-retry-b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let kit = ActorSystemTestKit::new("singleton-manager-actor-retry").unwrap();
    let manager = kit
        .system()
        .spawn(
            "singleton-manager",
            SingletonManagerActor::props(node_b.clone()),
        )
        .unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(Vec::new(), Duration::from_millis(500))
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyOldestChange {
            change: SingletonOldestChange::OldestChanged(Some(node_b)),
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendHandOverToMe { to: node_a.clone() }],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::HandOverRetry {
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendHandOverToMe { to: node_a.clone() }],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::HandOverInProgress {
            from: node_a,
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(Vec::new(), Duration::from_millis(500))
        .unwrap();

    manager
        .tell(SingletonManagerMsg::HandOverRetry {
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(Vec::new(), Duration::from_millis(500))
        .unwrap();

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_manager_actor_starts_when_previous_oldest_is_removed() {
    let node_a = node("singleton-actor-remove-a", 1);
    let node_b = node("singleton-actor-remove-b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let retry_interval = Duration::from_millis(25);
    let settings = SingletonManagerSettings::new(retry_interval).unwrap();
    let (kit, time) =
        ActorSystemTestKit::with_manual_time("singleton-manager-actor-remove").unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let state = kit
        .create_probe::<SingletonManagerSnapshot>("state")
        .unwrap();
    let manager = kit
        .system()
        .spawn(
            "singleton-manager",
            SingletonManagerActor::props_with_effect_sink(
                node_b.clone(),
                settings,
                effects.actor_ref(),
            ),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: None,
        })
        .unwrap();
    manager
        .tell(SingletonManagerMsg::ApplyOldestChange {
            change: SingletonOldestChange::OldestChanged(Some(node_b.clone())),
            reply_to: None,
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendHandOverToMe { to: node_a.clone() }],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::MarkRemoved {
            node: node_a.clone(),
            reply_to: None,
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::StartSingleton],
            Duration::from_millis(500),
        )
        .unwrap();
    time.advance(retry_interval);
    effects.expect_no_msg(Duration::from_millis(50)).unwrap();

    manager
        .tell(SingletonManagerMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap(),
        SingletonManagerSnapshot {
            self_node: node_b,
            state: SingletonManagerState::Oldest {
                singleton_running: true,
            },
            removed_members: vec![node_a],
        }
    );

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_manager_actor_automatic_timer_retries_handover_until_progress() {
    assert_eq!(
        SingletonManagerSettings::new(Duration::ZERO).unwrap_err(),
        SingletonManagerSettingsError::ZeroHandOverRetryInterval
    );

    let node_a = node("singleton-actor-auto-retry-a", 1);
    let node_b = node("singleton-actor-auto-retry-b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let retry_interval = Duration::from_millis(25);
    let settings = SingletonManagerSettings::new(retry_interval).unwrap();
    let (kit, time) =
        ActorSystemTestKit::with_manual_time("singleton-manager-actor-auto-retry").unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let manager = kit
        .system()
        .spawn(
            "singleton-manager",
            SingletonManagerActor::props_with_effect_sink(
                node_b.clone(),
                settings,
                effects.actor_ref(),
            ),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: None,
        })
        .unwrap();
    manager
        .tell(SingletonManagerMsg::ApplyOldestChange {
            change: SingletonOldestChange::OldestChanged(Some(node_b)),
            reply_to: None,
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendHandOverToMe { to: node_a.clone() }],
            Duration::from_millis(500),
        )
        .unwrap();

    time.advance(retry_interval);
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendHandOverToMe { to: node_a.clone() }],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::HandOverInProgress {
            from: node_a,
            reply_to: None,
        })
        .unwrap();
    time.advance(retry_interval);
    effects.expect_no_msg(Duration::from_millis(50)).unwrap();

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_manager_actor_automatic_timer_retries_takeover_until_handover_starts() {
    let node_a = node("singleton-actor-auto-takeover-a", 1);
    let node_b = node("singleton-actor-auto-takeover-b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let retry_interval = Duration::from_millis(25);
    let settings = SingletonManagerSettings::new(retry_interval).unwrap();
    let (kit, time) =
        ActorSystemTestKit::with_manual_time("singleton-manager-auto-takeover").unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let manager = kit
        .system()
        .spawn(
            "singleton-manager",
            SingletonManagerActor::props_with_effect_sink(node_a, settings, effects.actor_ref()),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: None,
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::StartSingleton],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyOldestChange {
            change: SingletonOldestChange::OldestChanged(Some(node_b.clone())),
            reply_to: None,
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendTakeOverFromMe { to: node_b.clone() }],
            Duration::from_millis(500),
        )
        .unwrap();

    time.advance(retry_interval);
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendTakeOverFromMe { to: node_b.clone() }],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::HandOverToMe {
            from: node_b,
            reply_to: None,
        })
        .unwrap();
    effects.expect_msg(Duration::from_millis(500)).unwrap();
    time.advance(retry_interval);
    effects.expect_no_msg(Duration::from_millis(50)).unwrap();

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_manager_actor_stops_when_new_oldest_is_removed() {
    let node_a = node("singleton-actor-remove-new-old", 1);
    let node_b = node("singleton-actor-remove-new-new", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let retry_interval = Duration::from_millis(25);
    let settings = SingletonManagerSettings::new(retry_interval).unwrap();
    let (kit, time) =
        ActorSystemTestKit::with_manual_time("singleton-manager-remove-new-oldest").unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let state = kit
        .create_probe::<SingletonManagerSnapshot>("state")
        .unwrap();
    let manager = kit
        .system()
        .spawn(
            "singleton-manager",
            SingletonManagerActor::props_with_effect_sink(
                node_a.clone(),
                settings,
                effects.actor_ref(),
            ),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: None,
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::StartSingleton],
            Duration::from_millis(500),
        )
        .unwrap();
    manager
        .tell(SingletonManagerMsg::ApplyOldestChange {
            change: SingletonOldestChange::OldestChanged(Some(node_b.clone())),
            reply_to: None,
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendTakeOverFromMe { to: node_b.clone() }],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::MarkRemoved {
            node: node_b.clone(),
            reply_to: None,
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::StopSingleton],
            Duration::from_millis(500),
        )
        .unwrap();
    time.advance(retry_interval);
    effects.expect_no_msg(Duration::from_millis(50)).unwrap();

    manager
        .tell(SingletonManagerMsg::SingletonTerminated { reply_to: None })
        .unwrap();
    manager
        .tell(SingletonManagerMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap(),
        SingletonManagerSnapshot {
            self_node: node_a,
            state: SingletonManagerState::Younger {
                previous_oldest: Vec::new(),
            },
            removed_members: vec![node_b],
        }
    );

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_manager_actor_runs_handover_protocol_messages_in_order() {
    let node_a = node("singleton-actor-a", 1);
    let node_b = node("singleton-actor-b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let kit = ActorSystemTestKit::new("singleton-manager-actor-handover").unwrap();
    let manager = kit
        .system()
        .spawn(
            "singleton-manager",
            SingletonManagerActor::props(node_b.clone()),
        )
        .unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let state = kit
        .create_probe::<SingletonManagerSnapshot>("state")
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(Vec::new(), Duration::from_millis(500))
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyOldestChange {
            change: SingletonOldestChange::OldestChanged(Some(node_b.clone())),
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendHandOverToMe { to: node_a.clone() }],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::HandOverInProgress {
            from: node_a.clone(),
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(Vec::new(), Duration::from_millis(500))
        .unwrap();

    manager
        .tell(SingletonManagerMsg::HandOverDone {
            from: node_a,
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::StartSingleton],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap().state,
        SingletonManagerState::Oldest {
            singleton_running: true,
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
