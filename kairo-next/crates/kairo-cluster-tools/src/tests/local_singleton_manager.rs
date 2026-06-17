use super::*;

#[derive(Clone)]
enum LocalSingletonProbeMsg {
    Stop,
    Ping(ActorRef<&'static str>),
}

struct LocalSingletonProbe {
    started: ActorRef<&'static str>,
    stopped: ActorRef<&'static str>,
}

impl Actor for LocalSingletonProbe {
    type Msg = LocalSingletonProbeMsg;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        let _ = self.started.tell("started");
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        let _ = self.stopped.tell("stopped");
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            LocalSingletonProbeMsg::Stop => ctx.stop(ctx.myself())?,
            LocalSingletonProbeMsg::Ping(reply_to) => {
                let _ = reply_to.tell("pong");
            }
        }
        Ok(())
    }
}

#[test]
fn local_singleton_manager_spawns_child_when_oldest() {
    let node_a = node("local-singleton-a", 1);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let kit = ActorSystemTestKit::new("local-singleton-start").unwrap();
    let started = kit.create_probe::<&'static str>("started").unwrap();
    let stopped = kit.create_probe::<&'static str>("stopped").unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let singleton_reply = kit
        .create_probe::<Option<ActorRef<LocalSingletonProbeMsg>>>("singleton-ref")
        .unwrap();
    let ping_reply = kit.create_probe::<&'static str>("ping").unwrap();
    let state = kit
        .create_probe::<LocalSingletonManagerSnapshot>("state")
        .unwrap();
    let manager = kit
        .system()
        .spawn(
            "local-singleton-manager",
            LocalSingletonManagerActor::<LocalSingletonProbe>::props(
                node_a.clone(),
                "singleton",
                {
                    let started = started.actor_ref();
                    let stopped = stopped.actor_ref();
                    move || {
                        let started = started.clone();
                        let stopped = stopped.clone();
                        Props::new(move || LocalSingletonProbe { started, stopped })
                    }
                },
                LocalSingletonProbeMsg::Stop,
            ),
        )
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::ApplyInitialObservation {
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
    started
        .expect_msg_eq("started", Duration::from_millis(500))
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::GetSingleton {
            reply_to: singleton_reply.actor_ref(),
        })
        .unwrap();
    let singleton = singleton_reply
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .expect("singleton child should be available");
    singleton
        .tell(LocalSingletonProbeMsg::Ping(ping_reply.actor_ref()))
        .unwrap();
    ping_reply
        .expect_msg_eq("pong", Duration::from_millis(500))
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(
        snapshot.state,
        SingletonManagerState::Oldest {
            singleton_running: true,
        }
    );
    assert_eq!(snapshot.self_node, node_a);
    assert!(snapshot.singleton_path.is_some());
    stopped.expect_no_msg(Duration::from_millis(100)).unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_singleton_manager_stops_child_before_handover_done() {
    let node_a = node("local-singleton-oldest", 1);
    let node_b = node("local-singleton-new", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let kit = ActorSystemTestKit::new("local-singleton-handover").unwrap();
    let started = kit.create_probe::<&'static str>("started").unwrap();
    let stopped = kit.create_probe::<&'static str>("stopped").unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let state = kit
        .create_probe::<LocalSingletonManagerSnapshot>("state")
        .unwrap();
    let manager = kit
        .system()
        .spawn(
            "local-singleton-manager",
            LocalSingletonManagerActor::<LocalSingletonProbe>::props(
                node_a,
                "singleton",
                {
                    let started = started.actor_ref();
                    let stopped = stopped.actor_ref();
                    move || {
                        let started = started.clone();
                        let stopped = stopped.clone();
                        Props::new(move || LocalSingletonProbe { started, stopped })
                    }
                },
                LocalSingletonProbeMsg::Stop,
            ),
        )
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::ApplyInitialObservation {
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
    started
        .expect_msg_eq("started", Duration::from_millis(500))
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::ApplyOldestChange {
            change: SingletonOldestChange::OldestChanged(Some(node_b.clone())),
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendTakeOverFromMe { to: node_b.clone() }],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::HandOverToMe {
            from: node_b.clone(),
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![
                SingletonManagerEffect::SendHandOverInProgress { to: node_b.clone() },
                SingletonManagerEffect::StopSingleton,
            ],
            Duration::from_millis(500),
        )
        .unwrap();
    stopped
        .expect_msg_eq("stopped", Duration::from_millis(500))
        .unwrap();

    let mut end_snapshot = None;
    for _ in 0..100 {
        manager
            .tell(LocalSingletonManagerMsg::GetState {
                reply_to: state.actor_ref(),
            })
            .unwrap();
        let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
        if snapshot.state == SingletonManagerState::End {
            end_snapshot = Some(snapshot);
            break;
        }
    }
    let snapshot = end_snapshot.expect("singleton manager should finish handover");
    assert!(snapshot.singleton_path.is_none());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_singleton_manager_automatic_timer_retries_handover_until_progress() {
    let node_a = node("local-singleton-auto-retry-a", 1);
    let node_b = node("local-singleton-auto-retry-b", 2);
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
    let (kit, time) = ActorSystemTestKit::with_manual_time("local-singleton-auto-retry").unwrap();
    let started = kit.create_probe::<&'static str>("started").unwrap();
    let stopped = kit.create_probe::<&'static str>("stopped").unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let manager = kit
        .system()
        .spawn(
            "local-singleton-manager",
            LocalSingletonManagerActor::<LocalSingletonProbe>::props_with_effect_sink(
                node_b.clone(),
                "singleton",
                {
                    let started = started.actor_ref();
                    let stopped = stopped.actor_ref();
                    move || {
                        let started = started.clone();
                        let stopped = stopped.clone();
                        Props::new(move || LocalSingletonProbe { started, stopped })
                    }
                },
                LocalSingletonProbeMsg::Stop,
                settings,
                effects.actor_ref(),
            ),
        )
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: None,
        })
        .unwrap();
    manager
        .tell(LocalSingletonManagerMsg::ApplyOldestChange {
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
        .tell(LocalSingletonManagerMsg::HandOverInProgress {
            from: node_a,
            reply_to: None,
        })
        .unwrap();
    time.advance(retry_interval);
    effects.expect_no_msg(Duration::from_millis(50)).unwrap();
    started.expect_no_msg(Duration::from_millis(50)).unwrap();

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_singleton_manager_starts_child_when_previous_oldest_is_removed() {
    let node_a = node("local-singleton-remove-a", 1);
    let node_b = node("local-singleton-remove-b", 2);
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
        ActorSystemTestKit::with_manual_time("local-singleton-remove-previous").unwrap();
    let started = kit.create_probe::<&'static str>("started").unwrap();
    let stopped = kit.create_probe::<&'static str>("stopped").unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let state = kit
        .create_probe::<LocalSingletonManagerSnapshot>("state")
        .unwrap();
    let manager = kit
        .system()
        .spawn(
            "local-singleton-manager",
            LocalSingletonManagerActor::<LocalSingletonProbe>::props_with_effect_sink(
                node_b.clone(),
                "singleton",
                {
                    let started = started.actor_ref();
                    let stopped = stopped.actor_ref();
                    move || {
                        let started = started.clone();
                        let stopped = stopped.clone();
                        Props::new(move || LocalSingletonProbe { started, stopped })
                    }
                },
                LocalSingletonProbeMsg::Stop,
                settings,
                effects.actor_ref(),
            ),
        )
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: None,
        })
        .unwrap();
    manager
        .tell(LocalSingletonManagerMsg::ApplyOldestChange {
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
        .tell(LocalSingletonManagerMsg::MarkRemoved {
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
    started
        .expect_msg_eq("started", Duration::from_millis(500))
        .unwrap();
    time.advance(retry_interval);
    effects.expect_no_msg(Duration::from_millis(50)).unwrap();
    stopped.expect_no_msg(Duration::from_millis(50)).unwrap();

    manager
        .tell(LocalSingletonManagerMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(snapshot.self_node, node_b);
    assert_eq!(
        snapshot.state,
        SingletonManagerState::Oldest {
            singleton_running: true,
        }
    );
    assert_eq!(snapshot.removed_members, vec![node_a]);
    assert!(snapshot.singleton_path.is_some());

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_singleton_manager_automatic_timer_retries_takeover_until_handover_starts() {
    let node_a = node("local-singleton-auto-takeover-a", 1);
    let node_b = node("local-singleton-auto-takeover-b", 2);
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
        ActorSystemTestKit::with_manual_time("local-singleton-auto-takeover").unwrap();
    let started = kit.create_probe::<&'static str>("started").unwrap();
    let stopped = kit.create_probe::<&'static str>("stopped").unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let manager = kit
        .system()
        .spawn(
            "local-singleton-manager",
            LocalSingletonManagerActor::<LocalSingletonProbe>::props_with_effect_sink(
                node_a,
                "singleton",
                {
                    let started = started.actor_ref();
                    let stopped = stopped.actor_ref();
                    move || {
                        let started = started.clone();
                        let stopped = stopped.clone();
                        Props::new(move || LocalSingletonProbe { started, stopped })
                    }
                },
                LocalSingletonProbeMsg::Stop,
                settings,
                effects.actor_ref(),
            ),
        )
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::ApplyInitialObservation {
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
    started
        .expect_msg_eq("started", Duration::from_millis(500))
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::ApplyOldestChange {
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
        .tell(LocalSingletonManagerMsg::HandOverToMe {
            from: node_b.clone(),
            reply_to: None,
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![
                SingletonManagerEffect::SendHandOverInProgress { to: node_b.clone() },
                SingletonManagerEffect::StopSingleton,
            ],
            Duration::from_millis(500),
        )
        .unwrap();
    stopped
        .expect_msg_eq("stopped", Duration::from_millis(500))
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendHandOverDone { to: node_b }],
            Duration::from_millis(500),
        )
        .unwrap();
    time.advance(retry_interval);
    effects.expect_no_msg(Duration::from_millis(50)).unwrap();

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_singleton_manager_stops_child_when_new_oldest_is_removed() {
    let node_a = node("local-singleton-remove-new-old", 1);
    let node_b = node("local-singleton-remove-new-new", 2);
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
        ActorSystemTestKit::with_manual_time("local-singleton-remove-new-oldest").unwrap();
    let started = kit.create_probe::<&'static str>("started").unwrap();
    let stopped = kit.create_probe::<&'static str>("stopped").unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let state = kit
        .create_probe::<LocalSingletonManagerSnapshot>("state")
        .unwrap();
    let manager = kit
        .system()
        .spawn(
            "local-singleton-manager",
            LocalSingletonManagerActor::<LocalSingletonProbe>::props_with_effect_sink(
                node_a.clone(),
                "singleton",
                {
                    let started = started.actor_ref();
                    let stopped = stopped.actor_ref();
                    move || {
                        let started = started.clone();
                        let stopped = stopped.clone();
                        Props::new(move || LocalSingletonProbe { started, stopped })
                    }
                },
                LocalSingletonProbeMsg::Stop,
                settings,
                effects.actor_ref(),
            ),
        )
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::ApplyInitialObservation {
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
    started
        .expect_msg_eq("started", Duration::from_millis(500))
        .unwrap();
    manager
        .tell(LocalSingletonManagerMsg::ApplyOldestChange {
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
        .tell(LocalSingletonManagerMsg::MarkRemoved {
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
    stopped
        .expect_msg_eq("stopped", Duration::from_millis(500))
        .unwrap();
    time.advance(retry_interval);
    effects.expect_no_msg(Duration::from_millis(50)).unwrap();

    manager
        .tell(LocalSingletonManagerMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(snapshot.self_node, node_a);
    assert_eq!(
        snapshot.state,
        SingletonManagerState::Younger {
            previous_oldest: Vec::new(),
        }
    );
    assert_eq!(snapshot.removed_members, vec![node_b]);
    assert!(snapshot.singleton_path.is_none());

    kit.shutdown(Duration::from_secs(1)).unwrap();
}
