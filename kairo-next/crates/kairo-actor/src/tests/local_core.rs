use super::*;

#[test]
fn spawned_actor_receives_messages_in_tell_order() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    counter.tell(CounterMsg::Increment).unwrap();
    counter.tell(CounterMsg::Increment).unwrap();
    counter.tell(CounterMsg::Get(reply_tx)).unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
}

#[test]
fn actor_system_builder_configures_dispatcher_throughput() {
    let system = ActorSystem::builder("test")
        .dispatcher_throughput(2)
        .build()
        .unwrap();

    assert_eq!(system.dispatcher_settings().throughput(), 2);
}

#[test]
fn actor_system_builder_configures_mailbox_capacity() {
    let system = ActorSystem::builder("test")
        .mailbox_capacity(1)
        .build()
        .unwrap();

    assert_eq!(system.mailbox_settings().user_capacity(), Some(1));
}

#[test]
fn actor_system_builder_rejects_zero_dispatcher_throughput() {
    let error = ActorSystem::builder("test")
        .dispatcher_throughput(0)
        .build()
        .unwrap_err();

    assert!(matches!(error, ActorError::InvalidThroughput));
}

#[test]
fn actor_system_builder_rejects_zero_mailbox_capacity() {
    let error = ActorSystem::builder("test")
        .mailbox_capacity(0)
        .build()
        .unwrap_err();

    assert!(matches!(error, ActorError::InvalidMailboxCapacity));
}

#[test]
fn bounded_mailbox_overflow_rejects_send_and_records_dead_letter() {
    struct Blocked {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    impl Actor for Blocked {
        type Msg = u8;

        fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
            self.entered
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))?;
            self.release
                .recv()
                .map_err(|error| ActorError::Message(error.to_string()))?;
            Ok(())
        }

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
            Ok(())
        }
    }

    let system = ActorSystem::builder("test")
        .mailbox_capacity(1)
        .build()
        .unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "blocked",
            Props::new(move || Blocked {
                entered: entered_tx,
                release: release_rx,
            }),
        )
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    actor.tell(1).unwrap();
    let error = actor
        .tell(2)
        .expect_err("bounded mailbox should reject overflow");

    assert_eq!(error.reason(), "actor mailbox is full");
    assert_eq!(error.into_message(), 2);
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    assert_eq!(
        system.dead_letters().records()[0].reason(),
        "actor mailbox is full"
    );

    release_tx.send(()).unwrap();
    system.terminate(Duration::from_secs(1)).unwrap();
}

fn send_to_recipient<R>(recipient: &R, message: CounterMsg)
where
    R: Recipient<CounterMsg>,
{
    recipient.tell(message).unwrap();
}

#[test]
fn actor_ref_and_ignore_ref_are_recipients() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();
    let ignore = IgnoreRef::new();
    let (reply_tx, reply_rx) = mpsc::channel();

    send_to_recipient(&counter, CounterMsg::Increment);
    send_to_recipient(&ignore, CounterMsg::Increment);
    counter.tell(CounterMsg::Get(reply_tx)).unwrap();

    assert_eq!(ignore.path().as_str(), "kairo://local/ignore");
    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
}

#[test]
fn duplicate_live_actor_name_is_rejected() {
    let system = ActorSystem::builder("test").build().unwrap();
    let _counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();

    let error = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap_err();

    assert!(matches!(error, ActorError::DuplicateName(name) if name == "counter"));
}

#[test]
fn user_actor_names_follow_path_element_rules() {
    let system = ActorSystem::builder("test").build().unwrap();
    let valid = system
        .spawn("worker-1_.*+:@&=,!~';%20", Props::new(|| Noop))
        .unwrap();

    assert!(valid.path().as_str().contains("/worker-1_.*+:@&=,!~';%20#"));

    for invalid in [
        "",
        "$reserved",
        "bad/name",
        "bad#name",
        "bad name",
        "naive?",
        "naiveä",
        "bad%",
        "bad%zz",
    ] {
        let error = system.spawn(invalid, Props::new(|| Noop)).unwrap_err();
        assert!(matches!(error, ActorError::InvalidName(name) if name == invalid));
    }
}

#[test]
fn stop_prevents_later_user_message_delivery() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();

    counter.tell(CounterMsg::Stop).unwrap();

    let mut rejected = None;
    for _ in 0..100 {
        match counter.tell(CounterMsg::Increment) {
            Ok(()) => thread::sleep(Duration::from_millis(5)),
            Err(error) => {
                rejected = Some(error);
                break;
            }
        }
    }

    let error = rejected.expect("message sent after stop should be rejected");
    assert_eq!(error.reason(), "actor is stopped");
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );

    let records = system.dead_letters().records();
    assert_eq!(records[0].recipient(), counter.path());
    assert_eq!(records[0].reason(), "actor is stopped");
}

#[test]
fn missing_actor_ref_sends_to_dead_letters() {
    let system = ActorSystem::builder("test").build().unwrap();
    let missing: ActorRef<CounterMsg> = system.missing_ref("kairo://test/user/missing#404");

    let error = missing.tell(CounterMsg::Increment).unwrap_err();

    assert_eq!(error.reason(), "actor does not exist");
    assert!(missing.is_stopped());
    assert!(missing.wait_for_stop(Duration::from_secs(1)));
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    let records = system.dead_letters().records();
    assert_eq!(records[0].recipient(), missing.path());
    assert_eq!(records[0].reason(), "actor does not exist");
}

#[test]
fn actor_system_resolves_live_local_ref_by_exact_typed_path() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();
    let path = counter.path().to_string();
    let resolved: ActorRef<CounterMsg> = system
        .resolve_local(&path)
        .expect("live local actor should resolve by typed path");
    let (reply_tx, reply_rx) = mpsc::channel();

    resolved.tell(CounterMsg::Increment).unwrap();
    counter.tell(CounterMsg::Get(reply_tx)).unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    assert!(system.resolve_local::<()>(&path).is_none());

    counter.tell(CounterMsg::Stop).unwrap();
    assert!(counter.wait_for_stop(Duration::from_secs(1)));
    assert!(system.resolve_local::<CounterMsg>(&path).is_none());

    let missing: ActorRef<CounterMsg> = system.resolve_local_or_missing(path);
    let error = missing.tell(CounterMsg::Increment).unwrap_err();

    assert_eq!(error.reason(), "actor does not exist");
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
}

#[test]
fn actor_system_stop_wakes_idle_actor() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (stopped_tx, stopped_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "probe",
            Props::new(move || StopProbe {
                stopped: stopped_tx,
            }),
        )
        .unwrap();

    system.stop(&actor);

    stopped_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(actor.is_stopped());
}

#[test]
fn stopped_actor_name_can_be_reused_with_new_incarnation() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (first_stopped_tx, first_stopped_rx) = mpsc::channel();
    let first = system
        .spawn(
            "probe",
            Props::new(move || StopProbe {
                stopped: first_stopped_tx,
            }),
        )
        .unwrap();
    let first_path = first.path().clone();

    system.stop(&first);
    first_stopped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    let (second_stopped_tx, _second_stopped_rx) = mpsc::channel();
    let second = system
        .spawn(
            "probe",
            Props::new(move || StopProbe {
                stopped: second_stopped_tx,
            }),
        )
        .unwrap();

    assert_ne!(&first_path, second.path());
    assert!(first_path.as_str().contains("/user/probe#"));
    assert!(second.path().as_str().contains("/user/probe#"));
}

#[test]
fn system_stop_drains_queued_user_messages_to_dead_letters() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (release_tx, release_rx) = mpsc::channel();
    let received = Arc::new(AtomicU64::new(0));
    let actor = system
        .spawn(
            "blocked",
            Props::new({
                let received = Arc::clone(&received);
                move || BlockingStart {
                    release: release_rx,
                    received,
                }
            }),
        )
        .unwrap();

    actor.tell(()).unwrap();
    actor.tell(()).unwrap();
    system.stop(&actor);
    release_tx.send(()).unwrap();

    assert!(
        system
            .dead_letters()
            .wait_for_len(2, Duration::from_secs(1))
    );
    assert_eq!(received.load(Ordering::Relaxed), 0);
    assert_eq!(system.dead_letters().records()[0].recipient(), actor.path());
}

#[test]
fn actor_system_terminate_stops_top_level_actors() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (stopped_tx, stopped_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "probe",
            Props::new(move || StopProbe {
                stopped: stopped_tx,
            }),
        )
        .unwrap();

    system.terminate(Duration::from_secs(1)).unwrap();

    stopped_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(actor.is_stopped());
    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    assert!(system.is_terminating());
    assert!(system.is_terminated());
}

#[test]
fn actor_system_terminate_stops_system_actors() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (user_stopped_tx, user_stopped_rx) = mpsc::channel();
    let (system_stopped_tx, system_stopped_rx) = mpsc::channel();
    let user_actor = system
        .spawn(
            "user-worker",
            Props::new(move || StopProbe {
                stopped: user_stopped_tx,
            }),
        )
        .unwrap();
    let system_actor = system
        .spawn_system(
            "system-worker",
            Props::new(move || StopProbe {
                stopped: system_stopped_tx,
            }),
        )
        .unwrap();

    system.terminate(Duration::from_secs(1)).unwrap();

    user_stopped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    system_stopped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(user_actor.wait_for_stop(Duration::from_secs(1)));
    assert!(system_actor.wait_for_stop(Duration::from_secs(1)));
    assert!(system.is_terminated());
}

#[test]
fn actor_system_provider_exposes_guardian_refs_and_resolves_local_paths() {
    let system = ActorSystem::builder("test").build().unwrap();
    let provider = system.provider();
    let actor = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();

    assert_eq!(provider.root_guardian().path().as_str(), "kairo://test");
    assert_eq!(
        provider.user_guardian().path().as_str(),
        "kairo://test/user"
    );
    assert_eq!(
        provider.system_guardian().path().as_str(),
        "kairo://test/system"
    );
    assert_eq!(
        provider.temp_guardian().path().as_str(),
        "kairo://test/temp"
    );
    assert_eq!(
        provider.dead_letters().path().as_str(),
        "kairo://test/deadLetters"
    );

    let resolved = provider.resolve(actor.path());
    assert!(resolved.is_local());
    assert_eq!(resolved.path(), actor.path());
}

#[test]
fn actor_system_spawn_system_places_framework_actors_under_system_guardian() {
    let system = ActorSystem::builder("test").build().unwrap();
    let provider = system.provider();

    let system_actor = system
        .spawn_system("remote-watch", Props::new(|| Noop))
        .unwrap();
    let user_actor = system.spawn("remote-watch", Props::new(|| Noop)).unwrap();

    assert!(
        system_actor
            .path()
            .as_str()
            .starts_with("kairo://test/system/remote-watch#")
    );
    assert!(
        user_actor
            .path()
            .as_str()
            .starts_with("kairo://test/user/remote-watch#")
    );
    assert_eq!(
        system_actor.path().parent(),
        Some(provider.system_guardian().path().clone())
    );
    assert!(provider.resolve(system_actor.path()).is_local());
    assert!(
        system
            .spawn_system("remote-watch", Props::new(|| Noop))
            .is_err()
    );
}

#[test]
fn local_actor_ref_provider_allocates_unique_temp_paths_under_temp_root() {
    let system = ActorSystem::builder("test").build().unwrap();
    let provider = system.provider();

    let first = provider.temp_path("ask");
    let second = provider.temp_path("ask");

    assert_eq!(
        first.parent(),
        Some(provider.temp_guardian().path().clone())
    );
    assert_eq!(
        second.parent(),
        Some(provider.temp_guardian().path().clone())
    );
    assert_ne!(first, second);
    assert_eq!(first.name(), Some("ask$0"));
    assert_eq!(second.name(), Some("ask$1"));
}

#[test]
fn local_actor_ref_provider_distinguishes_missing_and_non_local_paths() {
    let system = ActorSystem::builder("test").build().unwrap();
    let provider = system.provider();

    let missing = ActorPath::new("kairo://test/user/missing#9");
    let foreign = ActorPath::new("kairo://other@127.0.0.1:2552/user/worker#1");

    let missing_result = provider.resolve(&missing);
    assert!(missing_result.is_missing());
    assert_eq!(missing_result.path(), &missing);

    let foreign_result = provider.resolve(&foreign);
    assert!(foreign_result.is_non_local());
    assert_eq!(foreign_result.path(), &foreign);
}

#[test]
fn actor_system_terminate_rejects_later_spawns() {
    let system = ActorSystem::builder("test").build().unwrap();

    system.terminate(Duration::from_secs(1)).unwrap();
    let error = system.spawn("late", Props::new(|| Noop)).unwrap_err();

    assert!(matches!(error, ActorError::SystemTerminating));
}

#[test]
fn actor_system_terminate_times_out_waiting_for_blocked_actor_start() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (_release_tx, release_rx) = mpsc::channel();
    let received = Arc::new(AtomicU64::new(0));
    let _actor = system
        .spawn(
            "blocked",
            Props::new({
                let received = Arc::clone(&received);
                move || BlockingStart {
                    release: release_rx,
                    received,
                }
            }),
        )
        .unwrap();

    let error = system.terminate(Duration::from_millis(10)).unwrap_err();

    assert!(matches!(error, ActorError::TerminationTimeout));
    assert!(system.is_terminating());
    assert!(!system.is_terminated());
}
