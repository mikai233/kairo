use super::*;

#[test]
fn coordinated_shutdown_runs_tasks_in_phase_order() {
    let system = ActorSystem::builder("test").build().unwrap();
    let shutdown = system.coordinated_shutdown();
    let (events_tx, events_rx) = mpsc::channel();

    shutdown
        .add_task(PHASE_SERVICE_UNBIND, "unbind", {
            let events = events_tx.clone();
            move || {
                events
                    .send("unbind")
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
        })
        .unwrap();
    shutdown
        .add_task(PHASE_SERVICE_STOP, "stop", move || {
            events_tx
                .send("stop")
                .map_err(|error| ActorError::Message(error.to_string()))
        })
        .unwrap();

    shutdown.run("test").unwrap();

    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "unbind"
    );
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "stop"
    );
    assert_eq!(shutdown.reason().as_deref(), Some("test"));
}

#[test]
fn coordinated_shutdown_run_from_starts_at_requested_phase() {
    let system = ActorSystem::builder("test").build().unwrap();
    let shutdown = system.coordinated_shutdown();
    let (events_tx, events_rx) = mpsc::channel();

    shutdown
        .add_task(PHASE_SERVICE_UNBIND, "unbind", {
            let events = events_tx.clone();
            move || {
                events
                    .send("unbind")
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
        })
        .unwrap();
    shutdown
        .add_task(PHASE_SERVICE_STOP, "stop", move || {
            events_tx
                .send("stop")
                .map_err(|error| ActorError::Message(error.to_string()))
        })
        .unwrap();

    shutdown.run_from("test", Some(PHASE_SERVICE_STOP)).unwrap();

    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "stop"
    );
    assert!(events_rx.recv_timeout(Duration::from_millis(50)).is_err());
    assert_eq!(shutdown.reason().as_deref(), Some("test"));
}

#[test]
fn coordinated_shutdown_run_from_rejects_unknown_phase() {
    let system = ActorSystem::builder("test").build().unwrap();
    let shutdown = system.coordinated_shutdown();

    let result = shutdown.run_from("test", Some("missing-phase"));

    assert!(matches!(
        result,
        Err(ActorError::UnknownShutdownPhase(phase)) if phase == "missing-phase"
    ));
    assert_eq!(shutdown.reason(), None);
}

#[test]
fn coordinated_shutdown_runs_same_phase_tasks_in_parallel_before_next_phase() {
    let system = ActorSystem::builder("test").build().unwrap();
    let shutdown = system.coordinated_shutdown();
    let (events_tx, events_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    shutdown
        .add_task(PHASE_SERVICE_STOP, "blocked", {
            let events = events_tx.clone();
            move || {
                events
                    .send("blocked-start")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                release_rx
                    .recv()
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                events
                    .send("blocked-done")
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
        })
        .unwrap();
    shutdown
        .add_task(PHASE_SERVICE_STOP, "peer", {
            let events = events_tx.clone();
            move || {
                events
                    .send("peer-start")
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
        })
        .unwrap();
    shutdown
        .add_task(PHASE_BEFORE_CLUSTER_SHUTDOWN, "next-phase", move || {
            events_tx
                .send("next-phase")
                .map_err(|error| ActorError::Message(error.to_string()))
        })
        .unwrap();

    let runner = shutdown.clone();
    let join = thread::spawn(move || runner.run("test"));

    let first = events_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let second = events_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let mut started = vec![first, second];
    started.sort_unstable();
    assert_eq!(started, ["blocked-start", "peer-start"]);
    assert!(events_rx.recv_timeout(Duration::from_millis(50)).is_err());

    release_tx.send(()).unwrap();
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "blocked-done"
    );
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "next-phase"
    );
    join.join().unwrap().unwrap();
}

#[test]
fn coordinated_shutdown_runs_only_once() {
    let system = ActorSystem::builder("test").build().unwrap();
    let shutdown = system.coordinated_shutdown();
    let ran = Arc::new(AtomicU64::new(0));

    shutdown
        .add_task(PHASE_SERVICE_STOP, "once", {
            let ran = Arc::clone(&ran);
            move || {
                ran.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        })
        .unwrap();

    shutdown.run("first").unwrap();
    shutdown.run("second").unwrap();

    assert_eq!(ran.load(Ordering::Relaxed), 1);
    assert_eq!(shutdown.reason().as_deref(), Some("first"));
}

#[test]
fn coordinated_shutdown_accepts_late_task_without_rerunning() {
    let system = ActorSystem::builder("test").build().unwrap();
    let shutdown = system.coordinated_shutdown();
    let ran = Arc::new(AtomicU64::new(0));

    shutdown.run("first").unwrap();
    shutdown
        .add_task(PHASE_SERVICE_STOP, "too-late", {
            let ran = Arc::clone(&ran);
            move || {
                ran.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        })
        .unwrap();

    shutdown.run("second").unwrap();

    assert_eq!(ran.load(Ordering::Relaxed), 0);
    assert_eq!(shutdown.reason().as_deref(), Some("first"));
}

#[test]
fn coordinated_shutdown_task_can_add_later_phase_task() {
    let system = ActorSystem::builder("test").build().unwrap();
    let shutdown = system.coordinated_shutdown();
    let (events_tx, events_rx) = mpsc::channel();

    shutdown
        .add_task(PHASE_SERVICE_UNBIND, "register-later", {
            let shutdown = shutdown.clone();
            let events = events_tx.clone();
            move || {
                events
                    .send("early")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                shutdown.add_task(PHASE_SERVICE_STOP, "late", move || {
                    events_tx
                        .send("late")
                        .map_err(|error| ActorError::Message(error.to_string()))
                })
            }
        })
        .unwrap();

    shutdown.run("test").unwrap();

    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "early"
    );
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "late"
    );
}

#[test]
fn coordinated_shutdown_cancellable_task_skips_cancelled_registration() {
    let system = ActorSystem::builder("test").build().unwrap();
    let shutdown = system.coordinated_shutdown();
    let (events_tx, events_rx) = mpsc::channel();

    let cancelled = shutdown
        .add_cancellable_task(PHASE_SERVICE_STOP, "cancelled", {
            let events = events_tx.clone();
            move || {
                events
                    .send("cancelled")
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
        })
        .unwrap();
    shutdown
        .add_task(PHASE_SERVICE_STOP, "active", move || {
            events_tx
                .send("active")
                .map_err(|error| ActorError::Message(error.to_string()))
        })
        .unwrap();

    assert!(cancelled.cancel());
    assert!(cancelled.is_cancelled());
    shutdown.run("test").unwrap();

    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "active"
    );
    assert!(events_rx.recv_timeout(Duration::from_millis(50)).is_err());
}

#[test]
fn coordinated_shutdown_duplicate_task_names_are_distinct_registrations() {
    let system = ActorSystem::builder("test").build().unwrap();
    let shutdown = system.coordinated_shutdown();
    let ran = Arc::new(AtomicU64::new(0));

    for _ in 0..3 {
        shutdown
            .add_task(PHASE_SERVICE_STOP, "same-name", {
                let ran = Arc::clone(&ran);
                move || {
                    ran.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            })
            .unwrap();
    }

    shutdown.run("test").unwrap();

    assert_eq!(ran.load(Ordering::Relaxed), 3);
}

#[test]
fn coordinated_shutdown_earlier_phase_can_cancel_later_phase_task() {
    let system = ActorSystem::builder("test").build().unwrap();
    let shutdown = system.coordinated_shutdown();
    let (events_tx, events_rx) = mpsc::channel();

    let later = shutdown
        .add_cancellable_task(PHASE_SERVICE_STOP, "later", {
            let events = events_tx.clone();
            move || {
                events
                    .send("later")
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
        })
        .unwrap();
    shutdown
        .add_task(PHASE_SERVICE_UNBIND, "cancel-later", move || {
            assert!(later.cancel());
            events_tx
                .send("cancelled")
                .map_err(|error| ActorError::Message(error.to_string()))
        })
        .unwrap();

    shutdown.run("test").unwrap();

    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "cancelled"
    );
    assert!(events_rx.recv_timeout(Duration::from_millis(50)).is_err());
}

#[test]
fn coordinated_shutdown_task_handle_cannot_cancel_running_task() {
    let system = ActorSystem::builder("test").build().unwrap();
    let shutdown = system.coordinated_shutdown();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();

    let handle = shutdown
        .add_cancellable_task(PHASE_SERVICE_STOP, "running", move || {
            started_tx
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))?;
            release_rx
                .recv()
                .map_err(|error| ActorError::Message(error.to_string()))?;
            finished_tx
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))
        })
        .unwrap();
    let shutdown_runner = shutdown.clone();
    let join = std::thread::spawn(move || shutdown_runner.run("test"));

    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(handle.is_running_or_done());
    assert!(
        !handle.cancel(),
        "a coordinated-shutdown task cannot be cancelled after it starts running"
    );
    assert!(!handle.is_cancelled());

    release_tx.send(()).unwrap();
    join.join().unwrap().unwrap();
    finished_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(handle.is_running_or_done());
}

#[test]
fn coordinated_shutdown_actor_termination_task_stops_actor() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();

    add_counter_stop_task(&system, "stop-counter", counter.clone());

    system.coordinated_shutdown().run("test").unwrap();

    assert!(counter.wait_for_stop(Duration::from_secs(1)));
}

#[test]
fn coordinated_shutdown_actor_termination_task_stops_system_actor() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn_system("system-counter", Props::new(|| Counter { value: 0 }))
        .unwrap();

    add_counter_stop_task(&system, "stop-system-counter", counter.clone());

    system.coordinated_shutdown().run("test").unwrap();

    assert!(counter.wait_for_stop(Duration::from_secs(1)));
}

fn add_counter_stop_task(system: &ActorSystem, task_name: &str, counter: ActorRef<CounterMsg>) {
    system
        .coordinated_shutdown()
        .add_actor_termination_task(
            PHASE_SERVICE_STOP,
            task_name,
            counter,
            Some(CounterMsg::Stop),
            Duration::from_secs(1),
        )
        .unwrap();
}

#[test]
fn coordinated_shutdown_actor_termination_task_without_message_waits_only() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();

    system
        .coordinated_shutdown()
        .add_actor_termination_task(
            PHASE_SERVICE_STOP,
            "wait-counter",
            counter.clone(),
            None,
            Duration::from_millis(20),
        )
        .unwrap();

    let result = system.coordinated_shutdown().run("test");

    assert!(
        matches!(result, Err(ActorError::ShutdownTaskFailed(reason)) if reason.contains("timed out"))
    );
    assert!(!counter.is_stopped());
    system.stop(&counter);
    assert!(counter.wait_for_stop(Duration::from_secs(1)));
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinated_shutdown_actor_termination_task_without_message_waits_only_for_system_actor() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn_system("system-counter", Props::new(|| Counter { value: 0 }))
        .unwrap();

    system
        .coordinated_shutdown()
        .add_actor_termination_task(
            PHASE_SERVICE_STOP,
            "wait-system-counter",
            counter.clone(),
            None,
            Duration::from_millis(20),
        )
        .unwrap();

    let result = system.coordinated_shutdown().run("test");

    assert!(
        matches!(result, Err(ActorError::ShutdownTaskFailed(reason)) if reason.contains("timed out"))
    );
    assert!(!counter.is_stopped());
    system.stop(&counter);
    assert!(counter.wait_for_stop(Duration::from_secs(1)));
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinated_shutdown_actor_termination_task_without_message_accepts_already_stopped_actor() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();
    system.stop(&counter);
    assert!(counter.wait_for_stop(Duration::from_secs(1)));

    system
        .coordinated_shutdown()
        .add_actor_termination_task(
            PHASE_SERVICE_STOP,
            "wait-counter",
            counter,
            None,
            Duration::from_secs(1),
        )
        .unwrap();

    system.coordinated_shutdown().run("test").unwrap();
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinated_shutdown_actor_termination_task_without_message_accepts_stopped_system_actor() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn_system("system-counter", Props::new(|| Counter { value: 0 }))
        .unwrap();
    system.stop(&counter);
    assert!(counter.wait_for_stop(Duration::from_secs(1)));

    system
        .coordinated_shutdown()
        .add_actor_termination_task(
            PHASE_SERVICE_STOP,
            "wait-system-counter",
            counter,
            None,
            Duration::from_secs(1),
        )
        .unwrap();

    system.coordinated_shutdown().run("test").unwrap();
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn actor_system_run_coordinated_shutdown_runs_tasks_then_terminates() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (task_tx, task_rx) = mpsc::channel();
    let (stopped_tx, stopped_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "probe",
            Props::new(move || StopProbe {
                stopped: stopped_tx,
            }),
        )
        .unwrap();

    system
        .coordinated_shutdown()
        .add_task(PHASE_SERVICE_STOP, "task", move || {
            task_tx
                .send("task")
                .map_err(|error| ActorError::Message(error.to_string()))
        })
        .unwrap();

    system
        .run_coordinated_shutdown("test", Duration::from_secs(1))
        .unwrap();

    assert_eq!(
        task_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "task"
    );
    stopped_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(actor.is_stopped());
    assert!(system.is_terminated());
}
