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
fn coordinated_shutdown_actor_termination_task_stops_actor() {
    let system = ActorSystem::builder("test").build().unwrap();
    let counter = system
        .spawn("counter", Props::new(|| Counter { value: 0 }))
        .unwrap();

    system
        .coordinated_shutdown()
        .add_actor_termination_task(
            PHASE_SERVICE_STOP,
            "stop-counter",
            counter.clone(),
            Some(CounterMsg::Stop),
            Duration::from_secs(1),
        )
        .unwrap();

    system.coordinated_shutdown().run("test").unwrap();

    assert!(counter.wait_for_stop(Duration::from_secs(1)));
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
