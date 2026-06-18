use super::*;

enum BackoffChildMsg {
    Stop,
    Record(u64),
}

struct BackoffChild {
    generation: u64,
    started: mpsc::Sender<u64>,
    received: mpsc::Sender<(u64, u64)>,
}

impl Actor for BackoffChild {
    type Msg = BackoffChildMsg;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.started
            .send(self.generation)
            .map_err(|error| ActorError::Message(error.to_string()))
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            BackoffChildMsg::Stop => ctx.stop(ctx.myself())?,
            BackoffChildMsg::Record(value) => self
                .received
                .send((self.generation, value))
                .map_err(|error| ActorError::Message(error.to_string()))?,
        }
        Ok(())
    }
}

#[test]
fn backoff_supervisor_restarts_child_after_delay() {
    let manual = ManualScheduler::new();
    let system = ActorSystem::builder("test")
        .manual_scheduler(manual.clone())
        .build()
        .unwrap();
    let settings =
        BackoffSupervisorSettings::new(Duration::from_millis(50), Duration::from_millis(200))
            .unwrap();
    let next_generation = Arc::new(AtomicU64::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let (received_tx, _received_rx) = mpsc::channel();
    let child_factory = {
        let next_generation = Arc::clone(&next_generation);
        move || {
            let generation = next_generation.fetch_add(1, Ordering::Relaxed) + 1;
            let started = started_tx.clone();
            let received = received_tx.clone();
            Props::new(move || BackoffChild {
                generation,
                started,
                received,
            })
        }
    };
    let supervisor = system
        .spawn(
            "backoff",
            BackoffSupervisor::<BackoffChild>::on_stop("child", child_factory, settings),
        )
        .unwrap();
    let (current_tx, current_rx) = mpsc::channel();
    let current_probe = system
        .spawn(
            "current-child-probe",
            Props::new(move || ChannelProbe {
                observed: current_tx,
            }),
        )
        .unwrap();
    let (count_tx, count_rx) = mpsc::channel();
    let count_probe = system
        .spawn(
            "restart-count-probe",
            Props::new(move || ChannelProbe { observed: count_tx }),
        )
        .unwrap();

    assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    supervisor
        .tell(BackoffSupervisorMsg::GetCurrentChild {
            reply_to: current_probe.clone(),
        })
        .unwrap();
    let first_child = current_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .child()
        .unwrap();
    let first_path = first_child.path().clone();

    first_child.tell(BackoffChildMsg::Stop).unwrap();
    assert!(first_child.wait_for_stop(Duration::from_secs(1)));

    assert_eq!(
        wait_for_restart_count(&supervisor, &count_probe, &count_rx, 1),
        1
    );

    manual.advance(Duration::from_millis(49));
    assert!(started_rx.recv_timeout(Duration::from_millis(100)).is_err());

    manual.advance(Duration::from_millis(1));
    assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
    supervisor
        .tell(BackoffSupervisorMsg::GetCurrentChild {
            reply_to: current_probe,
        })
        .unwrap();
    let second_child = current_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .child()
        .unwrap();

    assert_ne!(second_child.path(), &first_path);
}

#[test]
fn backoff_supervisor_stops_after_max_restarts() {
    let manual = ManualScheduler::new();
    let system = ActorSystem::builder("test-backoff-max-restarts")
        .manual_scheduler(manual.clone())
        .build()
        .unwrap();
    let settings =
        BackoffSupervisorSettings::new(Duration::from_millis(10), Duration::from_millis(10))
            .unwrap()
            .with_manual_reset()
            .with_max_restarts(1);
    let next_generation = Arc::new(AtomicU64::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let (received_tx, _received_rx) = mpsc::channel();
    let child_factory = {
        let next_generation = Arc::clone(&next_generation);
        move || {
            let generation = next_generation.fetch_add(1, Ordering::Relaxed) + 1;
            let started = started_tx.clone();
            let received = received_tx.clone();
            Props::new(move || BackoffChild {
                generation,
                started,
                received,
            })
        }
    };
    let supervisor = system
        .spawn(
            "backoff",
            BackoffSupervisor::<BackoffChild>::on_stop("child", child_factory, settings),
        )
        .unwrap();
    let (current_tx, current_rx) = mpsc::channel();
    let current_probe = system
        .spawn(
            "current-child-probe",
            Props::new(move || ChannelProbe {
                observed: current_tx,
            }),
        )
        .unwrap();
    let (count_tx, count_rx) = mpsc::channel();
    let count_probe = system
        .spawn(
            "restart-count-probe",
            Props::new(move || ChannelProbe { observed: count_tx }),
        )
        .unwrap();

    assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    supervisor
        .tell(BackoffSupervisorMsg::GetCurrentChild {
            reply_to: current_probe.clone(),
        })
        .unwrap();
    let first_child = current_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .child()
        .unwrap();

    first_child.tell(BackoffChildMsg::Stop).unwrap();
    assert!(first_child.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(
        wait_for_restart_count(&supervisor, &count_probe, &count_rx, 1),
        1
    );

    manual.advance(Duration::from_millis(10));
    assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);

    supervisor
        .tell(BackoffSupervisorMsg::GetCurrentChild {
            reply_to: current_probe,
        })
        .unwrap();
    let second_child = current_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .child()
        .unwrap();
    second_child.tell(BackoffChildMsg::Stop).unwrap();
    assert!(second_child.wait_for_stop(Duration::from_secs(1)));
    assert!(supervisor.wait_for_stop(Duration::from_secs(1)));

    manual.advance(Duration::from_millis(10));
    assert!(started_rx.recv_timeout(Duration::from_millis(50)).is_err());
}

#[test]
fn backoff_supervisor_dead_letters_messages_during_backoff() {
    let manual = ManualScheduler::new();
    let system = ActorSystem::builder("test-backoff-dead-letters")
        .manual_scheduler(manual.clone())
        .build()
        .unwrap();
    let settings =
        BackoffSupervisorSettings::new(Duration::from_millis(50), Duration::from_millis(50))
            .unwrap();
    let next_generation = Arc::new(AtomicU64::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let (received_tx, received_rx) = mpsc::channel();
    let child_factory = {
        let next_generation = Arc::clone(&next_generation);
        move || {
            let generation = next_generation.fetch_add(1, Ordering::Relaxed) + 1;
            let started = started_tx.clone();
            let received = received_tx.clone();
            Props::new(move || BackoffChild {
                generation,
                started,
                received,
            })
        }
    };
    let supervisor = system
        .spawn(
            "backoff",
            BackoffSupervisor::<BackoffChild>::on_stop("child", child_factory, settings),
        )
        .unwrap();
    let (current_tx, current_rx) = mpsc::channel();
    let current_probe = system
        .spawn(
            "current-child-probe",
            Props::new(move || ChannelProbe {
                observed: current_tx,
            }),
        )
        .unwrap();
    let (count_tx, count_rx) = mpsc::channel();
    let count_probe = system
        .spawn(
            "restart-count-probe",
            Props::new(move || ChannelProbe { observed: count_tx }),
        )
        .unwrap();

    assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    supervisor
        .tell(BackoffSupervisorMsg::GetCurrentChild {
            reply_to: current_probe,
        })
        .unwrap();
    let first_child = current_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .child()
        .unwrap();
    let first_path = first_child.path().clone();

    first_child.tell(BackoffChildMsg::Stop).unwrap();
    assert!(first_child.wait_for_stop(Duration::from_secs(1)));

    assert_eq!(
        wait_for_restart_count(&supervisor, &count_probe, &count_rx, 1),
        1
    );

    supervisor
        .tell(BackoffSupervisorMsg::Tell(BackoffChildMsg::Record(7)))
        .unwrap();
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    assert!(received_rx.recv_timeout(Duration::from_millis(50)).is_err());

    let records = system.dead_letters().records();
    assert_eq!(records[0].recipient(), &first_path);
    assert_eq!(records[0].reason(), "backoff child is stopped");
    assert_eq!(
        records[0].message_type(),
        std::any::type_name::<BackoffChildMsg>()
    );

    manual.advance(Duration::from_millis(50));
    assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
    assert!(received_rx.recv_timeout(Duration::from_millis(50)).is_err());

    supervisor
        .tell(BackoffSupervisorMsg::Tell(BackoffChildMsg::Record(8)))
        .unwrap();
    assert_eq!(
        received_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        (2, 8)
    );
}

fn wait_for_restart_count(
    supervisor: &ActorRef<BackoffSupervisorMsg<BackoffChildMsg>>,
    count_probe: &ActorRef<RestartCount>,
    count_rx: &mpsc::Receiver<RestartCount>,
    expected: u32,
) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        assert!(
            Instant::now() < deadline,
            "expected restart count {expected} before timeout"
        );
        supervisor
            .tell(BackoffSupervisorMsg::GetRestartCount {
                reply_to: count_probe.clone(),
            })
            .unwrap();
        let remaining = deadline.saturating_duration_since(Instant::now());
        let count = count_rx
            .recv_timeout(remaining.min(Duration::from_millis(50)))
            .unwrap()
            .count();
        if count == expected {
            return count;
        }
        assert!(
            Instant::now() < deadline,
            "expected restart count {expected}; last observed {count}"
        );
    }
}
