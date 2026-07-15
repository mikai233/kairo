use super::*;

#[test]
fn dispatcher_never_runs_two_turns_for_one_actor_concurrently() {
    const PRODUCERS: usize = 8;
    const MESSAGES_PER_PRODUCER: usize = 250;
    const TOTAL: usize = PRODUCERS * MESSAGES_PER_PRODUCER;

    struct GuardedActor {
        active: Arc<std::sync::atomic::AtomicBool>,
        concurrent_turn_seen: Arc<std::sync::atomic::AtomicBool>,
        processed: usize,
        done: mpsc::Sender<()>,
    }

    impl Actor for GuardedActor {
        type Msg = ();

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, (): ()) -> ActorResult {
            if self.active.swap(true, Ordering::AcqRel) {
                self.concurrent_turn_seen.store(true, Ordering::Release);
            }
            thread::yield_now();
            self.processed += 1;
            self.active.store(false, Ordering::Release);
            if self.processed == TOTAL {
                self.done
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            Ok(())
        }
    }

    let system = ActorSystem::builder("dispatcher-single-turn")
        .dispatcher_workers(4)
        .dispatcher_throughput(3)
        .build()
        .unwrap();
    let active = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let concurrent_turn_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (done_tx, done_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "guarded",
            Props::new({
                let active = Arc::clone(&active);
                let concurrent_turn_seen = Arc::clone(&concurrent_turn_seen);
                move || GuardedActor {
                    active,
                    concurrent_turn_seen,
                    processed: 0,
                    done: done_tx,
                }
            }),
        )
        .unwrap();

    let producers: Vec<_> = (0..PRODUCERS)
        .map(|_| {
            let actor = actor.clone();
            thread::spawn(move || {
                for _ in 0..MESSAGES_PER_PRODUCER {
                    actor.tell(()).unwrap();
                }
            })
        })
        .collect();
    for producer in producers {
        producer.join().unwrap();
    }

    done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(!concurrent_turn_seen.load(Ordering::Acquire));
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn dispatcher_throughput_reschedules_busy_actor_behind_other_mailboxes() {
    struct BusyActor {
        processed: Arc<AtomicU64>,
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    impl Actor for BusyActor {
        type Msg = ();

        fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
            self.entered
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))?;
            self.release
                .recv()
                .map_err(|error| ActorError::Message(error.to_string()))
        }

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, (): ()) -> ActorResult {
            self.processed.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    struct ProbeActor(mpsc::Sender<usize>, Arc<AtomicU64>);

    impl Actor for ProbeActor {
        type Msg = ();

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, (): ()) -> ActorResult {
            self.0
                .send(self.1.load(Ordering::Acquire) as usize)
                .map_err(|error| ActorError::Message(error.to_string()))
        }
    }

    const BUSY_MESSAGES: usize = 1_000;
    let system = ActorSystem::builder("dispatcher-fairness")
        .dispatcher_workers(1)
        .dispatcher_throughput(1)
        .build()
        .unwrap();
    let processed = Arc::new(AtomicU64::new(0));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let busy = system
        .spawn(
            "busy",
            Props::new({
                let processed = Arc::clone(&processed);
                move || BusyActor {
                    processed,
                    entered: entered_tx,
                    release: release_rx,
                }
            }),
        )
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    for _ in 0..BUSY_MESSAGES {
        busy.tell(()).unwrap();
    }
    let (probe_tx, probe_rx) = mpsc::channel();
    let probe = system
        .spawn(
            "probe",
            Props::new({
                let processed = Arc::clone(&processed);
                move || ProbeActor(probe_tx, processed)
            }),
        )
        .unwrap();
    probe.tell(()).unwrap();
    release_tx.send(()).unwrap();

    let busy_count_when_probe_ran = probe_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(busy_count_when_probe_ran < BUSY_MESSAGES);
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn dispatcher_reactivates_idle_mailbox_without_lost_wakeup() {
    struct Echo(mpsc::Sender<usize>);

    impl Actor for Echo {
        type Msg = usize;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, message: usize) -> ActorResult {
            self.0
                .send(message)
                .map_err(|error| ActorError::Message(error.to_string()))
        }
    }

    let system = ActorSystem::builder("dispatcher-wakeup")
        .dispatcher_workers(2)
        .build()
        .unwrap();
    let (echo_tx, echo_rx) = mpsc::channel();
    let echo = system
        .spawn("echo", Props::new(move || Echo(echo_tx)))
        .unwrap();

    for expected in 0..2_000 {
        echo.tell(expected).unwrap();
        assert_eq!(
            echo_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            expected
        );
    }
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn fixed_dispatcher_pool_starts_thousands_of_idle_actors() {
    struct Started(mpsc::Sender<()>);

    impl Actor for Started {
        type Msg = ();

        fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
            self.0
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))
        }

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, (): ()) -> ActorResult {
            Ok(())
        }
    }

    const ACTORS: usize = 2_000;
    let system = ActorSystem::builder("dispatcher-idle-actors")
        .dispatcher_workers(2)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let mut actors = Vec::with_capacity(ACTORS);
    for index in 0..ACTORS {
        let started_tx = started_tx.clone();
        actors.push(
            system
                .spawn(
                    format!("idle-{index}"),
                    Props::new(move || Started(started_tx)),
                )
                .unwrap(),
        );
    }
    drop(started_tx);
    for _ in 0..ACTORS {
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }

    system.terminate(Duration::from_secs(5)).unwrap();
    assert!(actors.iter().all(ActorRef::is_stopped));
}

#[test]
fn one_worker_dispatcher_cooperatively_stops_child_before_parent() {
    struct Child;

    impl Actor for Child {
        type Msg = ();

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, (): ()) -> ActorResult {
            Ok(())
        }
    }

    struct Parent(mpsc::Sender<()>);

    impl Actor for Parent {
        type Msg = ();

        fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
            ctx.spawn("child", Props::new(|| Child))?;
            self.0
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))
        }

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, (): ()) -> ActorResult {
            Ok(())
        }
    }

    let system = ActorSystem::builder("dispatcher-child-stop")
        .dispatcher_workers(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let parent = system
        .spawn("parent", Props::new(move || Parent(started_tx)))
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&parent);
    assert!(parent.wait_for_stop(Duration::from_secs(1)));
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn actor_factory_panic_stops_incarnation_without_losing_dispatcher_worker() {
    struct Echo(mpsc::Sender<()>);

    impl Actor for Echo {
        type Msg = ();

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, (): ()) -> ActorResult {
            self.0
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))
        }
    }

    let system = ActorSystem::builder("dispatcher-factory-panic")
        .dispatcher_workers(1)
        .build()
        .unwrap();
    let failed = system
        .spawn("echo", Props::new(|| -> Echo { panic!("factory failed") }))
        .unwrap();
    assert!(failed.wait_for_stop(Duration::from_secs(1)));

    let (echo_tx, echo_rx) = mpsc::channel();
    let replacement = system
        .spawn("echo", Props::new(move || Echo(echo_tx)))
        .unwrap();
    replacement.tell(()).unwrap();
    echo_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    system.terminate(Duration::from_secs(1)).unwrap();
}
