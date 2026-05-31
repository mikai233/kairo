use super::*;

enum BackoffChildMsg {
    Stop,
}

struct BackoffChild {
    generation: u64,
    started: mpsc::Sender<u64>,
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
    let child_factory = {
        let next_generation = Arc::clone(&next_generation);
        move || {
            let generation = next_generation.fetch_add(1, Ordering::Relaxed) + 1;
            let started = started_tx.clone();
            Props::new(move || BackoffChild {
                generation,
                started,
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

    let mut restart_count = None;
    for _ in 0..100 {
        supervisor
            .tell(BackoffSupervisorMsg::GetRestartCount {
                reply_to: count_probe.clone(),
            })
            .unwrap();
        let count = count_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .count();
        if count == 1 {
            restart_count = Some(count);
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(restart_count, Some(1));

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
