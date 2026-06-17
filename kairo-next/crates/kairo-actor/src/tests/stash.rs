use super::*;

enum StashProbeMsg {
    Work(usize),
    Open,
    UnstashOne,
    Fail,
    StopThenTryStash {
        reply_to: mpsc::Sender<(Result<(), ActorError>, usize)>,
    },
    TryStash {
        value: usize,
        reply_to: mpsc::Sender<Result<(), ActorError>>,
    },
    Inspect(mpsc::Sender<(usize, Option<usize>, bool)>),
    ClearAndInspect(mpsc::Sender<(usize, Option<usize>, bool)>),
    Get(mpsc::Sender<Vec<usize>>),
}

struct StashProbe {
    open: bool,
    values: Vec<usize>,
}

impl Actor for StashProbe {
    type Msg = StashProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            StashProbeMsg::Work(value) if self.open => {
                self.values.push(value);
                Ok(())
            }
            StashProbeMsg::Work(value) => ctx.stash(StashProbeMsg::Work(value)),
            StashProbeMsg::Open => {
                self.open = true;
                ctx.unstash_all()
            }
            StashProbeMsg::UnstashOne => {
                self.open = true;
                ctx.unstash(1)
            }
            StashProbeMsg::Fail => Err(ActorError::Message("boom".to_string())),
            StashProbeMsg::StopThenTryStash { reply_to } => {
                ctx.stop(ctx.myself())?;
                let result = ctx.stash(StashProbeMsg::Work(99));
                reply_to
                    .send((result, ctx.stash_len()))
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            StashProbeMsg::TryStash { value, reply_to } => reply_to
                .send(ctx.stash(StashProbeMsg::Work(value)))
                .map_err(|error| ActorError::Message(error.to_string())),
            StashProbeMsg::Inspect(reply_to) => reply_to
                .send((ctx.stash_len(), ctx.stash_capacity(), ctx.is_stash_full()))
                .map_err(|error| ActorError::Message(error.to_string())),
            StashProbeMsg::ClearAndInspect(reply_to) => {
                ctx.clear_stash();
                reply_to
                    .send((ctx.stash_len(), ctx.stash_capacity(), ctx.is_stash_full()))
                    .map_err(|error| ActorError::Message(error.to_string()))
            }
            StashProbeMsg::Get(reply_to) => reply_to
                .send(self.values.clone())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }
}

#[test]
fn stash_rejects_messages_after_self_stop_is_requested() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "stash",
            Props::new(|| StashProbe {
                open: false,
                values: Vec::new(),
            })
            .with_stash_capacity(8),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(StashProbeMsg::StopThenTryStash { reply_to: reply_tx })
        .unwrap();

    let (result, stash_len) = reply_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(stash_len, 0);
    assert!(matches!(result, Err(ActorError::ActorStopping { .. })));
    assert!(actor.wait_for_stop(Duration::from_secs(1)));
}

#[test]
fn stash_requires_explicit_capacity() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "stash",
            Props::new(|| StashProbe {
                open: false,
                values: Vec::new(),
            }),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(StashProbeMsg::TryStash {
            value: 1,
            reply_to: reply_tx,
        })
        .unwrap();

    assert!(matches!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Err(ActorError::StashDisabled)
    ));
}

#[test]
fn stash_rejects_messages_after_capacity_is_reached() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "stash",
            Props::new(|| StashProbe {
                open: false,
                values: Vec::new(),
            })
            .with_stash_capacity(1),
        )
        .unwrap();
    let (first_tx, first_rx) = mpsc::channel();
    let (second_tx, second_rx) = mpsc::channel();

    actor
        .tell(StashProbeMsg::TryStash {
            value: 1,
            reply_to: first_tx,
        })
        .unwrap();
    actor
        .tell(StashProbeMsg::TryStash {
            value: 2,
            reply_to: second_tx,
        })
        .unwrap();

    assert!(
        first_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .is_ok()
    );
    assert!(matches!(
        second_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Err(ActorError::StashFull { capacity: 1 })
    ));
}

#[test]
fn unstash_all_replays_stashed_messages_before_later_mailbox_messages() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "stash",
            Props::new(|| StashProbe {
                open: false,
                values: Vec::new(),
            })
            .with_stash_capacity(8),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor.tell(StashProbeMsg::Work(1)).unwrap();
    actor.tell(StashProbeMsg::Work(2)).unwrap();
    actor.tell(StashProbeMsg::Open).unwrap();
    actor.tell(StashProbeMsg::Work(3)).unwrap();
    actor.tell(StashProbeMsg::Get(reply_tx)).unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![1, 2, 3]
    );
}

#[test]
fn unstash_can_replay_a_limited_batch() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "stash",
            Props::new(|| StashProbe {
                open: false,
                values: Vec::new(),
            })
            .with_stash_capacity(8),
        )
        .unwrap();
    let (first_tx, first_rx) = mpsc::channel();
    let (second_tx, second_rx) = mpsc::channel();

    actor.tell(StashProbeMsg::Work(1)).unwrap();
    actor.tell(StashProbeMsg::Work(2)).unwrap();
    actor.tell(StashProbeMsg::UnstashOne).unwrap();
    actor.tell(StashProbeMsg::Get(first_tx)).unwrap();
    actor.tell(StashProbeMsg::Open).unwrap();
    actor.tell(StashProbeMsg::Get(second_tx)).unwrap();

    assert_eq!(
        first_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![1]
    );
    assert_eq!(
        second_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![1, 2]
    );
}

#[test]
fn clear_stash_drops_buffered_messages_and_updates_inspection_state() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "stash",
            Props::new(|| StashProbe {
                open: false,
                values: Vec::new(),
            })
            .with_stash_capacity(2),
        )
        .unwrap();
    let (first_tx, first_rx) = mpsc::channel();
    let (second_tx, second_rx) = mpsc::channel();
    let (full_tx, full_rx) = mpsc::channel();
    let (cleared_tx, cleared_rx) = mpsc::channel();
    let (values_tx, values_rx) = mpsc::channel();

    actor
        .tell(StashProbeMsg::TryStash {
            value: 1,
            reply_to: first_tx,
        })
        .unwrap();
    actor
        .tell(StashProbeMsg::TryStash {
            value: 2,
            reply_to: second_tx,
        })
        .unwrap();
    actor.tell(StashProbeMsg::Inspect(full_tx)).unwrap();
    actor
        .tell(StashProbeMsg::ClearAndInspect(cleared_tx))
        .unwrap();
    actor.tell(StashProbeMsg::Open).unwrap();
    actor.tell(StashProbeMsg::Get(values_tx)).unwrap();

    assert!(
        first_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .is_ok()
    );
    assert!(
        second_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .is_ok()
    );
    assert_eq!(
        full_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        (2, Some(2), true)
    );
    assert_eq!(
        cleared_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        (0, Some(2), false)
    );
    assert_eq!(
        values_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Vec::<usize>::new()
    );
}

#[test]
fn restart_replays_stashed_messages_before_later_mailbox_messages() {
    let generation = Arc::new(AtomicU64::new(0));
    let props_generation = Arc::clone(&generation);
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "stash",
            Props::restartable(move || {
                let open = props_generation.fetch_add(1, Ordering::SeqCst) > 0;
                StashProbe {
                    open,
                    values: Vec::new(),
                }
            })
            .with_stash_capacity(8),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor.tell(StashProbeMsg::Work(1)).unwrap();
    actor.tell(StashProbeMsg::Work(2)).unwrap();
    actor.tell(StashProbeMsg::Fail).unwrap();
    actor.tell(StashProbeMsg::Work(3)).unwrap();
    actor.tell(StashProbeMsg::Get(reply_tx)).unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![1, 2, 3]
    );
}

#[test]
fn resume_supervision_preserves_stashed_messages() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "stash",
            Props::new(|| StashProbe {
                open: false,
                values: Vec::new(),
            })
            .with_supervisor(SupervisorStrategy::Resume)
            .with_stash_capacity(8),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor.tell(StashProbeMsg::Work(1)).unwrap();
    actor.tell(StashProbeMsg::Work(2)).unwrap();
    actor.tell(StashProbeMsg::Fail).unwrap();
    actor.tell(StashProbeMsg::Open).unwrap();
    actor.tell(StashProbeMsg::Work(3)).unwrap();
    actor.tell(StashProbeMsg::Get(reply_tx)).unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![1, 2, 3]
    );
}

#[test]
fn stop_drains_stashed_messages_to_dead_letters() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "stash",
            Props::new(|| StashProbe {
                open: false,
                values: Vec::new(),
            })
            .with_stash_capacity(8),
        )
        .unwrap();
    let (inspect_tx, inspect_rx) = mpsc::channel();

    actor.tell(StashProbeMsg::Work(1)).unwrap();
    actor.tell(StashProbeMsg::Work(2)).unwrap();
    actor.tell(StashProbeMsg::Inspect(inspect_tx)).unwrap();
    assert_eq!(
        inspect_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        (2, Some(8), false)
    );

    system.stop(&actor);
    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    assert!(
        system
            .dead_letters()
            .wait_for_len(2, Duration::from_secs(1))
    );
    assert!(
        system
            .dead_letters()
            .records()
            .iter()
            .all(|record| record.recipient() == actor.path())
    );
}
