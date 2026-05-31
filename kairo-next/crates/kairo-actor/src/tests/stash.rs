use super::*;

enum StashProbeMsg {
    Work(usize),
    Open,
    UnstashOne,
    TryStash {
        value: usize,
        reply_to: mpsc::Sender<Result<(), ActorError>>,
    },
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
            StashProbeMsg::TryStash { value, reply_to } => reply_to
                .send(ctx.stash(StashProbeMsg::Work(value)))
                .map_err(|error| ActorError::Message(error.to_string())),
            StashProbeMsg::Get(reply_to) => reply_to
                .send(self.values.clone())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }
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
