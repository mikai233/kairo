use std::sync::mpsc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorResult, ActorSystem, Context, Props, Signal};

enum ScopedTaskMsg {
    StartAndFail {
        release: mpsc::Receiver<()>,
        send_rejected: mpsc::Sender<bool>,
        stale_delivered: mpsc::Sender<()>,
    },
    PipeAndFail {
        release: mpsc::Receiver<()>,
        stale_delivered: mpsc::Sender<()>,
    },
    TaskCompleted {
        stale_delivered: mpsc::Sender<()>,
    },
    Ping(mpsc::Sender<()>),
}

struct ScopedTaskActor {
    pre_restart: mpsc::Sender<()>,
}

impl Actor for ScopedTaskActor {
    type Msg = ScopedTaskMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ScopedTaskMsg::StartAndFail {
                release,
                send_rejected,
                stale_delivered,
            } => {
                ctx.spawn_task(move |myself| {
                    let _ = release.recv();
                    let result = myself.tell(ScopedTaskMsg::TaskCompleted { stale_delivered });
                    let _ = send_rejected.send(result.is_err());
                })?;
                Err(ActorError::Message("boom".to_string()))
            }
            ScopedTaskMsg::PipeAndFail {
                release,
                stale_delivered,
            } => {
                ctx.pipe_to_self(
                    move || {
                        let _ = release.recv();
                        Ok::<(), ()>(())
                    },
                    move |_| ScopedTaskMsg::TaskCompleted { stale_delivered },
                )?;
                Err(ActorError::Message("boom".to_string()))
            }
            ScopedTaskMsg::TaskCompleted { stale_delivered } => stale_delivered
                .send(())
                .map_err(|error| ActorError::Message(error.to_string())),
            ScopedTaskMsg::Ping(reply_to) => reply_to
                .send(())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }

    fn signal(&mut self, _ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        if let Signal::PreRestart = signal {
            self.pre_restart
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }
}

#[test]
fn spawn_task_send_is_rejected_after_owner_restart() {
    let system = ActorSystem::builder("tasks").build().unwrap();
    let (pre_restart_tx, pre_restart_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "task-owner",
            Props::restartable(move || ScopedTaskActor {
                pre_restart: pre_restart_tx.clone(),
            }),
        )
        .unwrap();
    let (release_tx, release_rx) = mpsc::channel();
    let (send_rejected_tx, send_rejected_rx) = mpsc::channel();
    let (stale_delivered_tx, stale_delivered_rx) = mpsc::channel();

    actor
        .tell(ScopedTaskMsg::StartAndFail {
            release: release_rx,
            send_rejected: send_rejected_tx,
            stale_delivered: stale_delivered_tx,
        })
        .unwrap();
    pre_restart_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    release_tx.send(()).unwrap();

    assert!(
        send_rejected_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
    );
    assert!(
        stale_delivered_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );

    let (ping_tx, ping_rx) = mpsc::channel();
    actor.tell(ScopedTaskMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

#[test]
fn pipe_to_self_completion_is_rejected_after_owner_restart() {
    let system = ActorSystem::builder("tasks").build().unwrap();
    let (pre_restart_tx, pre_restart_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "pipe-owner",
            Props::restartable(move || ScopedTaskActor {
                pre_restart: pre_restart_tx.clone(),
            }),
        )
        .unwrap();
    let (release_tx, release_rx) = mpsc::channel();
    let (stale_delivered_tx, stale_delivered_rx) = mpsc::channel();

    actor
        .tell(ScopedTaskMsg::PipeAndFail {
            release: release_rx,
            stale_delivered: stale_delivered_tx,
        })
        .unwrap();
    pre_restart_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    release_tx.send(()).unwrap();

    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    let dead_letters = system.dead_letters().records();
    assert_eq!(dead_letters[0].recipient(), actor.path());
    assert_eq!(dead_letters[0].reason(), "actor task is cancelled");
    assert!(
        stale_delivered_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );

    let (ping_tx, ping_rx) = mpsc::channel();
    actor.tell(ScopedTaskMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();
}
