use super::*;

enum TaskProbeMsg {
    PipeNumber {
        reply_to: mpsc::Sender<i32>,
    },
    PipeFailure {
        reply_to: mpsc::Sender<&'static str>,
    },
    SpawnTask {
        reply_to: mpsc::Sender<&'static str>,
    },
    SpawnTaskAfterRelease {
        ready_to: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
        result_to: mpsc::Sender<Result<(), String>>,
        reply_to: mpsc::Sender<&'static str>,
    },
    PipeAfterRelease {
        ready_to: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
        reply_to: mpsc::Sender<&'static str>,
    },
    Fail,
    Ping {
        reply_to: mpsc::Sender<&'static str>,
    },
    PipedNumber {
        value: i32,
        reply_to: mpsc::Sender<i32>,
    },
    PipedFailure {
        reason: &'static str,
        reply_to: mpsc::Sender<&'static str>,
    },
    TaskDone {
        reply_to: mpsc::Sender<&'static str>,
    },
}

struct TaskProbe;

impl Actor for TaskProbe {
    type Msg = TaskProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            TaskProbeMsg::PipeNumber { reply_to } => {
                ctx.pipe_to_self(
                    || Ok::<i32, &'static str>(41),
                    |result| TaskProbeMsg::PipedNumber {
                        value: result.expect("pipe task should succeed") + 1,
                        reply_to,
                    },
                )?;
            }
            TaskProbeMsg::PipeFailure { reply_to } => {
                ctx.pipe_to_self(
                    || Err::<i32, &'static str>("failed"),
                    |result| TaskProbeMsg::PipedFailure {
                        reason: result.expect_err("pipe task should fail"),
                        reply_to,
                    },
                )?;
            }
            TaskProbeMsg::SpawnTask { reply_to } => {
                ctx.spawn_task(move |myself| {
                    let _ = myself.tell(TaskProbeMsg::TaskDone { reply_to });
                })?;
            }
            TaskProbeMsg::SpawnTaskAfterRelease {
                ready_to,
                release,
                result_to,
                reply_to,
            } => {
                ctx.spawn_task(move |myself| {
                    let _ = ready_to.send(());
                    let _ = release.recv_timeout(Duration::from_secs(1));
                    let result = myself
                        .tell(TaskProbeMsg::TaskDone { reply_to })
                        .map(|_| ())
                        .map_err(|error| error.reason().to_string());
                    let _ = result_to.send(result);
                })?;
            }
            TaskProbeMsg::PipeAfterRelease {
                ready_to,
                release,
                reply_to,
            } => {
                ctx.pipe_to_self(
                    move || {
                        let _ = ready_to.send(());
                        release
                            .recv_timeout(Duration::from_secs(1))
                            .map_err(|_| "release timed out")
                    },
                    move |result| {
                        result.expect("pipe task should be released");
                        TaskProbeMsg::TaskDone { reply_to }
                    },
                )?;
            }
            TaskProbeMsg::Fail => return Err(ActorError::Message("task probe failed".to_string())),
            TaskProbeMsg::Ping { reply_to } => {
                reply_to
                    .send("pong")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TaskProbeMsg::PipedNumber { value, reply_to } => {
                reply_to
                    .send(value)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TaskProbeMsg::PipedFailure { reason, reply_to } => {
                reply_to
                    .send(reason)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            TaskProbeMsg::TaskDone { reply_to } => {
                reply_to
                    .send("done")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[test]
fn pipe_to_self_delivers_success_result_through_mailbox() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("task", Props::new(|| TaskProbe)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(TaskProbeMsg::PipeNumber { reply_to: reply_tx })
        .unwrap();

    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 42);
}

#[test]
fn pipe_to_self_delivers_failure_result_through_mailbox() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("task", Props::new(|| TaskProbe)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(TaskProbeMsg::PipeFailure { reply_to: reply_tx })
        .unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "failed"
    );
}

#[test]
fn spawn_task_sends_back_through_actor_ref() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system.spawn("task", Props::new(|| TaskProbe)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(TaskProbeMsg::SpawnTask { reply_to: reply_tx })
        .unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "done"
    );
}

#[test]
fn spawn_task_completion_after_owner_stop_is_rejected() {
    let system = ActorSystem::builder("test-task-stop").build().unwrap();
    let actor = system.spawn("task", Props::new(|| TaskProbe)).unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(TaskProbeMsg::SpawnTaskAfterRelease {
            ready_to: ready_tx,
            release: release_rx,
            result_to: result_tx,
            reply_to: reply_tx,
        })
        .unwrap();
    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&actor);
    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    release_tx.send(()).unwrap();

    let result = result_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(result, Err("actor is stopped".to_string()));
    assert!(reply_rx.recv_timeout(Duration::from_millis(50)).is_err());
    assert_dead_letter_count_with_reason(&system, 1, "actor is stopped");
}

#[test]
fn pipe_to_self_completion_after_owner_stop_is_rejected() {
    let system = ActorSystem::builder("test-pipe-stop").build().unwrap();
    let actor = system.spawn("task", Props::new(|| TaskProbe)).unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(TaskProbeMsg::PipeAfterRelease {
            ready_to: ready_tx,
            release: release_rx,
            reply_to: reply_tx,
        })
        .unwrap();
    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&actor);
    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    release_tx.send(()).unwrap();

    assert!(reply_rx.recv_timeout(Duration::from_millis(50)).is_err());
    assert_dead_letter_count_with_reason(&system, 1, "actor is stopped");
}

#[test]
fn spawn_task_completion_after_owner_restart_is_rejected() {
    let system = ActorSystem::builder("test-task-restart").build().unwrap();
    let actor = system
        .spawn(
            "task",
            Props::restartable(|| TaskProbe).with_supervisor(SupervisorStrategy::Restart),
        )
        .unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(TaskProbeMsg::SpawnTaskAfterRelease {
            ready_to: ready_tx,
            release: release_rx,
            result_to: result_tx,
            reply_to: reply_tx,
        })
        .unwrap();
    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    restart_actor_and_wait_until_live(&actor);
    release_tx.send(()).unwrap();

    let result = result_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(result, Err("actor task is cancelled".to_string()));
    assert!(reply_rx.recv_timeout(Duration::from_millis(50)).is_err());
    assert_dead_letter_count_with_reason(&system, 1, "actor task is cancelled");
}

#[test]
fn pipe_to_self_completion_after_owner_restart_is_rejected() {
    let system = ActorSystem::builder("test-pipe-restart").build().unwrap();
    let actor = system
        .spawn(
            "task",
            Props::restartable(|| TaskProbe).with_supervisor(SupervisorStrategy::Restart),
        )
        .unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(TaskProbeMsg::PipeAfterRelease {
            ready_to: ready_tx,
            release: release_rx,
            reply_to: reply_tx,
        })
        .unwrap();
    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    restart_actor_and_wait_until_live(&actor);
    release_tx.send(()).unwrap();

    assert!(reply_rx.recv_timeout(Duration::from_millis(50)).is_err());
    assert_dead_letter_count_with_reason(&system, 1, "actor task is cancelled");
}

fn restart_actor_and_wait_until_live(actor: &ActorRef<TaskProbeMsg>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    actor.tell(TaskProbeMsg::Fail).unwrap();
    actor
        .tell(TaskProbeMsg::Ping { reply_to: reply_tx })
        .unwrap();
    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "pong"
    );
}

fn assert_dead_letter_count_with_reason(system: &ActorSystem, count: usize, reason: &str) {
    assert!(
        system
            .dead_letters()
            .wait_for_len(count, Duration::from_secs(1))
    );

    let records = system.dead_letters().records();
    assert_eq!(records.len(), count);
    assert!(
        records.iter().all(|record| record.reason() == reason),
        "expected all dead letters to have reason `{reason}`, got {records:?}"
    );
}
