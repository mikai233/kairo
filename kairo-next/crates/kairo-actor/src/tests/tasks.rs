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
