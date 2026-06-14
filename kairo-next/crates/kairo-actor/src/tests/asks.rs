use super::*;

#[derive(Debug)]
struct AskReply(i32);

enum AskTargetMsg {
    Reply {
        value: i32,
        reply_to: ActorRef<AskReply>,
    },
    CaptureOnly {
        reply_to: ActorRef<AskReply>,
        captured: mpsc::Sender<ActorRef<AskReply>>,
    },
}

struct AskTarget;

impl Actor for AskTarget {
    type Msg = AskTargetMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            AskTargetMsg::Reply { value, reply_to } => {
                reply_to
                    .tell(AskReply(value + 1))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            AskTargetMsg::CaptureOnly { reply_to, captured } => {
                captured
                    .send(reply_to)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

enum AskProbeMsg {
    AskSuccess {
        target: ActorRef<AskTargetMsg>,
        reply_to: mpsc::Sender<Result<i32, String>>,
    },
    AskTimeout {
        target: ActorRef<AskTargetMsg>,
        captured: mpsc::Sender<ActorRef<AskReply>>,
        reply_to: mpsc::Sender<Result<i32, String>>,
    },
    AskPending {
        target: ActorRef<AskTargetMsg>,
        captured: mpsc::Sender<ActorRef<AskReply>>,
        reply_to: mpsc::Sender<Result<i32, String>>,
    },
    AskAndStop {
        target: ActorRef<AskTargetMsg>,
        captured: mpsc::Sender<ActorRef<AskReply>>,
        reply_to: mpsc::Sender<Result<i32, String>>,
    },
    AskAndFail {
        target: ActorRef<AskTargetMsg>,
        captured: mpsc::Sender<ActorRef<AskReply>>,
        reply_to: mpsc::Sender<Result<i32, String>>,
    },
    Asked {
        result: AskResult<AskReply>,
        reply_to: mpsc::Sender<Result<i32, String>>,
    },
    Ping(mpsc::Sender<()>),
}

struct AskProbe {
    pre_restart: Option<mpsc::Sender<()>>,
}

impl Actor for AskProbe {
    type Msg = AskProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            AskProbeMsg::AskSuccess { target, reply_to } => {
                ctx.ask(
                    target,
                    Duration::from_secs(1),
                    |reply_to| AskTargetMsg::Reply {
                        value: 41,
                        reply_to,
                    },
                    move |result| AskProbeMsg::Asked { result, reply_to },
                )?;
            }
            AskProbeMsg::AskTimeout {
                target,
                captured,
                reply_to,
            } => {
                ctx.ask(
                    target,
                    Duration::from_millis(20),
                    |reply_to| AskTargetMsg::CaptureOnly { reply_to, captured },
                    move |result| AskProbeMsg::Asked { result, reply_to },
                )?;
            }
            AskProbeMsg::AskPending {
                target,
                captured,
                reply_to,
            } => {
                ctx.ask(
                    target,
                    Duration::from_secs(60),
                    |reply_to| AskTargetMsg::CaptureOnly { reply_to, captured },
                    move |result| AskProbeMsg::Asked { result, reply_to },
                )?;
            }
            AskProbeMsg::AskAndStop {
                target,
                captured,
                reply_to,
            } => {
                ctx.ask(
                    target,
                    Duration::from_secs(1),
                    |reply_to| AskTargetMsg::CaptureOnly { reply_to, captured },
                    move |result| AskProbeMsg::Asked { result, reply_to },
                )?;
                ctx.stop(ctx.myself())?;
            }
            AskProbeMsg::AskAndFail {
                target,
                captured,
                reply_to,
            } => {
                ctx.ask(
                    target,
                    Duration::from_secs(1),
                    |reply_to| AskTargetMsg::CaptureOnly { reply_to, captured },
                    move |result| AskProbeMsg::Asked { result, reply_to },
                )?;
                return Err(ActorError::Message("boom".to_string()));
            }
            AskProbeMsg::Asked { result, reply_to } => {
                let observed = result
                    .map(|reply| reply.0)
                    .map_err(|error| error.to_string());
                reply_to
                    .send(observed)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            AskProbeMsg::Ping(reply_to) => reply_to
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))?,
        }
        Ok(())
    }

    fn signal(&mut self, _ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        if let Signal::PreRestart = signal
            && let Some(pre_restart) = &self.pre_restart
        {
            pre_restart
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }
}

#[test]
fn ask_temp_ref_is_unregistered_when_actor_system_terminates() {
    let system = ActorSystem::builder("test").build().unwrap();
    let target = system
        .spawn("ask-target", Props::new(|| AskTarget))
        .unwrap();
    let probe = system
        .spawn("ask-probe", Props::new(|| AskProbe { pre_restart: None }))
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();
    let (captured_tx, captured_rx) = mpsc::channel();

    probe
        .tell(AskProbeMsg::AskPending {
            target,
            captured: captured_tx,
            reply_to: reply_tx,
        })
        .unwrap();
    let reply_ref = captured_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let reply_path = reply_ref.path().clone();
    assert_eq!(reply_path.parent().unwrap().as_str(), "kairo://test/temp");
    assert!(
        system
            .resolve_local::<AskReply>(reply_path.as_str())
            .is_some()
    );

    system.terminate(Duration::from_secs(1)).unwrap();

    assert!(probe.wait_for_stop(Duration::from_secs(1)));
    assert!(system.is_terminated());
    assert!(
        system
            .resolve_local::<AskReply>(reply_path.as_str())
            .is_none()
    );
    let error = reply_ref.tell(AskReply(100)).unwrap_err();
    assert_eq!(error.reason(), "ask is completed");
    assert!(reply_rx.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn ask_sends_request_and_maps_reply_through_owner_mailbox() {
    let system = ActorSystem::builder("test").build().unwrap();
    let target = system
        .spawn("ask-target", Props::new(|| AskTarget))
        .unwrap();
    let probe = system
        .spawn("ask-probe", Props::new(|| AskProbe { pre_restart: None }))
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    probe
        .tell(AskProbeMsg::AskSuccess {
            target,
            reply_to: reply_tx,
        })
        .unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Ok(42)
    );
}

#[test]
fn ask_timeout_maps_failure_through_owner_mailbox() {
    let system = ActorSystem::builder("test").build().unwrap();
    let target = system
        .spawn("ask-target", Props::new(|| AskTarget))
        .unwrap();
    let probe = system
        .spawn("ask-probe", Props::new(|| AskProbe { pre_restart: None }))
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();
    let (captured_tx, _captured_rx) = mpsc::channel();

    probe
        .tell(AskProbeMsg::AskTimeout {
            target,
            captured: captured_tx,
            reply_to: reply_tx,
        })
        .unwrap();

    let error = reply_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(matches!(error, Err(message) if message.contains("ask timed out")));
}

#[test]
fn ask_late_reply_is_rejected_after_timeout() {
    let system = ActorSystem::builder("test").build().unwrap();
    let target = system
        .spawn("ask-target", Props::new(|| AskTarget))
        .unwrap();
    let probe = system
        .spawn("ask-probe", Props::new(|| AskProbe { pre_restart: None }))
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();
    let (captured_tx, captured_rx) = mpsc::channel();

    probe
        .tell(AskProbeMsg::AskTimeout {
            target,
            captured: captured_tx,
            reply_to: reply_tx,
        })
        .unwrap();
    let reply_ref = captured_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(
        reply_ref.path().parent().unwrap().as_str(),
        "kairo://test/temp"
    );
    assert!(reply_ref.path().name().unwrap().starts_with("ask$"));
    assert!(
        system
            .resolve_local::<AskReply>(reply_ref.path().as_str())
            .is_some()
    );
    assert!(
        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .is_err()
    );

    let error = reply_ref.tell(AskReply(100)).unwrap_err();

    assert_eq!(error.reason(), "ask is completed");
    assert!(
        system
            .resolve_local::<AskReply>(reply_ref.path().as_str())
            .is_none()
    );
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    assert_eq!(
        system.dead_letters().records()[0].recipient(),
        reply_ref.path()
    );
}

#[test]
fn ask_reply_is_rejected_after_owner_stops() {
    let system = ActorSystem::builder("test").build().unwrap();
    let target = system
        .spawn("ask-target", Props::new(|| AskTarget))
        .unwrap();
    let probe = system
        .spawn("ask-probe", Props::new(|| AskProbe { pre_restart: None }))
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();
    let (captured_tx, captured_rx) = mpsc::channel();

    probe
        .tell(AskProbeMsg::AskAndStop {
            target,
            captured: captured_tx,
            reply_to: reply_tx,
        })
        .unwrap();
    let reply_ref = captured_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(probe.wait_for_stop(Duration::from_secs(1)));
    assert!(
        system
            .resolve_local::<AskReply>(reply_ref.path().as_str())
            .is_none()
    );

    let error = reply_ref.tell(AskReply(100)).unwrap_err();

    assert_eq!(error.reason(), "ask is completed");
    assert!(reply_rx.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn ask_reply_is_rejected_after_owner_restart() {
    let system = ActorSystem::builder("test").build().unwrap();
    let target = system
        .spawn("ask-target", Props::new(|| AskTarget))
        .unwrap();
    let (pre_restart_tx, pre_restart_rx) = mpsc::channel();
    let probe = system
        .spawn(
            "ask-probe",
            Props::restartable(move || AskProbe {
                pre_restart: Some(pre_restart_tx.clone()),
            }),
        )
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();
    let (captured_tx, captured_rx) = mpsc::channel();

    probe
        .tell(AskProbeMsg::AskAndFail {
            target,
            captured: captured_tx,
            reply_to: reply_tx,
        })
        .unwrap();
    let reply_ref = captured_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    pre_restart_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(
        system
            .resolve_local::<AskReply>(reply_ref.path().as_str())
            .is_none()
    );

    let error = reply_ref.tell(AskReply(100)).unwrap_err();

    assert_eq!(error.reason(), "ask is completed");
    assert!(reply_rx.recv_timeout(Duration::from_millis(100)).is_err());
    let (ping_tx, ping_rx) = mpsc::channel();
    probe.tell(AskProbeMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();
}
