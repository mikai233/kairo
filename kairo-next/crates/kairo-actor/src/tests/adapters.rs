use super::*;

struct ExternalProbeMsg {
    label: &'static str,
    reply_to: mpsc::Sender<(&'static str, usize)>,
}

enum AdapterProbeMsg {
    CreateAdapter(mpsc::Sender<ActorRef<ExternalProbeMsg>>),
    StopThenCreateAdapter(mpsc::Sender<Result<(), String>>),
    Adapted(ExternalProbeMsg),
    BlockAndFail {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    },
    Ping(mpsc::Sender<usize>),
}

enum AdapterWatcherMsg {
    Watch {
        adapter: ActorRef<ExternalProbeMsg>,
        ack: mpsc::Sender<()>,
    },
}

struct AdapterProbe {
    adapted_count: usize,
}

struct AdapterWatcher {
    terminated: mpsc::Sender<AnyActorRef>,
}

impl Actor for AdapterWatcher {
    type Msg = AdapterWatcherMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            AdapterWatcherMsg::Watch { adapter, ack } => {
                ctx.watch(&adapter)?;
                ack.send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }

    fn signal(&mut self, _ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        if let Signal::Terminated(subject) = signal {
            self.terminated
                .send(subject)
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }
}

impl Actor for AdapterProbe {
    type Msg = AdapterProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            AdapterProbeMsg::CreateAdapter(reply_to) => {
                let adapter = ctx.message_adapter(AdapterProbeMsg::Adapted)?;
                reply_to
                    .send(adapter)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            AdapterProbeMsg::StopThenCreateAdapter(reply_to) => {
                ctx.stop(ctx.myself())?;
                let result = ctx
                    .message_adapter(AdapterProbeMsg::Adapted)
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                reply_to
                    .send(result)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            AdapterProbeMsg::Adapted(message) => {
                self.adapted_count += 1;
                message
                    .reply_to
                    .send((message.label, self.adapted_count))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            AdapterProbeMsg::BlockAndFail { entered, release } => {
                entered
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                release
                    .recv()
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                return Err(ActorError::Message("boom".to_string()));
            }
            AdapterProbeMsg::Ping(reply_to) => reply_to
                .send(self.adapted_count)
                .map_err(|error| ActorError::Message(error.to_string()))?,
        }
        Ok(())
    }
}

#[test]
fn message_adapter_is_rejected_after_self_stop_is_requested() {
    let system = ActorSystem::builder("test-adapter-stop-requested")
        .build()
        .unwrap();
    let actor = system
        .spawn("adapter", Props::new(|| AdapterProbe { adapted_count: 0 }))
        .unwrap();
    let (result_tx, result_rx) = mpsc::channel();

    actor
        .tell(AdapterProbeMsg::StopThenCreateAdapter(result_tx))
        .unwrap();

    let result = result_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(result, Err(format!("actor `{}` is stopping", actor.path())));
    assert!(actor.wait_for_stop(Duration::from_secs(1)));
}

#[test]
fn message_adapter_maps_external_protocol_into_owner_mailbox() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn("adapter", Props::new(|| AdapterProbe { adapted_count: 0 }))
        .unwrap();
    let (adapter_tx, adapter_rx) = mpsc::channel();
    let (reply_tx, reply_rx) = mpsc::channel();

    actor
        .tell(AdapterProbeMsg::CreateAdapter(adapter_tx))
        .unwrap();
    let adapter = adapter_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    adapter
        .tell(ExternalProbeMsg {
            label: "external",
            reply_to: reply_tx,
        })
        .unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("external", 1)
    );
    assert!(
        adapter
            .path()
            .as_str()
            .starts_with(&format!("{}/$adapter-", actor.path()))
    );
}

#[test]
fn message_adapter_rejects_after_owner_stops() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn("adapter", Props::new(|| AdapterProbe { adapted_count: 0 }))
        .unwrap();
    let (adapter_tx, adapter_rx) = mpsc::channel();
    let (reply_tx, _reply_rx) = mpsc::channel();

    actor
        .tell(AdapterProbeMsg::CreateAdapter(adapter_tx))
        .unwrap();
    let adapter = adapter_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    system.stop(&actor);
    assert!(actor.wait_for_stop(Duration::from_secs(1)));

    let error = adapter
        .tell(ExternalProbeMsg {
            label: "late",
            reply_to: reply_tx,
        })
        .unwrap_err();

    assert_eq!(error.reason(), "actor is stopped");
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    assert_eq!(
        system.dead_letters().records()[0].recipient(),
        adapter.path()
    );
}

#[test]
fn actor_system_terminate_stops_user_message_adapter_refs() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn("adapter", Props::new(|| AdapterProbe { adapted_count: 0 }))
        .unwrap();

    assert_actor_system_terminate_stops_message_adapter_refs(system, actor);
}

#[test]
fn actor_system_terminate_stops_system_message_adapter_refs() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn_system(
            "system-adapter",
            Props::new(|| AdapterProbe { adapted_count: 0 }),
        )
        .unwrap();

    assert_actor_system_terminate_stops_message_adapter_refs(system, actor);
}

fn assert_actor_system_terminate_stops_message_adapter_refs(
    system: ActorSystem,
    actor: ActorRef<AdapterProbeMsg>,
) {
    let (adapter_tx, adapter_rx) = mpsc::channel();
    let (reply_tx, _reply_rx) = mpsc::channel();

    actor
        .tell(AdapterProbeMsg::CreateAdapter(adapter_tx))
        .unwrap();
    let adapter = adapter_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let adapter_path = adapter.path().clone();

    system.terminate(Duration::from_secs(1)).unwrap();
    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    assert!(adapter.wait_for_stop(Duration::from_secs(1)));
    assert!(system.is_terminated());

    let error = adapter
        .tell(ExternalProbeMsg {
            label: "late",
            reply_to: reply_tx,
        })
        .unwrap_err();

    assert_eq!(error.reason(), "actor is stopped");
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    let records = system.dead_letters().records();
    assert!(
        records
            .iter()
            .any(|record| record.recipient() == &adapter_path
                && record.reason() == "actor is stopped"
                && record.message_type() == std::any::type_name::<ExternalProbeMsg>())
    );
}

#[test]
fn message_adapter_notifies_watchers_after_owner_stops() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn("adapter", Props::new(|| AdapterProbe { adapted_count: 0 }))
        .unwrap();
    let (terminated_tx, terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || AdapterWatcher {
                terminated: terminated_tx.clone(),
            }),
        )
        .unwrap();
    let (adapter_tx, adapter_rx) = mpsc::channel();
    let (watch_ack_tx, watch_ack_rx) = mpsc::channel();

    actor
        .tell(AdapterProbeMsg::CreateAdapter(adapter_tx))
        .unwrap();
    let adapter = adapter_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let adapter_path = adapter.path().clone();
    watcher
        .tell(AdapterWatcherMsg::Watch {
            adapter: adapter.clone(),
            ack: watch_ack_tx,
        })
        .unwrap();
    watch_ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&actor);
    assert!(actor.wait_for_stop(Duration::from_secs(1)));
    assert!(adapter.wait_for_stop(Duration::from_secs(1)));

    let terminated = terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(terminated.path(), &adapter_path);
}

#[test]
fn message_adapter_rejects_and_drops_stale_messages_after_owner_restart() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "adapter",
            Props::restartable(|| AdapterProbe { adapted_count: 0 })
                .with_supervisor(SupervisorStrategy::Restart),
        )
        .unwrap();
    let (adapter_tx, adapter_rx) = mpsc::channel();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (stale_tx, stale_rx) = mpsc::channel();

    actor
        .tell(AdapterProbeMsg::CreateAdapter(adapter_tx))
        .unwrap();
    let adapter = adapter_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    actor
        .tell(AdapterProbeMsg::BlockAndFail {
            entered: entered_tx,
            release: release_rx,
        })
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    adapter
        .tell(ExternalProbeMsg {
            label: "stale",
            reply_to: stale_tx,
        })
        .unwrap();
    release_tx.send(()).unwrap();

    let (ping_tx, ping_rx) = mpsc::channel();
    actor.tell(AdapterProbeMsg::Ping(ping_tx)).unwrap();
    assert_eq!(ping_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 0);
    assert!(stale_rx.recv_timeout(Duration::from_millis(100)).is_err());

    let (late_tx, _late_rx) = mpsc::channel();
    let error = adapter
        .tell(ExternalProbeMsg {
            label: "late",
            reply_to: late_tx,
        })
        .unwrap_err();
    assert_eq!(error.reason(), "actor is stopped");
}

#[test]
fn message_adapter_survives_owner_resume_supervision() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "adapter",
            Props::new(|| AdapterProbe { adapted_count: 0 })
                .with_supervisor(SupervisorStrategy::Resume),
        )
        .unwrap();
    let (adapter_tx, adapter_rx) = mpsc::channel();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (queued_tx, queued_rx) = mpsc::channel();

    actor
        .tell(AdapterProbeMsg::CreateAdapter(adapter_tx))
        .unwrap();
    let adapter = adapter_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    actor
        .tell(AdapterProbeMsg::BlockAndFail {
            entered: entered_tx,
            release: release_rx,
        })
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    adapter
        .tell(ExternalProbeMsg {
            label: "queued",
            reply_to: queued_tx,
        })
        .unwrap();
    release_tx.send(()).unwrap();

    assert_eq!(
        queued_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("queued", 1)
    );

    let (late_tx, late_rx) = mpsc::channel();
    adapter
        .tell(ExternalProbeMsg {
            label: "late",
            reply_to: late_tx,
        })
        .unwrap();
    assert_eq!(
        late_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("late", 2)
    );
    assert!(!adapter.is_stopped());
}

#[test]
fn message_adapter_notifies_watchers_after_owner_restart() {
    let system = ActorSystem::builder("test").build().unwrap();
    let actor = system
        .spawn(
            "adapter",
            Props::restartable(|| AdapterProbe { adapted_count: 0 })
                .with_supervisor(SupervisorStrategy::Restart),
        )
        .unwrap();
    let (terminated_tx, terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || AdapterWatcher {
                terminated: terminated_tx.clone(),
            }),
        )
        .unwrap();
    let (adapter_tx, adapter_rx) = mpsc::channel();
    let (watch_ack_tx, watch_ack_rx) = mpsc::channel();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    actor
        .tell(AdapterProbeMsg::CreateAdapter(adapter_tx))
        .unwrap();
    let adapter = adapter_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let adapter_path = adapter.path().clone();
    watcher
        .tell(AdapterWatcherMsg::Watch {
            adapter: adapter.clone(),
            ack: watch_ack_tx,
        })
        .unwrap();
    watch_ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    actor
        .tell(AdapterProbeMsg::BlockAndFail {
            entered: entered_tx,
            release: release_rx,
        })
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    release_tx.send(()).unwrap();

    let terminated = terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(terminated.path(), &adapter_path);
    assert!(adapter.wait_for_stop(Duration::from_secs(1)));

    let (ping_tx, ping_rx) = mpsc::channel();
    actor.tell(AdapterProbeMsg::Ping(ping_tx)).unwrap();
    assert_eq!(ping_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 0);
}
