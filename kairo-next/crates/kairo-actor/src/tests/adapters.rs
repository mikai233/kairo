use super::*;

struct ExternalProbeMsg {
    label: &'static str,
    reply_to: mpsc::Sender<(&'static str, usize)>,
}

enum AdapterProbeMsg {
    CreateAdapter(mpsc::Sender<ActorRef<ExternalProbeMsg>>),
    Adapted(ExternalProbeMsg),
}

struct AdapterProbe {
    adapted_count: usize,
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
            AdapterProbeMsg::Adapted(message) => {
                self.adapted_count += 1;
                message
                    .reply_to
                    .send((message.label, self.adapted_count))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
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
