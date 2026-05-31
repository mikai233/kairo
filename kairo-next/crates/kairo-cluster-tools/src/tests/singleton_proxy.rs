use super::*;

#[test]
fn singleton_proxy_buffers_and_flushes_to_identified_singleton() {
    let kit = ActorSystemTestKit::new("singleton-proxy-flush").unwrap();
    let singleton = kit.create_probe::<String>("singleton").unwrap();
    let state = kit
        .create_probe::<SingletonProxySnapshot>("proxy-state")
        .unwrap();
    let proxy = kit
        .system()
        .spawn(
            "singleton-proxy",
            SingletonProxyActor::<String>::props(SingletonProxySettings::new(4).unwrap()),
        )
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::Route("one".to_string()))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("two".to_string()))
        .unwrap();
    singleton.expect_no_msg(Duration::from_millis(100)).unwrap();

    proxy
        .tell(SingletonProxyMsg::IdentifySingleton {
            singleton: singleton.actor_ref(),
        })
        .unwrap();
    singleton
        .expect_msg_eq("one".to_string(), Duration::from_millis(500))
        .unwrap();
    singleton
        .expect_msg_eq("two".to_string(), Duration::from_millis(500))
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::Route("three".to_string()))
        .unwrap();
    singleton
        .expect_msg_eq("three".to_string(), Duration::from_millis(500))
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(snapshot.buffered_messages, 0);
    assert_eq!(snapshot.dropped_messages, 0);
    assert_eq!(
        snapshot.singleton_path.as_ref(),
        Some(singleton.actor_ref().path())
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_proxy_drops_oldest_message_when_buffer_is_full() {
    let kit = ActorSystemTestKit::new("singleton-proxy-overflow").unwrap();
    let singleton = kit.create_probe::<String>("singleton").unwrap();
    let state = kit
        .create_probe::<SingletonProxySnapshot>("proxy-state")
        .unwrap();
    let proxy = kit
        .system()
        .spawn(
            "singleton-proxy",
            SingletonProxyActor::<String>::props(SingletonProxySettings::new(2).unwrap()),
        )
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::Route("one".to_string()))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("two".to_string()))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("three".to_string()))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap(),
        SingletonProxySnapshot {
            current_oldest: None,
            registered_routes: 0,
            singleton_path: None,
            buffered_messages: 2,
            dropped_messages: 1,
        }
    );

    proxy
        .tell(SingletonProxyMsg::IdentifySingleton {
            singleton: singleton.actor_ref(),
        })
        .unwrap();
    singleton
        .expect_msg_eq("two".to_string(), Duration::from_millis(500))
        .unwrap();
    singleton
        .expect_msg_eq("three".to_string(), Duration::from_millis(500))
        .unwrap();
    singleton.expect_no_msg(Duration::from_millis(100)).unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_proxy_identifies_registered_route_from_initial_observation() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b,
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let kit = ActorSystemTestKit::new("singleton-proxy-initial-oldest").unwrap();
    let singleton = kit.create_probe::<String>("singleton").unwrap();
    let state = kit
        .create_probe::<SingletonProxySnapshot>("proxy-state")
        .unwrap();
    let proxy = kit
        .system()
        .spawn(
            "singleton-proxy",
            SingletonProxyActor::<String>::props(SingletonProxySettings::new(4).unwrap()),
        )
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::Route("before".to_string()))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::RegisterRoute {
            node: node_a.clone(),
            singleton: singleton.actor_ref(),
        })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::ApplyInitialObservation { observation })
        .unwrap();

    singleton
        .expect_msg_eq("before".to_string(), Duration::from_millis(500))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("after".to_string()))
        .unwrap();
    singleton
        .expect_msg_eq("after".to_string(), Duration::from_millis(500))
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(snapshot.current_oldest, Some(node_a));
    assert_eq!(snapshot.registered_routes, 1);
    assert_eq!(
        snapshot.singleton_path.as_ref(),
        Some(singleton.actor_ref().path())
    );
    assert_eq!(snapshot.buffered_messages, 0);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteSingletonMsg {
    value: u8,
}

impl RemoteMessage for RemoteSingletonMsg {
    const MANIFEST: &'static str = "kairo.cluster-tools.test.RemoteSingletonMsg";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy)]
struct RemoteSingletonMsgCodec;

impl MessageCodec<RemoteSingletonMsg> for RemoteSingletonMsgCodec {
    fn serializer_id(&self) -> u32 {
        73_001
    }

    fn encode(&self, message: &RemoteSingletonMsg) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value]))
    }

    fn decode(
        &self,
        payload: Bytes,
        _version: u16,
    ) -> kairo_serialization::Result<RemoteSingletonMsg> {
        Ok(RemoteSingletonMsg { value: payload[0] })
    }
}

#[derive(Default)]
struct CollectingRemoteOutbound {
    sent: Mutex<Vec<RemoteEnvelope>>,
    changed: Condvar,
}

impl CollectingRemoteOutbound {
    fn wait_for_len(&self, len: usize, timeout: Duration) -> Vec<RemoteEnvelope> {
        let deadline = Instant::now() + timeout;
        let mut sent = self.sent.lock().expect("remote outbound poisoned");
        while sent.len() < len {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            let (next_sent, wait) = self
                .changed
                .wait_timeout(sent, remaining)
                .expect("remote outbound poisoned");
            sent = next_sent;
            if wait.timed_out() {
                break;
            }
        }
        sent.clone()
    }
}

impl RemoteOutbound for CollectingRemoteOutbound {
    fn send(&self, envelope: RemoteEnvelope) -> kairo_remote::Result<()> {
        self.sent
            .lock()
            .expect("remote outbound poisoned")
            .push(envelope);
        self.changed.notify_all();
        Ok(())
    }
}

fn remote_singleton_registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    registry
        .register::<RemoteSingletonMsg, _>(RemoteSingletonMsgCodec)
        .unwrap();
    Arc::new(registry)
}

#[test]
fn singleton_proxy_flushes_buffered_messages_to_remote_target() {
    let self_node = node("singleton-proxy-remote-local", 1);
    let remote_node = remote_node("singleton-proxy-remote", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        self_node,
        SingletonScope::all(),
        [member(remote_node.clone(), MemberStatus::Up, 1)],
    );
    let kit = ActorSystemTestKit::new("singleton-proxy-remote").unwrap();
    let proxy = kit
        .system()
        .spawn(
            "singleton-proxy",
            SingletonProxyActor::<RemoteSingletonMsg>::props(
                SingletonProxySettings::new(4).unwrap(),
            ),
        )
        .unwrap();
    let outbound = Arc::new(CollectingRemoteOutbound::default());
    let remote_ref = RemoteActorRef::new(
        ActorRefWireData::new(format!("{}/user/singleton", remote_node.address)).unwrap(),
        remote_singleton_registry(),
        outbound.clone() as Arc<dyn RemoteOutbound>,
    );

    proxy
        .tell(SingletonProxyMsg::Route(RemoteSingletonMsg { value: 1 }))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::RegisterTarget {
            node: remote_node.clone(),
            singleton: RemoteSingletonProxyTarget::remote(remote_ref),
        })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::ApplyInitialObservation { observation })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route(RemoteSingletonMsg { value: 2 }))
        .unwrap();

    let sent = outbound.wait_for_len(2, Duration::from_secs(1));
    assert_eq!(sent.len(), 2);
    assert_eq!(
        sent[0].recipient.path(),
        "kairo://singleton-proxy-remote@singleton-proxy-remote.example.test:2552/user/singleton"
    );
    assert_eq!(
        sent[0].message.manifest.as_str(),
        RemoteSingletonMsg::MANIFEST
    );
    assert_eq!(sent[0].message.payload, Bytes::from_static(&[1]));
    assert_eq!(sent[1].message.payload, Bytes::from_static(&[2]));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_proxy_reidentifies_when_oldest_route_changes() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let kit = ActorSystemTestKit::new("singleton-proxy-oldest-change").unwrap();
    let singleton_a = kit.create_probe::<String>("singleton-a").unwrap();
    let singleton_b = kit.create_probe::<String>("singleton-b").unwrap();
    let proxy = kit
        .system()
        .spawn(
            "singleton-proxy",
            SingletonProxyActor::<String>::props(SingletonProxySettings::new(4).unwrap()),
        )
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::RegisterRoute {
            node: node_a.clone(),
            singleton: singleton_a.actor_ref(),
        })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::ApplyInitialObservation { observation })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("one".to_string()))
        .unwrap();
    singleton_a
        .expect_msg_eq("one".to_string(), Duration::from_millis(500))
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::ApplyOldestChange {
            change: SingletonOldestChange::OldestChanged(Some(node_b.clone())),
        })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("two".to_string()))
        .unwrap();
    singleton_a
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();
    singleton_b
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::RegisterRoute {
            node: node_b,
            singleton: singleton_b.actor_ref(),
        })
        .unwrap();
    singleton_b
        .expect_msg_eq("two".to_string(), Duration::from_millis(500))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("three".to_string()))
        .unwrap();
    singleton_b
        .expect_msg_eq("three".to_string(), Duration::from_millis(500))
        .unwrap();
    singleton_a
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[derive(Clone)]
enum SingletonProxyTargetMsg {
    Payload(&'static str),
    Stop,
}

struct SingletonProxyTarget {
    observed: ActorRef<&'static str>,
}

impl Actor for SingletonProxyTarget {
    type Msg = SingletonProxyTargetMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            SingletonProxyTargetMsg::Payload(value) => {
                let _ = self.observed.tell(value);
            }
            SingletonProxyTargetMsg::Stop => ctx.stop(ctx.myself())?,
        }
        Ok(())
    }
}

#[test]
fn singleton_proxy_clears_current_singleton_on_termination_and_buffers_again() {
    let kit = ActorSystemTestKit::new("singleton-proxy-termination").unwrap();
    let observed = kit.create_probe::<&'static str>("observed").unwrap();
    let state = kit
        .create_probe::<SingletonProxySnapshot>("proxy-state")
        .unwrap();
    let proxy = kit
        .system()
        .spawn(
            "singleton-proxy",
            SingletonProxyActor::<SingletonProxyTargetMsg>::props(
                SingletonProxySettings::new(4).unwrap(),
            ),
        )
        .unwrap();
    let target_1 = kit
        .system()
        .spawn(
            "target-1",
            Props::new({
                let observed = observed.actor_ref();
                move || SingletonProxyTarget { observed }
            }),
        )
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::IdentifySingleton {
            singleton: target_1.clone(),
        })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route(SingletonProxyTargetMsg::Payload(
            "first",
        )))
        .unwrap();
    observed
        .expect_msg_eq("first", Duration::from_millis(500))
        .unwrap();

    target_1.tell(SingletonProxyTargetMsg::Stop).unwrap();
    assert!(target_1.wait_for_stop(Duration::from_secs(1)));
    let mut cleared = None;
    for _ in 0..100 {
        proxy
            .tell(SingletonProxyMsg::GetState {
                reply_to: state.actor_ref(),
            })
            .unwrap();
        let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
        if snapshot.singleton_path.is_none() {
            cleared = Some(snapshot);
            break;
        }
    }
    assert_eq!(
        cleared.expect("proxy should observe singleton termination"),
        SingletonProxySnapshot {
            current_oldest: None,
            registered_routes: 0,
            singleton_path: None,
            buffered_messages: 0,
            dropped_messages: 0,
        }
    );

    proxy
        .tell(SingletonProxyMsg::Route(SingletonProxyTargetMsg::Payload(
            "buffered",
        )))
        .unwrap();
    observed.expect_no_msg(Duration::from_millis(100)).unwrap();

    let target_2 = kit
        .system()
        .spawn(
            "target-2",
            Props::new({
                let observed = observed.actor_ref();
                move || SingletonProxyTarget { observed }
            }),
        )
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::IdentifySingleton {
            singleton: target_2,
        })
        .unwrap();
    observed
        .expect_msg_eq("buffered", Duration::from_millis(500))
        .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
