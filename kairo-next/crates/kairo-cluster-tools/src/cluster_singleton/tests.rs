use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{Actor, ActorResult, Context, Props};
use kairo_cluster::{
    ClusterDaemonBootstrapSettings, ClusterDaemonHandle, ClusterGossipProcessSettings,
    ClusterMembershipMsg, DeadlineFailureDetectorSettings, Gossip, HeartbeatSenderSettings,
    MemberStatus, register_cluster_daemon, register_cluster_protocol_codecs,
};
use kairo_remote::{
    RemoteSettings, TcpRemoteActorRuntime, TcpRemoteReconnectSettings,
    register_remote_protocol_codecs,
};
use kairo_serialization::{
    MessageCodec, Registry, RemoteMessage, SerializationError, SerializationRegistry,
};
use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

use super::*;
use crate::register_cluster_tools_protocol_codecs;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Command {
    Ping(u8),
    Stop,
}

impl RemoteMessage for Command {
    const MANIFEST: &'static str = "kairo.cluster-tools.test.ClusterSingletonCommand";
    const VERSION: u16 = 1;
}

struct CommandCodec;

impl MessageCodec<Command> for CommandCodec {
    fn serializer_id(&self) -> u32 {
        19_102
    }

    fn encode(&self, message: &Command) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from_static(match message {
            Command::Ping(value) => return Ok(Bytes::from(vec![0, *value])),
            Command::Stop => &[1],
        }))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Command> {
        match payload.as_ref() {
            [0, value] => Ok(Command::Ping(*value)),
            [1] => Ok(Command::Stop),
            _ => Err(SerializationError::Message(
                "invalid cluster singleton command".to_string(),
            )),
        }
    }
}

struct SingletonActor {
    sink: kairo_actor::ActorRef<u8>,
}

#[derive(Debug, Clone)]
enum LocalCommand {
    Ping(u8),
    Stop,
}

struct LocalProtocolSingleton {
    sink: kairo_actor::ActorRef<u8>,
}

impl Actor for LocalProtocolSingleton {
    type Msg = LocalCommand;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            LocalCommand::Ping(value) => {
                let _ = self.sink.tell(value);
            }
            LocalCommand::Stop => ctx.stop(ctx.myself())?,
        }
        Ok(())
    }
}

impl Actor for SingletonActor {
    type Msg = Command;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            Command::Ping(value) => {
                let _ = self.sink.tell(value);
            }
            Command::Stop => ctx.stop(ctx.myself())?,
        }
        Ok(())
    }
}

struct SingletonNode {
    kit: ActorSystemTestKit,
    runtime: TcpRemoteActorRuntime,
    cluster: ClusterDaemonHandle,
    singleton: ClusterSingletonRef<Command>,
    secondary: ClusterSingletonRef<Command>,
    local_protocol: ClusterSingletonRef<LocalCommand>,
    deliveries: TestProbe<u8>,
    secondary_deliveries: TestProbe<u8>,
    local_deliveries: TestProbe<u8>,
    gossip: TestProbe<Gossip>,
    proxy_state: TestProbe<crate::SingletonProxySnapshot>,
}

impl SingletonNode {
    fn start(
        system: &str,
        node_uid: u64,
        remote_uid: u64,
        seed_nodes: Vec<kairo_actor::Address>,
        registry: Arc<Registry>,
    ) -> Self {
        let kit = ActorSystemTestKit::new(system).unwrap();
        let deliveries = kit.create_probe("singleton-deliveries").unwrap();
        let secondary_deliveries = kit.create_probe("secondary-singleton-deliveries").unwrap();
        let local_deliveries = kit
            .create_probe("local-protocol-singleton-deliveries")
            .unwrap();
        let mut builder = TcpRemoteActorRuntime::builder(
            kit.system().clone(),
            registry,
            RemoteSettings::new("127.0.0.1", 0),
            remote_uid,
        )
        .with_reconnect_settings(
            TcpRemoteReconnectSettings::new(Duration::from_millis(100), Duration::from_millis(300))
                .unwrap(),
        );
        let cluster_registration = register_cluster_daemon(
            &mut builder,
            ClusterDaemonBootstrapSettings::new(node_uid)
                .with_seed_nodes(seed_nodes)
                .with_config_digest(Some(Bytes::from_static(b"cluster-singleton-extension")))
                .with_gossip_process_settings(
                    ClusterGossipProcessSettings::new(Duration::from_millis(15)).unwrap(),
                )
                .with_heartbeat_sender_settings(
                    HeartbeatSenderSettings::new(
                        5,
                        DeadlineFailureDetectorSettings::new(
                            Duration::from_millis(15),
                            Duration::from_millis(200),
                        )
                        .unwrap(),
                    )
                    .with_heartbeat_expected_response_after(Duration::from_millis(10)),
                ),
        )
        .unwrap();
        let singleton_registration = register_cluster_singleton(
            &mut builder,
            cluster_registration.clone(),
            ClusterSingletonSettings::default()
                .with_route_refresh_interval(Duration::from_millis(10)),
        )
        .unwrap();
        let runtime = builder.bind().unwrap();
        let cluster = cluster_registration.activate(&runtime).unwrap();
        let extension = singleton_registration.activate(&runtime).unwrap();
        let sink = deliveries.actor_ref();
        let singleton = extension
            .init(Singleton::new(
                "orders",
                move || {
                    let sink = sink.clone();
                    Props::new(move || SingletonActor { sink: sink.clone() })
                },
                Command::Stop,
            ))
            .unwrap();
        let duplicate = extension
            .init(Singleton::new(
                "orders",
                || -> Props<SingletonActor> { panic!("duplicate singleton factory must not run") },
                Command::Stop,
            ))
            .unwrap();
        assert_eq!(singleton.proxy().path(), duplicate.proxy().path());
        let secondary_sink = secondary_deliveries.actor_ref();
        let secondary = extension
            .init(Singleton::new(
                "billing",
                move || {
                    let sink = secondary_sink.clone();
                    Props::new(move || SingletonActor { sink: sink.clone() })
                },
                Command::Stop,
            ))
            .unwrap();
        assert_ne!(singleton.proxy().path(), secondary.proxy().path());
        let local_sink = local_deliveries.actor_ref();
        let local_protocol = extension
            .init_local(Singleton::new(
                "sharding-coordinator-local-protocol",
                move || {
                    let sink = local_sink.clone();
                    Props::new(move || LocalProtocolSingleton { sink: sink.clone() })
                },
                LocalCommand::Stop,
            ))
            .unwrap();
        Self {
            gossip: kit.create_probe("cluster-gossip").unwrap(),
            proxy_state: kit.create_probe("singleton-proxy-state").unwrap(),
            kit,
            runtime,
            cluster,
            singleton,
            secondary,
            local_protocol,
            deliveries,
            secondary_deliveries,
            local_deliveries,
        }
    }

    fn gossip(&self) -> Gossip {
        self.cluster
            .membership()
            .tell(ClusterMembershipMsg::SendCurrentGossip {
                reply_to: self.gossip.actor_ref(),
            })
            .unwrap();
        self.gossip.expect_msg(Duration::from_secs(1)).unwrap()
    }

    fn proxy_state(&self) -> crate::SingletonProxySnapshot {
        self.singleton
            .proxy()
            .tell(SingletonProxyMsg::GetState {
                reply_to: self.proxy_state.actor_ref(),
            })
            .unwrap();
        self.proxy_state.expect_msg(Duration::from_secs(1)).unwrap()
    }

    fn shutdown(self) {
        self.kit.system().stop(self.cluster.root());
        self.runtime.shutdown().unwrap();
        self.kit.shutdown(Duration::from_secs(2)).unwrap();
    }
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_remote_protocol_codecs(&mut registry).unwrap();
    register_cluster_protocol_codecs(&mut registry).unwrap();
    register_cluster_tools_protocol_codecs(&mut registry).unwrap();
    registry.register::<Command, _>(CommandCodec).unwrap();
    Arc::new(registry)
}

#[test]
fn stable_singleton_name_hash_has_documented_fnv1a_value() {
    assert_eq!(stable_name_token("orders"), 0x0012_5d92_50be_8b4c);
}

#[test]
fn stable_delivery_endpoint_buffers_until_the_live_child_is_known() {
    let kit = ActorSystemTestKit::new("singleton-delivery-buffer").unwrap();
    let deliveries = kit.create_probe::<u8>("deliveries").unwrap();
    let endpoint = kit
        .system()
        .spawn(
            "endpoint",
            Props::new(|| SingletonDeliveryActor {
                singleton: None,
                buffer: std::collections::VecDeque::new(),
                buffer_size: 2,
            }),
        )
        .unwrap();

    endpoint.tell(SingletonDeliveryMsg::Deliver(1)).unwrap();
    endpoint.tell(SingletonDeliveryMsg::Deliver(2)).unwrap();
    endpoint.tell(SingletonDeliveryMsg::Deliver(3)).unwrap();
    endpoint
        .tell(SingletonDeliveryMsg::Update(Some(deliveries.actor_ref())))
        .unwrap();

    deliveries.expect_msg_eq(2, Duration::from_secs(1)).unwrap();
    deliveries.expect_msg_eq(3, Duration::from_secs(1)).unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn composed_singleton_routes_to_oldest_and_hands_over_when_it_leaves() {
    let registry = registry();
    let seed = SingletonNode::start("singleton-extension-seed", 1, 201, vec![], registry.clone());
    await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
        (seed
            .gossip()
            .member(seed.cluster.self_node())
            .map(|member| member.status)
            == Some(MemberStatus::Up))
        .then_some(())
        .ok_or_else(|| "singleton seed has not formed".to_string())
    })
    .unwrap();
    let peer = SingletonNode::start(
        "singleton-extension-peer",
        2,
        202,
        vec![seed.cluster.self_node().address.clone()],
        registry,
    );
    await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
        let seed_gossip = seed.gossip();
        let peer_gossip = peer.gossip();
        (seed_gossip.member(peer.cluster.self_node()).is_some()
            && peer_gossip.member(seed.cluster.self_node()).is_some())
        .then_some(())
        .ok_or_else(|| "singleton nodes have not converged".to_string())
    })
    .unwrap();

    await_assert(Duration::from_secs(2), Duration::from_millis(10), || {
        let seed_state = seed.proxy_state();
        let peer_state = peer.proxy_state();
        (seed_state.current_oldest.as_ref() == Some(seed.cluster.self_node())
            && seed_state.singleton_path.is_some()
            && peer_state.current_oldest.as_ref() == Some(seed.cluster.self_node())
            && peer_state.singleton_path.is_some())
        .then_some(())
        .ok_or_else(|| {
            format!("singleton routes not ready: seed={seed_state:?}, peer={peer_state:?}")
        })
    })
    .unwrap();

    await_assert(Duration::from_secs(3), Duration::from_millis(25), || {
        peer.singleton.tell(Command::Ping(1)).unwrap();
        seed.deliveries
            .expect_msg_eq(1, Duration::from_millis(75))
            .map_err(|_| "singleton is not yet reachable on the oldest node".to_string())
    })
    .unwrap();
    await_assert(Duration::from_secs(3), Duration::from_millis(25), || {
        peer.secondary.tell(Command::Ping(11)).unwrap();
        seed.secondary_deliveries
            .expect_msg_eq(11, Duration::from_millis(75))
            .map_err(|_| "second named singleton is not isolated and reachable".to_string())
    })
    .unwrap();
    seed.local_protocol.tell(LocalCommand::Ping(21)).unwrap();
    seed.local_deliveries
        .expect_msg_eq(21, Duration::from_secs(1))
        .unwrap();
    peer.local_protocol.tell(LocalCommand::Ping(22)).unwrap();

    seed.cluster.cluster().leave_self().unwrap();
    let mut attempt: u8 = 2;
    await_assert(Duration::from_secs(4), Duration::from_millis(25), || {
        let value = attempt;
        attempt = attempt.wrapping_add(1);
        peer.singleton.tell(Command::Ping(value)).unwrap();
        peer.deliveries
            .expect_msg(Duration::from_millis(75))
            .map(|_| ())
            .map_err(|_| "singleton has not handed over to the peer".to_string())
    })
    .unwrap();
    peer.local_deliveries
        .expect_msg_eq(22, Duration::from_secs(2))
        .unwrap();
    await_assert(Duration::from_secs(3), Duration::from_millis(25), || {
        peer.secondary.tell(Command::Ping(111)).unwrap();
        peer.secondary_deliveries
            .expect_msg_eq(111, Duration::from_millis(75))
            .map_err(|_| "second singleton has not handed over to the peer".to_string())
    })
    .unwrap();

    peer.shutdown();
    seed.shutdown();
}
