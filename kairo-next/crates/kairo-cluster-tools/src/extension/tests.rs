use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kairo_cluster::{
    ClusterDaemonBootstrapSettings, ClusterGossipProcessSettings, ClusterMembershipMsg,
    DeadlineFailureDetectorSettings, Gossip, HeartbeatSenderSettings, MemberStatus,
    register_cluster_daemon, register_cluster_protocol_codecs,
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
use crate::{
    DistributedPubSubMediatorMsg, DistributedPubSubPublishReport, PubSubRegistryState,
    PubSubSubscribeAck, TopicName, TopicPublishMode, register_cluster_tools_protocol_codecs,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestMessage(String);

impl RemoteMessage for TestMessage {
    const MANIFEST: &'static str = "kairo.cluster-tools.test.DistributedPubSubExtensionMessage";
    const VERSION: u16 = 1;
}

struct TestMessageCodec;

impl MessageCodec<TestMessage> for TestMessageCodec {
    fn serializer_id(&self) -> u32 {
        19_101
    }

    fn encode(&self, message: &TestMessage) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::copy_from_slice(message.0.as_bytes()))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<TestMessage> {
        String::from_utf8(payload.to_vec())
            .map(TestMessage)
            .map_err(|error| SerializationError::Message(error.to_string()))
    }
}

struct ComposedPubSubNode {
    kit: ActorSystemTestKit,
    runtime: TcpRemoteActorRuntime,
    cluster: kairo_cluster::ClusterDaemonHandle,
    pubsub: DistributedPubSubHandle<TestMessage>,
    gossip_probe: TestProbe<Gossip>,
    connector_probe: TestProbe<DistributedPubSubConnectorSnapshot>,
    registry_probe: TestProbe<PubSubRegistryState>,
}

impl ComposedPubSubNode {
    fn start(
        system: &str,
        node_uid: u64,
        remote_uid: u64,
        seed_nodes: Vec<kairo_actor::Address>,
        registry: Arc<Registry>,
    ) -> Self {
        let kit = ActorSystemTestKit::new(system).unwrap();
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
                .with_config_digest(Some(Bytes::from_static(b"pubsub-extension")))
                .with_gossip_process_settings(
                    ClusterGossipProcessSettings::new(Duration::from_millis(15)).unwrap(),
                )
                .with_heartbeat_sender_settings(
                    HeartbeatSenderSettings::new(
                        5,
                        DeadlineFailureDetectorSettings::new(
                            Duration::from_millis(15),
                            Duration::from_millis(100),
                        )
                        .unwrap(),
                    )
                    .with_heartbeat_expected_response_after(Duration::from_millis(10)),
                ),
        )
        .unwrap();
        let pubsub_registration = register_distributed_pubsub(
            &mut builder,
            cluster_registration.clone(),
            DistributedPubSubSettings::default().with_gossip_interval(Duration::from_millis(20)),
        )
        .unwrap();
        let runtime = builder.bind().unwrap();
        let cluster = cluster_registration.activate(&runtime).unwrap();
        let pubsub = pubsub_registration.activate(&runtime).unwrap();
        assert!(
            kit.system()
                .has_extension::<DistributedPubSubExtension<TestMessage>>()
        );
        Self {
            gossip_probe: kit.create_probe("cluster-gossip").unwrap(),
            connector_probe: kit.create_probe("pubsub-connector").unwrap(),
            registry_probe: kit.create_probe("pubsub-registry").unwrap(),
            kit,
            runtime,
            cluster,
            pubsub,
        }
    }

    fn gossip(&self) -> Gossip {
        self.cluster
            .membership()
            .tell(ClusterMembershipMsg::SendCurrentGossip {
                reply_to: self.gossip_probe.actor_ref(),
            })
            .unwrap();
        self.gossip_probe
            .expect_msg(Duration::from_secs(1))
            .unwrap()
    }

    fn connector(&self) -> DistributedPubSubConnectorSnapshot {
        self.pubsub
            .connector()
            .tell(DistributedPubSubConnectorMsg::Snapshot {
                reply_to: self.connector_probe.actor_ref(),
            })
            .unwrap();
        self.connector_probe
            .expect_msg(Duration::from_secs(1))
            .unwrap()
    }

    fn registry(&self) -> PubSubRegistryState {
        self.pubsub
            .mediator()
            .tell(DistributedPubSubMediatorMsg::GetRegistry {
                reply_to: self.registry_probe.actor_ref(),
            })
            .unwrap();
        self.registry_probe
            .expect_msg(Duration::from_secs(1))
            .unwrap()
    }

    fn shutdown(self) {
        self.kit.system().stop(self.cluster.root());
        self.runtime.shutdown().unwrap();
        self.kit.shutdown(Duration::from_secs(1)).unwrap();
    }
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_remote_protocol_codecs(&mut registry).unwrap();
    register_cluster_protocol_codecs(&mut registry).unwrap();
    register_cluster_tools_protocol_codecs(&mut registry).unwrap();
    registry
        .register::<TestMessage, _>(TestMessageCodec)
        .unwrap();
    Arc::new(registry)
}

#[test]
fn settings_reject_invalid_composed_runtime_values() {
    assert!(
        DistributedPubSubSettings::default()
            .with_gossip_interval(Duration::ZERO)
            .validate()
            .is_err()
    );
    assert!(
        DistributedPubSubSettings::default()
            .with_max_delta_entries(0)
            .validate()
            .is_err()
    );
    assert!(
        DistributedPubSubSettings::default()
            .with_role(" ")
            .validate()
            .is_err()
    );
}

#[test]
fn composed_extension_converges_subscription_and_publishes_remotely() {
    let registry = registry();
    let seed = ComposedPubSubNode::start("pubsub-extension-seed", 1, 101, vec![], registry.clone());
    await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
        (seed
            .gossip()
            .member(seed.cluster.self_node())
            .map(|member| member.status)
            == Some(MemberStatus::Up))
        .then_some(())
        .ok_or_else(|| "pubsub seed has not formed".to_string())
    })
    .unwrap();
    let peer = ComposedPubSubNode::start(
        "pubsub-extension-peer",
        2,
        102,
        vec![seed.cluster.self_node().address.clone()],
        registry,
    );
    await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
        let seed_peers = seed.connector().peers;
        let peer_peers = peer.connector().peers;
        (seed_peers == vec![peer.cluster.self_node().clone()]
            && peer_peers == vec![seed.cluster.self_node().clone()])
        .then_some(())
        .ok_or_else(|| "pubsub connectors have not derived cluster peers".to_string())
    })
    .unwrap();

    let topic = TopicName::new("orders");
    let subscriber = seed.kit.create_probe::<TestMessage>("subscriber").unwrap();
    let ack = seed
        .kit
        .create_probe::<PubSubSubscribeAck>("subscribe-ack")
        .unwrap();
    seed.pubsub
        .mediator()
        .tell(DistributedPubSubMediatorMsg::Subscribe {
            topic: topic.clone(),
            subscriber: subscriber.actor_ref(),
            reply_to: Some(ack.actor_ref()),
        })
        .unwrap();
    ack.expect_msg(Duration::from_secs(1)).unwrap();

    await_assert(Duration::from_secs(3), Duration::from_millis(20), || {
        (peer.registry().broadcast_targets(&topic, false) == vec![seed.cluster.self_node().clone()])
            .then_some(())
            .ok_or_else(|| "subscription has not converged to peer".to_string())
    })
    .unwrap();

    let report = peer
        .kit
        .create_probe::<DistributedPubSubPublishReport>("publish-report")
        .unwrap();
    peer.pubsub
        .mediator()
        .tell(DistributedPubSubMediatorMsg::Publish {
            topic,
            message: TestMessage("ship".to_string()),
            mode: TopicPublishMode::Broadcast,
            reply_to: Some(report.actor_ref()),
        })
        .unwrap();
    assert!(
        report
            .expect_msg(Duration::from_secs(1))
            .unwrap()
            .delivery
            .is_success()
    );
    subscriber
        .expect_msg_eq(TestMessage("ship".to_string()), Duration::from_secs(1))
        .unwrap();

    peer.shutdown();
    seed.shutdown();
}
