use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{ActorRef, Address, PHASE_BEFORE_CLUSTER_SHUTDOWN, Props};
use kairo_cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus,
    UniqueAddress,
};
use kairo_remote::RemoteSettings;
use kairo_serialization::{MessageCodec, Registry, RemoteMessage, SerializationRegistry};
use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

use super::{
    ClusterToolsTcpPeerBootstrap, ClusterToolsTcpPeerBootstrapSettings,
    ClusterToolsTcpPeerConnectorMsg, ClusterToolsTcpPeerConnectorSettings,
    ClusterToolsTcpPeerRuntime,
};
use crate::{
    ClusterToolsSystemInbound, ClusterToolsTcpPeerConnectorSnapshot, DistributedPubSubMediatorMsg,
    PubSubGossipMsg, PubSubGossipWireInbound, PubSubRemoteDeliveryInbound, SingletonManagerMsg,
    SingletonManagerRemoteInbound, register_cluster_tools_protocol_codecs,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestMessage {
    value: u8,
}

impl RemoteMessage for TestMessage {
    const MANIFEST: &'static str = "kairo.cluster-tools.test.peer-bootstrap-message";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy)]
struct TestMessageCodec;

impl MessageCodec<TestMessage> for TestMessageCodec {
    fn serializer_id(&self) -> u32 {
        59_205
    }

    fn encode(&self, message: &TestMessage) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value]))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<TestMessage> {
        Ok(TestMessage { value: payload[0] })
    }
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_cluster_tools_protocol_codecs(&mut registry).unwrap();
    registry
        .register::<TestMessage, _>(TestMessageCodec)
        .unwrap();
    Arc::new(registry)
}

fn inbound_for(
    name: &str,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
    self_node: UniqueAddress,
) -> ClusterToolsSystemInbound<TestMessage> {
    let gossip = kit
        .create_probe::<PubSubGossipMsg>(format!("{name}-gossip"))
        .unwrap();
    let mediator = kit
        .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>(format!("{name}-mediator"))
        .unwrap();
    let manager = kit
        .create_probe::<SingletonManagerMsg>(format!("{name}-singleton-manager"))
        .unwrap();
    ClusterToolsSystemInbound::new(self_node.clone())
        .with_pubsub_gossip(PubSubGossipWireInbound::new(
            self_node.clone(),
            registry.clone(),
            gossip.actor_ref(),
        ))
        .with_pubsub_delivery(PubSubRemoteDeliveryInbound::new(
            self_node.clone(),
            registry.clone(),
            mediator.actor_ref(),
        ))
        .with_singleton_manager(SingletonManagerRemoteInbound::new(
            self_node,
            registry,
            manager.actor_ref(),
        ))
}

#[test]
fn bootstrap_binds_connector_and_registers_coordinated_shutdown_stop() {
    let _guard = bootstrap_socket_test_lock();
    let kit = ActorSystemTestKit::new("cluster-tools-peer-bootstrap").unwrap();
    let registry = registry();
    let publisher_node = UniqueAddress::new(Address::local("cluster-tools-peer-bootstrap"), 1);
    let publisher = kit
        .system()
        .spawn(
            "publisher",
            Props::new(move || ClusterEventPublisher::new(publisher_node.clone())),
        )
        .unwrap();
    let cluster = Cluster::new(publisher);
    let settings = ClusterToolsTcpPeerBootstrapSettings::new()
        .with_connector_name("tools-peer")
        .with_shutdown_timeout(Duration::from_secs(1));
    let system = kit.system().clone();
    let kit_ref = &kit;

    let bootstrap = ClusterToolsTcpPeerBootstrap::bind_and_spawn(
        &system,
        cluster,
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        settings,
        move |self_node| inbound_for("bootstrap", kit_ref, registry, self_node),
    )
    .unwrap();

    assert_eq!(bootstrap.self_node().uid, 1);
    assert_eq!(
        bootstrap.local_address().system(),
        "cluster-tools-peer-bootstrap"
    );
    assert!(!bootstrap.connector().is_stopped());

    system
        .coordinated_shutdown()
        .run_from("test", Some(PHASE_BEFORE_CLUSTER_SHUTDOWN))
        .unwrap();

    assert!(bootstrap.connector().wait_for_stop(Duration::from_secs(1)));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_two_nodes_install_peer_routes_from_cluster_membership() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-receiver").unwrap();
    let registry = registry();
    let sender_runtime = bind_runtime(
        "cluster-tools-bootstrap-sender",
        1,
        11,
        &sender_kit,
        registry.clone(),
    );
    let receiver_runtime = bind_runtime(
        "cluster-tools-bootstrap-receiver",
        2,
        22,
        &receiver_kit,
        registry,
    );
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(&receiver_kit, "receiver-publisher", receiver_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let receiver_cluster = Cluster::new(receiver_publisher.clone());
    let settings = ClusterToolsTcpPeerBootstrapSettings::new().with_connector_settings(
        ClusterToolsTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    );

    let sender_bootstrap = ClusterToolsTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings.clone().with_connector_name("sender-tools-peer"),
    )
    .unwrap();
    let receiver_bootstrap = ClusterToolsTcpPeerBootstrap::spawn_with_runtime(
        receiver_kit.system(),
        receiver_cluster,
        receiver_runtime,
        settings.with_connector_name("receiver-tools-peer"),
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ClusterToolsTcpPeerConnectorSnapshot>("sender-snapshots")
        .unwrap();
    let receiver_snapshots = receiver_kit
        .create_probe::<ClusterToolsTcpPeerConnectorSnapshot>("receiver-snapshots")
        .unwrap();

    let gossip = Gossip::from_members([
        Member::new(sender_node.clone(), Vec::new()).with_status(MemberStatus::Up),
        Member::new(receiver_node.clone(), Vec::new()).with_status(MemberStatus::Up),
    ]);
    sender_publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(gossip.clone()))
        .unwrap();
    receiver_publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(gossip))
        .unwrap();

    await_connector_route(
        sender_bootstrap.connector(),
        &sender_snapshots,
        &receiver_node,
    );
    await_connector_route(
        receiver_bootstrap.connector(),
        &receiver_snapshots,
        &sender_node,
    );

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    run_bootstrap_shutdown(&receiver_kit, receiver_bootstrap.connector());
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership() {
    let _guard = bootstrap_socket_test_lock();
    let first_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-first").unwrap();
    let second_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-second").unwrap();
    let third_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-third").unwrap();
    let registry = registry();
    let first_runtime = bind_runtime(
        "cluster-tools-bootstrap-first",
        1,
        11,
        &first_kit,
        registry.clone(),
    );
    let second_runtime = bind_runtime(
        "cluster-tools-bootstrap-second",
        2,
        22,
        &second_kit,
        registry.clone(),
    );
    let third_runtime = bind_runtime("cluster-tools-bootstrap-third", 3, 33, &third_kit, registry);
    let first_node = first_runtime.self_node().clone();
    let second_node = second_runtime.self_node().clone();
    let third_node = third_runtime.self_node().clone();
    let first_publisher = spawn_publisher(&first_kit, "first-publisher", first_node.clone());
    let second_publisher = spawn_publisher(&second_kit, "second-publisher", second_node.clone());
    let third_publisher = spawn_publisher(&third_kit, "third-publisher", third_node.clone());
    let first_cluster = Cluster::new(first_publisher.clone());
    let second_cluster = Cluster::new(second_publisher.clone());
    let third_cluster = Cluster::new(third_publisher.clone());
    let settings = ClusterToolsTcpPeerBootstrapSettings::new().with_connector_settings(
        ClusterToolsTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    );

    let first_bootstrap = ClusterToolsTcpPeerBootstrap::spawn_with_runtime(
        first_kit.system(),
        first_cluster,
        first_runtime,
        settings.clone().with_connector_name("first-tools-peer"),
    )
    .unwrap();
    let second_bootstrap = ClusterToolsTcpPeerBootstrap::spawn_with_runtime(
        second_kit.system(),
        second_cluster,
        second_runtime,
        settings.clone().with_connector_name("second-tools-peer"),
    )
    .unwrap();
    let third_bootstrap = ClusterToolsTcpPeerBootstrap::spawn_with_runtime(
        third_kit.system(),
        third_cluster,
        third_runtime,
        settings.with_connector_name("third-tools-peer"),
    )
    .unwrap();
    let first_snapshots = first_kit
        .create_probe::<ClusterToolsTcpPeerConnectorSnapshot>("first-snapshots")
        .unwrap();
    let second_snapshots = second_kit
        .create_probe::<ClusterToolsTcpPeerConnectorSnapshot>("second-snapshots")
        .unwrap();
    let third_snapshots = third_kit
        .create_probe::<ClusterToolsTcpPeerConnectorSnapshot>("third-snapshots")
        .unwrap();

    let gossip = Gossip::from_members([
        Member::new(first_node.clone(), Vec::new()).with_status(MemberStatus::Up),
        Member::new(second_node.clone(), Vec::new()).with_status(MemberStatus::Up),
        Member::new(third_node.clone(), Vec::new()).with_status(MemberStatus::Up),
    ]);
    for publisher in [&first_publisher, &second_publisher, &third_publisher] {
        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(gossip.clone()))
            .unwrap();
    }

    await_connector_routes(
        first_bootstrap.connector(),
        &first_snapshots,
        &[second_node.clone(), third_node.clone()],
    );
    await_connector_routes(
        second_bootstrap.connector(),
        &second_snapshots,
        &[first_node.clone(), third_node.clone()],
    );
    await_connector_routes(
        third_bootstrap.connector(),
        &third_snapshots,
        &[first_node, second_node],
    );

    run_bootstrap_shutdown(&first_kit, first_bootstrap.connector());
    run_bootstrap_shutdown(&second_kit, second_bootstrap.connector());
    run_bootstrap_shutdown(&third_kit, third_bootstrap.connector());
    first_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}

fn bind_runtime(
    system: &str,
    node_uid: u64,
    system_uid: u64,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> ClusterToolsTcpPeerRuntime<TestMessage> {
    ClusterToolsTcpPeerRuntime::bind(
        system,
        node_uid,
        system_uid,
        RemoteSettings::new("127.0.0.1", 0),
        move |self_node| inbound_for(system, kit, registry, self_node),
    )
    .unwrap()
}

fn spawn_publisher(
    kit: &ActorSystemTestKit,
    name: &str,
    self_node: UniqueAddress,
) -> ActorRef<ClusterEventPublisherMsg> {
    kit.system()
        .spawn(
            name,
            Props::new(move || ClusterEventPublisher::new(self_node.clone())),
        )
        .unwrap()
}

fn await_connector_route(
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterToolsTcpPeerConnectorSnapshot>,
    expected_peer: &UniqueAddress,
) {
    await_connector_routes(connector, snapshots, std::slice::from_ref(expected_peer));
}

fn await_connector_routes(
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterToolsTcpPeerConnectorSnapshot>,
    expected_peers: &[UniqueAddress],
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterToolsTcpPeerConnectorMsg::Snapshot {
                    reply_to: snapshots.actor_ref(),
                })
                .map_err(|error| error.reason().to_string())?;
            let snapshot = snapshots
                .expect_msg(Duration::from_millis(100))
                .map_err(|error| error.to_string())?;
            let has_all_expected_peers = expected_peers.iter().all(|expected_peer| {
                snapshot
                    .active_targets
                    .iter()
                    .any(|target| target.node() == expected_peer)
            });
            if snapshot.route_count == expected_peers.len() && has_all_expected_peers {
                Ok(())
            } else {
                retry_pending_connector_routes(connector, &snapshot)?;
                Err(format!("unexpected connector snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap();
}

fn retry_pending_connector_routes(
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    snapshot: &ClusterToolsTcpPeerConnectorSnapshot,
) -> Result<(), String> {
    if let Some(now) = snapshot
        .pending_reconnects
        .iter()
        .map(|pending| pending.next_retry_at)
        .max()
    {
        connector
            .tell(ClusterToolsTcpPeerConnectorMsg::RetryDuePeerRoutes { now })
            .map_err(|error| error.reason().to_string())?;
    }
    Ok(())
}

fn run_bootstrap_shutdown(
    kit: &ActorSystemTestKit,
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
) {
    kit.system()
        .coordinated_shutdown()
        .run_from("test", Some(PHASE_BEFORE_CLUSTER_SHUTDOWN))
        .unwrap();
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
}

fn bootstrap_socket_test_lock() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap()
}
