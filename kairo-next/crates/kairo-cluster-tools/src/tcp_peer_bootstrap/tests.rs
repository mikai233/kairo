mod support;

use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{Address, Props, Recipient};
use kairo_cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus,
    UniqueAddress,
};
use kairo_remote::{RemoteOutbound, RemoteSettings};
use kairo_testkit::ActorSystemTestKit;

use super::{
    ClusterToolsTcpPeerBootstrap, ClusterToolsTcpPeerBootstrapSettings,
    ClusterToolsTcpPeerConnectorSettings,
};
use crate::{
    ClusterToolsTcpPeerConnectorSnapshot, DistributedPubSubMediatorMsg, LocalPubSubMsg,
    PubSubRemoteDeliveryOutbound, TopicName, TopicPublishMode,
};

use support::*;

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

    run_bootstrap_shutdown(&kit, bootstrap.connector());
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
fn bootstrap_installed_peer_route_delivers_pubsub_publish_to_receiver() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-deliver-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-deliver-receiver").unwrap();
    let registry = registry();
    let sender_runtime = bind_runtime(
        "cluster-tools-bootstrap-deliver-sender",
        1,
        11,
        &sender_kit,
        registry.clone(),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let (receiver_runtime, receiver_probes) = bind_runtime_with_probes(
        "cluster-tools-bootstrap-deliver-receiver",
        2,
        22,
        &receiver_kit,
        registry.clone(),
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

    let outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        receiver_node,
        registry,
        Arc::new(sender_cache) as Arc<dyn RemoteOutbound>,
    );
    outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 77 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();

    match receiver_probes
        .mediator
        .expect_msg(Duration::from_secs(1))
        .unwrap()
    {
        DistributedPubSubMediatorMsg::LocalDelivery(LocalPubSubMsg::Publish {
            topic,
            message,
            mode,
            reply_to,
        }) => {
            assert_eq!(topic, TopicName::new("orders"));
            assert_eq!(message, TestMessage { value: 77 });
            assert_eq!(mode, TopicPublishMode::Broadcast);
            assert!(reply_to.is_none());
        }
        _ => panic!("expected pubsub publish delivery"),
    }

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    run_bootstrap_shutdown(&receiver_kit, receiver_bootstrap.connector());
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_removes_peer_route_when_cluster_membership_drops_peer() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-remove-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-remove-receiver").unwrap();
    let registry = registry();
    let sender_runtime = bind_runtime(
        "cluster-tools-bootstrap-remove-sender",
        1,
        11,
        &sender_kit,
        registry.clone(),
    );
    let receiver_runtime = bind_runtime(
        "cluster-tools-bootstrap-remove-receiver",
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

    let both_nodes = Gossip::from_members([
        Member::new(sender_node.clone(), Vec::new()).with_status(MemberStatus::Up),
        Member::new(receiver_node.clone(), Vec::new()).with_status(MemberStatus::Up),
    ]);
    sender_publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(both_nodes.clone()))
        .unwrap();
    receiver_publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(both_nodes))
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

    sender_publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([
                Member::new(sender_node.clone(), Vec::new()).with_status(MemberStatus::Up)
            ]),
        ))
        .unwrap();

    await_connector_no_routes(sender_bootstrap.connector(), &sender_snapshots);

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
