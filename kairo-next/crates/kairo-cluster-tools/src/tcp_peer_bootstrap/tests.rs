mod support;

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{Address, Props, Recipient};
use kairo_cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus,
    UniqueAddress,
};
use kairo_remote::{RemoteOutbound, RemoteSettings};
use kairo_testkit::{ActorSystemTestKit, MultiNodeTestKit};

use super::{
    ClusterToolsTcpPeerBootstrap, ClusterToolsTcpPeerBootstrapSettings,
    ClusterToolsTcpPeerConnectorSettings,
};
use crate::{
    ClusterToolsTcpPeerConnectorSnapshot, DistributedPubSubMediatorMsg, LocalPubSubMsg,
    PubSubRemoteDeliveryOutbound, TopicName, TopicPublishMode,
};

use support::*;

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn assert_pubsub_publish(
    probes: &ClusterToolsInboundProbes,
    expected_topic: TopicName,
    expected_message: TestMessage,
) {
    match probes.mediator.expect_msg(Duration::from_secs(1)).unwrap() {
        DistributedPubSubMediatorMsg::LocalDelivery(LocalPubSubMsg::Publish {
            topic,
            message,
            mode,
            reply_to,
        }) => {
            assert_eq!(topic, expected_topic);
            assert_eq!(message, expected_message);
            assert_eq!(mode, TopicPublishMode::Broadcast);
            assert!(reply_to.is_none());
        }
        _ => panic!("expected pubsub publish delivery"),
    }
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
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
        settings,
        move |self_node| inbound_for("bootstrap", kit_ref, registry, self_node),
    )
    .unwrap();

    assert_eq!(bootstrap.self_node().uid, 1);
    assert_eq!(
        bootstrap.local_address().system(),
        "cluster-tools-peer-bootstrap"
    );
    assert!(
        bootstrap
            .connector()
            .path()
            .as_str()
            .starts_with("kairo://cluster-tools-peer-bootstrap/system/tools-peer#")
    );
    assert!(!bootstrap.connector().is_stopped());

    run_bootstrap_shutdown(&kit, bootstrap.connector());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_two_nodes_install_peer_routes_from_cluster_membership() {
    let _guard = bootstrap_socket_test_lock();
    let nodes = MultiNodeTestKit::new([
        "cluster-tools-bootstrap-sender",
        "cluster-tools-bootstrap-receiver",
    ])
    .unwrap();
    let sender_kit = nodes.kit("cluster-tools-bootstrap-sender").unwrap();
    let receiver_kit = nodes.kit("cluster-tools-bootstrap-receiver").unwrap();
    let registry = registry();
    let sender_runtime = bind_runtime(
        "cluster-tools-bootstrap-sender",
        1,
        11,
        sender_kit,
        registry.clone(),
    );
    let receiver_runtime = bind_runtime(
        "cluster-tools-bootstrap-receiver",
        2,
        22,
        receiver_kit,
        registry,
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let receiver_cache = receiver_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(receiver_kit, "receiver-publisher", receiver_node.clone());
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
    let sender_snapshots = nodes
        .create_probe_on::<ClusterToolsTcpPeerConnectorSnapshot>(
            "cluster-tools-bootstrap-sender",
            "sender-snapshots",
        )
        .unwrap();
    let receiver_snapshots = nodes
        .create_probe_on::<ClusterToolsTcpPeerConnectorSnapshot>(
            "cluster-tools-bootstrap-receiver",
            "receiver-snapshots",
        )
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
    assert_eq!(sender_cache.route_count(), 1);
    assert_eq!(receiver_cache.route_count(), 1);

    run_bootstrap_shutdown(sender_kit, sender_bootstrap.connector());
    assert_eq!(sender_cache.route_count(), 0);
    sender_publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([
                Member::new(sender_node.clone(), Vec::new()).with_status(MemberStatus::Up)
            ]),
        ))
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(sender_kit.system().dead_letters().is_empty());
    assert_eq!(sender_cache.route_count(), 0);

    run_bootstrap_shutdown(receiver_kit, receiver_bootstrap.connector());
    assert_eq!(receiver_cache.route_count(), 0);
    nodes.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_coordinated_shutdown_stops_connector_after_live_route() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-shutdown-sender").unwrap();
    let receiver_kit =
        ActorSystemTestKit::new("cluster-tools-bootstrap-shutdown-receiver").unwrap();
    let registry = registry();
    let sender_runtime = bind_runtime(
        "cluster-tools-bootstrap-shutdown-sender",
        1,
        11,
        &sender_kit,
        registry.clone(),
    );
    let receiver_runtime = bind_runtime(
        "cluster-tools-bootstrap-shutdown-receiver",
        2,
        22,
        &receiver_kit,
        registry,
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let receiver_cache = receiver_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(&receiver_kit, "receiver-publisher", receiver_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let receiver_cluster = Cluster::new(receiver_publisher.clone());
    let settings = ClusterToolsTcpPeerBootstrapSettings::new()
        .with_connector_settings(
            ClusterToolsTcpPeerConnectorSettings::new(Duration::from_millis(25))
                .unwrap()
                .with_automatic_retry_ticks(false),
        )
        .with_shutdown_timeout(Duration::from_secs(1));

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
    assert_eq!(sender_cache.route_count(), 1);
    assert_eq!(receiver_cache.route_count(), 1);

    let sender_connector = sender_bootstrap.connector().clone();
    sender_kit
        .system()
        .run_coordinated_shutdown(
            "cluster-tools bootstrap shutdown test",
            Duration::from_secs(1),
        )
        .unwrap();
    assert!(sender_connector.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(sender_cache.route_count(), 0);

    run_bootstrap_shutdown(&receiver_kit, receiver_bootstrap.connector());
    assert_eq!(receiver_cache.route_count(), 0);
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_installed_peer_route_delivers_pubsub_publish_to_receiver() {
    let _guard = bootstrap_socket_test_lock();
    let nodes = MultiNodeTestKit::new([
        "cluster-tools-bootstrap-deliver-sender",
        "cluster-tools-bootstrap-deliver-receiver",
    ])
    .unwrap();
    let sender_kit = nodes.kit("cluster-tools-bootstrap-deliver-sender").unwrap();
    let receiver_kit = nodes
        .kit("cluster-tools-bootstrap-deliver-receiver")
        .unwrap();
    let registry = registry();
    let sender_runtime = bind_runtime(
        "cluster-tools-bootstrap-deliver-sender",
        1,
        11,
        sender_kit,
        registry.clone(),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let (receiver_runtime, receiver_probes) = bind_runtime_with_probes(
        "cluster-tools-bootstrap-deliver-receiver",
        2,
        22,
        receiver_kit,
        registry.clone(),
    );
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(receiver_kit, "receiver-publisher", receiver_node.clone());
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

    run_bootstrap_shutdown(sender_kit, sender_bootstrap.connector());
    run_bootstrap_shutdown(receiver_kit, receiver_bootstrap.connector());
    nodes.shutdown(Duration::from_secs(1)).unwrap();
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
fn bootstrap_clears_pending_reconnect_when_peer_leaves_before_retry() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tools-bootstrap-remove-pending-sender").unwrap();
    let registry = registry();
    let sender_runtime = bind_runtime(
        "cluster-tools-bootstrap-remove-pending-sender",
        1,
        11,
        &sender_kit,
        registry,
    );
    let sender_node = sender_runtime.self_node().clone();
    let missing_node = UniqueAddress::new(
        Address::new(
            "kairo",
            "cluster-tools-bootstrap-remove-pending-missing",
            Some("127.0.0.1".to_string()),
            Some(unused_port()),
        ),
        2,
    );
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ClusterToolsTcpPeerBootstrapSettings::new()
        .with_connector_settings(
            ClusterToolsTcpPeerConnectorSettings::new(Duration::from_millis(25))
                .unwrap()
                .with_automatic_retry_ticks(false),
        )
        .with_connector_name("sender-tools-peer");

    let sender_bootstrap = ClusterToolsTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings,
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ClusterToolsTcpPeerConnectorSnapshot>("sender-snapshots")
        .unwrap();

    publish_gossip(
        &sender_publisher,
        up_gossip([sender_node.clone(), missing_node.clone()]),
    );
    await_connector_pending_reconnect(
        sender_bootstrap.connector(),
        &sender_snapshots,
        &missing_node,
    );

    publish_gossip(&sender_publisher, up_gossip([sender_node]));
    await_connector_no_routes_or_pending(sender_bootstrap.connector(), &sender_snapshots);

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_reinstalls_peer_route_for_replacement_unique_address() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-replace-sender").unwrap();
    let old_receiver_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-replace-old").unwrap();
    let new_receiver_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-replace-new").unwrap();
    let registry = registry();
    let sender_runtime = bind_runtime(
        "cluster-tools-bootstrap-replace-sender",
        1,
        11,
        &sender_kit,
        registry.clone(),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let (old_receiver_runtime, old_receiver_probes) = bind_runtime_with_probes(
        "cluster-tools-bootstrap-replace-old",
        2,
        22,
        &old_receiver_kit,
        registry.clone(),
    );
    let (new_receiver_runtime, new_receiver_probes) = bind_runtime_with_probes(
        "cluster-tools-bootstrap-replace-new",
        3,
        33,
        &new_receiver_kit,
        registry.clone(),
    );
    let sender_node = sender_runtime.self_node().clone();
    let old_receiver_node = old_receiver_runtime.self_node().clone();
    let new_receiver_node = new_receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ClusterToolsTcpPeerBootstrapSettings::new().with_connector_settings(
        ClusterToolsTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    );

    let sender_bootstrap = ClusterToolsTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings.with_connector_name("sender-tools-peer"),
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ClusterToolsTcpPeerConnectorSnapshot>("sender-snapshots")
        .unwrap();

    publish_gossip(
        &sender_publisher,
        up_gossip([sender_node.clone(), old_receiver_node.clone()]),
    );
    await_connector_route(
        sender_bootstrap.connector(),
        &sender_snapshots,
        &old_receiver_node,
    );

    let sender_outbound = Arc::new(sender_cache.clone()) as Arc<dyn RemoteOutbound>;
    let old_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        old_receiver_node.clone(),
        registry.clone(),
        sender_outbound.clone(),
    );
    old_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 13 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &old_receiver_probes,
        TopicName::new("orders"),
        TestMessage { value: 13 },
    );

    publish_gossip(&sender_publisher, up_gossip([sender_node.clone()]));
    await_connector_no_routes(sender_bootstrap.connector(), &sender_snapshots);

    let old_peer_error = old_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 21 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .expect_err("old peer route should reject sends after removal");
    assert!(
        old_peer_error
            .reason()
            .contains("no remote association route"),
        "unexpected old-peer send error: {old_peer_error:?}"
    );
    old_receiver_probes
        .mediator
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    publish_gossip(
        &sender_publisher,
        up_gossip([sender_node.clone(), new_receiver_node.clone()]),
    );
    await_connector_route(
        sender_bootstrap.connector(),
        &sender_snapshots,
        &new_receiver_node,
    );

    let new_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        new_receiver_node.clone(),
        registry,
        sender_outbound,
    );
    new_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 34 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &new_receiver_probes,
        TopicName::new("orders"),
        TestMessage { value: 34 },
    );

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    old_receiver_runtime.shutdown().unwrap();
    new_receiver_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    old_receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    new_receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_sender_keeps_remaining_pubsub_route_delivering_after_peer_removed() {
    let _guard = bootstrap_socket_test_lock();
    let first_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-reduce-first").unwrap();
    let second_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-reduce-second").unwrap();
    let third_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-reduce-third").unwrap();
    let registry = registry();
    let first_runtime = bind_runtime(
        "cluster-tools-bootstrap-reduce-first",
        1,
        11,
        &first_kit,
        registry.clone(),
    );
    let first_cache = first_runtime.association_cache().clone();
    let (second_runtime, second_probes) = bind_runtime_with_probes(
        "cluster-tools-bootstrap-reduce-second",
        2,
        22,
        &second_kit,
        registry.clone(),
    );
    let (third_runtime, third_probes) = bind_runtime_with_probes(
        "cluster-tools-bootstrap-reduce-third",
        3,
        33,
        &third_kit,
        registry.clone(),
    );
    let first_node = first_runtime.self_node().clone();
    let second_node = second_runtime.self_node().clone();
    let third_node = third_runtime.self_node().clone();
    let first_publisher = spawn_publisher(&first_kit, "first-publisher", first_node.clone());
    let first_cluster = Cluster::new(first_publisher.clone());
    let settings = ClusterToolsTcpPeerBootstrapSettings::new().with_connector_settings(
        ClusterToolsTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    );

    let first_bootstrap = ClusterToolsTcpPeerBootstrap::spawn_with_runtime(
        first_kit.system(),
        first_cluster,
        first_runtime,
        settings.with_connector_name("first-tools-peer"),
    )
    .unwrap();
    let first_snapshots = first_kit
        .create_probe::<ClusterToolsTcpPeerConnectorSnapshot>("first-snapshots")
        .unwrap();

    publish_gossip(
        &first_publisher,
        up_gossip([first_node.clone(), second_node.clone(), third_node.clone()]),
    );
    await_connector_routes(
        first_bootstrap.connector(),
        &first_snapshots,
        &[second_node.clone(), third_node.clone()],
    );
    assert_eq!(first_cache.route_count(), 2);

    let first_outbound = Arc::new(first_cache.clone()) as Arc<dyn RemoteOutbound>;
    let second_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        second_node.clone(),
        registry.clone(),
        first_outbound.clone(),
    );
    let third_outbound =
        PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(third_node, registry, first_outbound);

    second_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 21 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    third_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("invoices"),
            message: TestMessage { value: 34 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();

    assert_pubsub_publish(
        &second_probes,
        TopicName::new("orders"),
        TestMessage { value: 21 },
    );
    assert_pubsub_publish(
        &third_probes,
        TopicName::new("invoices"),
        TestMessage { value: 34 },
    );

    publish_gossip(
        &first_publisher,
        up_gossip([first_node.clone(), second_node.clone()]),
    );
    await_connector_route(first_bootstrap.connector(), &first_snapshots, &second_node);
    assert_eq!(first_cache.route_count(), 1);

    let removed_peer_error = third_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("invoices"),
            message: TestMessage { value: 89 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .expect_err("removed peer route should reject sends");
    assert!(
        removed_peer_error
            .reason()
            .contains("no remote association route"),
        "unexpected removed-peer send error: {removed_peer_error:?}"
    );
    third_probes
        .mediator
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    second_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 55 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &second_probes,
        TopicName::new("orders"),
        TestMessage { value: 55 },
    );

    run_bootstrap_shutdown(&first_kit, first_bootstrap.connector());
    assert_eq!(first_cache.route_count(), 0);
    second_runtime.shutdown().unwrap();
    third_runtime.shutdown().unwrap();
    first_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership() {
    let _guard = bootstrap_socket_test_lock();
    let first_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-first").unwrap();
    let second_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-second").unwrap();
    let third_kit = ActorSystemTestKit::new("cluster-tools-bootstrap-third").unwrap();
    let registry = registry();
    let (first_runtime, first_probes) = bind_runtime_with_probes(
        "cluster-tools-bootstrap-first",
        1,
        11,
        &first_kit,
        registry.clone(),
    );
    let first_cache = first_runtime.association_cache().clone();
    let (second_runtime, second_probes) = bind_runtime_with_probes(
        "cluster-tools-bootstrap-second",
        2,
        22,
        &second_kit,
        registry.clone(),
    );
    let second_cache = second_runtime.association_cache().clone();
    let (third_runtime, third_probes) = bind_runtime_with_probes(
        "cluster-tools-bootstrap-third",
        3,
        33,
        &third_kit,
        registry.clone(),
    );
    let third_cache = third_runtime.association_cache().clone();
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
    publish_gossip(&first_publisher, gossip.clone());

    await_connector_routes(
        first_bootstrap.connector(),
        &first_snapshots,
        &[second_node.clone(), third_node.clone()],
    );
    assert_eq!(first_cache.route_count(), 2);

    let first_outbound = Arc::new(first_cache.clone()) as Arc<dyn RemoteOutbound>;
    let second_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        second_node.clone(),
        registry.clone(),
        first_outbound.clone(),
    );
    let third_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        third_node.clone(),
        registry.clone(),
        first_outbound,
    );
    second_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 21 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    third_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("invoices"),
            message: TestMessage { value: 34 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();

    match second_probes
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
            assert_eq!(message, TestMessage { value: 21 });
            assert_eq!(mode, TopicPublishMode::Broadcast);
            assert!(reply_to.is_none());
        }
        _ => panic!("expected pubsub publish delivery at second node"),
    }
    match third_probes
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
            assert_eq!(topic, TopicName::new("invoices"));
            assert_eq!(message, TestMessage { value: 34 });
            assert_eq!(mode, TopicPublishMode::Broadcast);
            assert!(reply_to.is_none());
        }
        _ => panic!("expected pubsub publish delivery at third node"),
    }

    publish_gossip(&second_publisher, gossip.clone());
    publish_gossip(&third_publisher, gossip);

    await_connector_routes(
        second_bootstrap.connector(),
        &second_snapshots,
        &[first_node.clone(), third_node.clone()],
    );
    assert_eq!(second_cache.route_count(), 2);
    await_connector_routes(
        third_bootstrap.connector(),
        &third_snapshots,
        &[first_node.clone(), second_node.clone()],
    );
    assert_eq!(third_cache.route_count(), 2);

    let second_outbound = Arc::new(second_cache.clone()) as Arc<dyn RemoteOutbound>;
    let second_to_third = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        third_node.clone(),
        registry.clone(),
        second_outbound,
    );
    second_to_third
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("peer-orders"),
            message: TestMessage { value: 45 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &third_probes,
        TopicName::new("peer-orders"),
        TestMessage { value: 45 },
    );

    let third_outbound = Arc::new(third_cache.clone()) as Arc<dyn RemoteOutbound>;
    let third_to_second = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        second_node.clone(),
        registry.clone(),
        third_outbound,
    );
    third_to_second
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("peer-invoices"),
            message: TestMessage { value: 56 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &second_probes,
        TopicName::new("peer-invoices"),
        TestMessage { value: 56 },
    );

    let second_outbound = Arc::new(second_cache.clone()) as Arc<dyn RemoteOutbound>;
    let second_to_first = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        first_node.clone(),
        registry.clone(),
        second_outbound,
    );
    second_to_first
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("first-orders"),
            message: TestMessage { value: 67 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &first_probes,
        TopicName::new("first-orders"),
        TestMessage { value: 67 },
    );

    let third_outbound = Arc::new(third_cache.clone()) as Arc<dyn RemoteOutbound>;
    let third_to_first = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        first_node.clone(),
        registry.clone(),
        third_outbound,
    );
    third_to_first
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("first-invoices"),
            message: TestMessage { value: 78 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &first_probes,
        TopicName::new("first-invoices"),
        TestMessage { value: 78 },
    );

    let reduced_gossip = Gossip::from_members([
        Member::new(first_node.clone(), Vec::new()).with_status(MemberStatus::Up),
        Member::new(second_node.clone(), Vec::new()).with_status(MemberStatus::Up),
    ]);
    first_publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            reduced_gossip.clone(),
        ))
        .unwrap();
    second_publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(reduced_gossip))
        .unwrap();

    await_connector_routes(
        first_bootstrap.connector(),
        &first_snapshots,
        std::slice::from_ref(&second_node),
    );
    assert_eq!(first_cache.route_count(), 1);
    await_connector_routes(
        second_bootstrap.connector(),
        &second_snapshots,
        std::slice::from_ref(&first_node),
    );
    assert_eq!(second_cache.route_count(), 1);
    assert_eq!(third_cache.route_count(), 2);

    let removed_second_to_third_error = second_to_third
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("peer-orders-after-reduction"),
            message: TestMessage { value: 89 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .expect_err("second-to-third route should reject sends after third is removed");
    assert!(
        removed_second_to_third_error
            .reason()
            .contains("no remote association route"),
        "unexpected second-to-third send error: {removed_second_to_third_error:?}"
    );
    third_probes
        .mediator
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    let first_to_second_after_reduction = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        second_node.clone(),
        registry,
        Arc::new(first_cache.clone()) as Arc<dyn RemoteOutbound>,
    );
    first_to_second_after_reduction
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders-after-reduction"),
            message: TestMessage { value: 90 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &second_probes,
        TopicName::new("orders-after-reduction"),
        TestMessage { value: 90 },
    );

    second_to_first
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("first-after-reduction"),
            message: TestMessage { value: 91 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &first_probes,
        TopicName::new("first-after-reduction"),
        TestMessage { value: 91 },
    );

    run_bootstrap_shutdown(&first_kit, first_bootstrap.connector());
    assert_eq!(first_cache.route_count(), 0);
    run_bootstrap_shutdown(&second_kit, second_bootstrap.connector());
    assert_eq!(second_cache.route_count(), 0);
    run_bootstrap_shutdown(&third_kit, third_bootstrap.connector());
    assert_eq!(third_cache.route_count(), 0);
    first_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}
