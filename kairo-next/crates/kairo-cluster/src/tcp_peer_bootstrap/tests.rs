mod support;

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{Address, Props};
use kairo_remote::{RemoteOutbound, RemoteSettings};
use kairo_testkit::{ActorSystemTestKit, MultiNodeTestKit};

use super::{
    ClusterTcpPeerBootstrap, ClusterTcpPeerBootstrapIdentity, ClusterTcpPeerBootstrapSettings,
};
use crate::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, ClusterMembershipMsg,
    ClusterMembershipRemoteEnvelopeOutbound, ClusterMembershipWireOutbound,
    ClusterTcpPeerConnectorSettings, ClusterTcpPeerConnectorSnapshot, Gossip, Join, Member,
    MemberStatus, UniqueAddress,
};

use support::*;

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[test]
fn bootstrap_binds_connector_and_registers_coordinated_shutdown_stop() {
    let _guard = bootstrap_socket_test_lock();
    let kit = ActorSystemTestKit::new("cluster-peer-bootstrap").unwrap();
    let publisher_node = UniqueAddress::new(Address::local("cluster-peer-bootstrap"), 1);
    let publisher = kit
        .system()
        .spawn(
            "publisher",
            Props::new(move || ClusterEventPublisher::new(publisher_node.clone())),
        )
        .unwrap();
    let cluster = Cluster::new(publisher);
    let settings = ClusterTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
        .with_connector_name("cluster-peer")
        .with_connector_settings(
            ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25)).unwrap(),
        )
        .with_shutdown_timeout(Duration::from_secs(1));
    let identity = ClusterTcpPeerBootstrapIdentity::new(1, 11);

    let bootstrap = ClusterTcpPeerBootstrap::bind_and_spawn(
        kit.system(),
        cluster,
        identity,
        settings,
        |self_node, _cache| crate::ClusterSystemInbound::new(self_node),
    )
    .unwrap();

    assert_eq!(bootstrap.self_node().uid, 1);
    assert_eq!(bootstrap.local_address().system(), "cluster-peer-bootstrap");
    assert!(
        bootstrap
            .connector()
            .path()
            .as_str()
            .starts_with("kairo://cluster-peer-bootstrap/system/cluster-peer#")
    );
    assert!(!bootstrap.connector().is_stopped());

    run_bootstrap_shutdown(&kit, bootstrap.connector());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_two_nodes_install_peer_routes_from_cluster_membership() {
    let _guard = bootstrap_socket_test_lock();
    let nodes =
        MultiNodeTestKit::new(["cluster-bootstrap-sender", "cluster-bootstrap-receiver"]).unwrap();
    let sender_kit = nodes.kit("cluster-bootstrap-sender").unwrap();
    let receiver_kit = nodes.kit("cluster-bootstrap-receiver").unwrap();
    let sender_runtime = bind_runtime("cluster-bootstrap-sender", 1, 11, sender_kit);
    let receiver_runtime = bind_runtime("cluster-bootstrap-receiver", 2, 22, receiver_kit);
    let sender_cache = sender_runtime.association_cache().clone();
    let receiver_cache = receiver_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(receiver_kit, "receiver-publisher", receiver_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let receiver_cluster = Cluster::new(receiver_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
        .with_connector_settings(
            ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25))
                .unwrap()
                .with_automatic_retry_ticks(false),
        );

    let sender_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings.clone().with_connector_name("sender-cluster-peer"),
    )
    .unwrap();
    let receiver_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
        receiver_kit.system(),
        receiver_cluster,
        receiver_runtime,
        settings.with_connector_name("receiver-cluster-peer"),
    )
    .unwrap();
    let sender_snapshots = nodes
        .create_probe_on::<ClusterTcpPeerConnectorSnapshot>(
            "cluster-bootstrap-sender",
            "sender-snapshots",
        )
        .unwrap();
    let receiver_snapshots = nodes
        .create_probe_on::<ClusterTcpPeerConnectorSnapshot>(
            "cluster-bootstrap-receiver",
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
    run_bootstrap_shutdown(receiver_kit, receiver_bootstrap.connector());
    assert_eq!(receiver_cache.route_count(), 0);
    nodes.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_installed_peer_route_delivers_membership_join_to_receiver() {
    let _guard = bootstrap_socket_test_lock();
    let nodes = MultiNodeTestKit::new([
        "cluster-bootstrap-deliver-sender",
        "cluster-bootstrap-deliver-receiver",
    ])
    .unwrap();
    let sender_kit = nodes.kit("cluster-bootstrap-deliver-sender").unwrap();
    let receiver_kit = nodes.kit("cluster-bootstrap-deliver-receiver").unwrap();
    let registry = registry();
    let sender_runtime = bind_runtime("cluster-bootstrap-deliver-sender", 1, 11, sender_kit);
    let sender_cache = sender_runtime.association_cache().clone();
    let (receiver_runtime, receiver_probes) =
        bind_runtime_with_probes("cluster-bootstrap-deliver-receiver", 2, 22, receiver_kit);
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(receiver_kit, "receiver-publisher", receiver_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let receiver_cluster = Cluster::new(receiver_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
        .with_connector_settings(
            ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25))
                .unwrap()
                .with_automatic_retry_ticks(false),
        );

    let sender_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings.clone().with_connector_name("sender-cluster-peer"),
    )
    .unwrap();
    let receiver_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
        receiver_kit.system(),
        receiver_cluster,
        receiver_runtime,
        settings.with_connector_name("receiver-cluster-peer"),
    )
    .unwrap();
    let sender_snapshots = nodes
        .create_probe_on::<ClusterTcpPeerConnectorSnapshot>(
            "cluster-bootstrap-deliver-sender",
            "sender-snapshots",
        )
        .unwrap();
    let receiver_snapshots = nodes
        .create_probe_on::<ClusterTcpPeerConnectorSnapshot>(
            "cluster-bootstrap-deliver-receiver",
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

    let membership_outbound = ClusterMembershipWireOutbound::new(
        receiver_node,
        registry,
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(
            Arc::new(sender_cache) as Arc<dyn RemoteOutbound>
        ),
    );
    membership_outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: sender_node.clone(),
                roles: vec!["backend".to_string()],
            },
            reply_to: None,
        })
        .unwrap();

    match receiver_probes
        .membership
        .expect_msg(Duration::from_secs(1))
        .unwrap()
    {
        ClusterMembershipMsg::Join { join, reply_to } => {
            assert_eq!(join.node, sender_node);
            assert_eq!(join.roles, vec!["backend".to_string()]);
            assert!(reply_to.is_none());
        }
        _ => panic!("expected cluster join"),
    }

    run_bootstrap_shutdown(sender_kit, sender_bootstrap.connector());
    run_bootstrap_shutdown(receiver_kit, receiver_bootstrap.connector());
    nodes.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_removes_peer_route_when_cluster_membership_drops_peer() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-bootstrap-remove-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("cluster-bootstrap-remove-receiver").unwrap();
    let sender_runtime = bind_runtime("cluster-bootstrap-remove-sender", 1, 11, &sender_kit);
    let receiver_runtime = bind_runtime("cluster-bootstrap-remove-receiver", 2, 22, &receiver_kit);
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(&receiver_kit, "receiver-publisher", receiver_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let receiver_cluster = Cluster::new(receiver_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
        .with_connector_settings(
            ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25))
                .unwrap()
                .with_automatic_retry_ticks(false),
        );

    let sender_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings.clone().with_connector_name("sender-cluster-peer"),
    )
    .unwrap();
    let receiver_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
        receiver_kit.system(),
        receiver_cluster,
        receiver_runtime,
        settings.with_connector_name("receiver-cluster-peer"),
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("sender-snapshots")
        .unwrap();
    let receiver_snapshots = receiver_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("receiver-snapshots")
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
    let sender_kit = ActorSystemTestKit::new("cluster-bootstrap-remove-pending-sender").unwrap();
    let sender_runtime = bind_runtime(
        "cluster-bootstrap-remove-pending-sender",
        1,
        11,
        &sender_kit,
    );
    let sender_node = sender_runtime.self_node().clone();
    let missing_node = UniqueAddress::new(
        Address::new(
            "kairo",
            "cluster-bootstrap-remove-pending-missing",
            Some("127.0.0.1".to_string()),
            Some(unused_port()),
        ),
        2,
    );
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
        .with_connector_settings(
            ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25))
                .unwrap()
                .with_automatic_retry_ticks(false),
        )
        .with_connector_name("sender-cluster-peer");

    let sender_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings,
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("sender-snapshots")
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
    let sender_kit = ActorSystemTestKit::new("cluster-bootstrap-replace-sender").unwrap();
    let old_receiver_kit = ActorSystemTestKit::new("cluster-bootstrap-replace-old").unwrap();
    let new_receiver_kit = ActorSystemTestKit::new("cluster-bootstrap-replace-new").unwrap();
    let sender_runtime = bind_runtime("cluster-bootstrap-replace-sender", 1, 11, &sender_kit);
    let old_receiver_runtime =
        bind_runtime("cluster-bootstrap-replace-old", 2, 22, &old_receiver_kit);
    let new_receiver_runtime =
        bind_runtime("cluster-bootstrap-replace-new", 3, 33, &new_receiver_kit);
    let sender_node = sender_runtime.self_node().clone();
    let old_receiver_node = old_receiver_runtime.self_node().clone();
    let new_receiver_node = new_receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
        .with_connector_settings(
            ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25))
                .unwrap()
                .with_automatic_retry_ticks(false),
        );

    let sender_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings.with_connector_name("sender-cluster-peer"),
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("sender-snapshots")
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

    publish_gossip(&sender_publisher, up_gossip([sender_node.clone()]));
    await_connector_no_routes(sender_bootstrap.connector(), &sender_snapshots);

    publish_gossip(
        &sender_publisher,
        up_gossip([sender_node.clone(), new_receiver_node.clone()]),
    );
    await_connector_route(
        sender_bootstrap.connector(),
        &sender_snapshots,
        &new_receiver_node,
    );

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    old_receiver_runtime.shutdown().unwrap();
    new_receiver_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    old_receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    new_receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership() {
    let _guard = bootstrap_socket_test_lock();
    let first_kit = ActorSystemTestKit::new("cluster-bootstrap-first").unwrap();
    let second_kit = ActorSystemTestKit::new("cluster-bootstrap-second").unwrap();
    let third_kit = ActorSystemTestKit::new("cluster-bootstrap-third").unwrap();
    let registry = registry();
    let first_runtime = bind_runtime("cluster-bootstrap-first", 1, 11, &first_kit);
    let first_cache = first_runtime.association_cache().clone();
    let (second_runtime, second_probes) =
        bind_runtime_with_probes("cluster-bootstrap-second", 2, 22, &second_kit);
    let (third_runtime, third_probes) =
        bind_runtime_with_probes("cluster-bootstrap-third", 3, 33, &third_kit);
    let first_node = first_runtime.self_node().clone();
    let second_node = second_runtime.self_node().clone();
    let third_node = third_runtime.self_node().clone();
    let first_publisher = spawn_publisher(&first_kit, "first-publisher", first_node.clone());
    let second_publisher = spawn_publisher(&second_kit, "second-publisher", second_node.clone());
    let third_publisher = spawn_publisher(&third_kit, "third-publisher", third_node.clone());
    let first_cluster = Cluster::new(first_publisher.clone());
    let second_cluster = Cluster::new(second_publisher.clone());
    let third_cluster = Cluster::new(third_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
        .with_connector_settings(
            ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25))
                .unwrap()
                .with_automatic_retry_ticks(false),
        );

    let first_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
        first_kit.system(),
        first_cluster,
        first_runtime,
        settings.clone().with_connector_name("first-cluster-peer"),
    )
    .unwrap();
    let second_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
        second_kit.system(),
        second_cluster,
        second_runtime,
        settings.clone().with_connector_name("second-cluster-peer"),
    )
    .unwrap();
    let third_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
        third_kit.system(),
        third_cluster,
        third_runtime,
        settings.with_connector_name("third-cluster-peer"),
    )
    .unwrap();
    let first_snapshots = first_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("first-snapshots")
        .unwrap();
    let second_snapshots = second_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("second-snapshots")
        .unwrap();
    let third_snapshots = third_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("third-snapshots")
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

    let first_outbound = Arc::new(first_cache) as Arc<dyn RemoteOutbound>;
    let second_membership_outbound = ClusterMembershipWireOutbound::new(
        second_node.clone(),
        registry.clone(),
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(first_outbound.clone()),
    );
    let third_membership_outbound = ClusterMembershipWireOutbound::new(
        third_node.clone(),
        registry,
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(first_outbound),
    );
    second_membership_outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: first_node.clone(),
                roles: vec!["backend".to_string()],
            },
            reply_to: None,
        })
        .unwrap();
    third_membership_outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: first_node.clone(),
                roles: vec!["frontend".to_string()],
            },
            reply_to: None,
        })
        .unwrap();

    match second_probes
        .membership
        .expect_msg(Duration::from_secs(1))
        .unwrap()
    {
        ClusterMembershipMsg::Join { join, reply_to } => {
            assert_eq!(join.node, first_node);
            assert_eq!(join.roles, vec!["backend".to_string()]);
            assert!(reply_to.is_none());
        }
        _ => panic!("expected cluster join at second node"),
    }
    match third_probes
        .membership
        .expect_msg(Duration::from_secs(1))
        .unwrap()
    {
        ClusterMembershipMsg::Join { join, reply_to } => {
            assert_eq!(join.node, first_node);
            assert_eq!(join.roles, vec!["frontend".to_string()]);
            assert!(reply_to.is_none());
        }
        _ => panic!("expected cluster join at third node"),
    }

    publish_gossip(&second_publisher, gossip.clone());
    publish_gossip(&third_publisher, gossip);

    await_connector_routes(
        second_bootstrap.connector(),
        &second_snapshots,
        &[first_node.clone(), third_node.clone()],
    );
    await_connector_routes(
        third_bootstrap.connector(),
        &third_snapshots,
        &[first_node.clone(), second_node.clone()],
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
    await_connector_routes(
        second_bootstrap.connector(),
        &second_snapshots,
        std::slice::from_ref(&first_node),
    );

    run_bootstrap_shutdown(&first_kit, first_bootstrap.connector());
    run_bootstrap_shutdown(&second_kit, second_bootstrap.connector());
    run_bootstrap_shutdown(&third_kit, third_bootstrap.connector());
    first_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}
