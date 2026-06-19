mod support;

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorError, Address, Props};
use kairo_remote::{
    RemoteAssociationAddress, RemoteAssociationCache, RemoteOutbound, RemoteSettings,
};
use kairo_testkit::{ActorSystemTestKit, ManualTime, MultiNodeTestKit, await_assert};

use super::{
    ClusterTcpPeerBootstrap, ClusterTcpPeerBootstrapError, ClusterTcpPeerBootstrapIdentity,
    ClusterTcpPeerBootstrapSettings,
};
use crate::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, ClusterMembershipMsg,
    ClusterMembershipRemoteEnvelopeOutbound, ClusterMembershipWireOutbound,
    ClusterTcpPeerConnectorSettings, ClusterTcpPeerConnectorSnapshot, Gossip, Join, Member,
    MemberStatus, UniqueAddress,
};

use support::*;

#[derive(Default)]
struct NoopOutbound;

impl RemoteOutbound for NoopOutbound {
    fn send(&self, _envelope: kairo_serialization::RemoteEnvelope) -> kairo_remote::Result<()> {
        Ok(())
    }
}

struct LateRouteOnClose {
    cache: RemoteAssociationCache,
    late_address: RemoteAssociationAddress,
}

impl RemoteOutbound for LateRouteOnClose {
    fn send(&self, _envelope: kairo_serialization::RemoteEnvelope) -> kairo_remote::Result<()> {
        Ok(())
    }

    fn close(&self, _reason: &str) -> kairo_remote::Result<()> {
        self.cache
            .insert_route(self.late_address.clone(), Arc::new(NoopOutbound));
        Ok(())
    }
}

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn node(system: &str, port: u16, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
        uid,
    )
}

fn send_join_until_received(
    outbound: &ClusterMembershipWireOutbound,
    probes: &ClusterInboundProbes,
    join: Join,
    timeout: Duration,
) {
    let mut last_error = None;
    await_assert(
        timeout,
        Duration::from_millis(10),
        || -> Result<(), String> {
            if let Err(error) = outbound.send_membership(ClusterMembershipMsg::Join {
                join: join.clone(),
                reply_to: None,
            }) {
                last_error = Some(error.to_string());
            }

            match probes.membership.expect_msg(Duration::from_millis(50)) {
                Ok(ClusterMembershipMsg::Join {
                    join: received,
                    reply_to,
                }) => {
                    assert_eq!(received, join);
                    assert!(reply_to.is_none());
                    Ok(())
                }
                Ok(other) => panic!("expected cluster join, got {other:?}"),
                Err(error) => Err(format!(
                    "cluster join was not delivered yet: {error}; last send error: {last_error:?}"
                )),
            }
        },
    )
    .unwrap();
}

fn await_connector_route_without_manual_retry(
    time: &ManualTime,
    connector: &kairo_actor::ActorRef<crate::ClusterTcpPeerConnectorMsg>,
    snapshots: &kairo_testkit::TestProbe<ClusterTcpPeerConnectorSnapshot>,
    expected_peer: &UniqueAddress,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(crate::ClusterTcpPeerConnectorMsg::Snapshot {
                    reply_to: snapshots.actor_ref(),
                })
                .map_err(|error| error.reason().to_string())?;
            let snapshot = snapshots
                .expect_msg(Duration::from_millis(100))
                .map_err(|error| error.to_string())?;
            let has_expected_peer = snapshot
                .active_targets
                .iter()
                .any(|target| target.node() == expected_peer);
            if snapshot.route_count == 1
                && has_expected_peer
                && snapshot.pending_reconnects.is_empty()
            {
                Ok(())
            } else {
                time.advance_to_next();
                Err(format!("unexpected connector snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap();
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
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
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
fn bootstrap_stops_connector_when_shutdown_registration_fails() {
    let _guard = bootstrap_socket_test_lock();
    let kit = ActorSystemTestKit::new("cluster-bootstrap-registration-failure").unwrap();
    let publisher_node =
        UniqueAddress::new(Address::local("cluster-bootstrap-registration-failure"), 1);
    let publisher = spawn_publisher(&kit, "publisher", publisher_node);
    let cluster = Cluster::new(publisher);
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_name("cluster-peer")
    .with_connector_settings(
        ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    )
    .with_shutdown_task_name("");

    let error = match ClusterTcpPeerBootstrap::bind_and_spawn(
        kit.system(),
        cluster.clone(),
        ClusterTcpPeerBootstrapIdentity::new(1, 11),
        settings,
        |self_node, _cache| crate::ClusterSystemInbound::new(self_node),
    ) {
        Ok(_) => panic!("invalid shutdown task name should fail bootstrap"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        ClusterTcpPeerBootstrapError::Actor(ActorError::InvalidShutdownTaskName)
    ));
    let replacement = ClusterTcpPeerBootstrap::bind_and_spawn(
        kit.system(),
        cluster,
        ClusterTcpPeerBootstrapIdentity::new(2, 22),
        ClusterTcpPeerBootstrapSettings::new(
            RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
        )
        .with_connector_name("cluster-peer")
        .with_connector_settings(
            ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25))
                .unwrap()
                .with_automatic_retry_ticks(false),
        ),
        |self_node, _cache| crate::ClusterSystemInbound::new(self_node),
    )
    .expect("same connector name should be reusable after registration failure cleanup");

    run_bootstrap_shutdown(&kit, replacement.connector());
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
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
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
    await_cache_route_count(&sender_cache, 0);
    publish_gossip_and_wait(
        sender_kit,
        &sender_publisher,
        Gossip::from_members([
            Member::new(sender_node.clone(), Vec::new()).with_status(MemberStatus::Up)
        ]),
        "sender-after-shutdown-state",
    );
    assert!(sender_kit.system().dead_letters().is_empty());
    await_cache_route_count(&sender_cache, 0);
    run_bootstrap_shutdown(receiver_kit, receiver_bootstrap.connector());
    await_cache_route_count(&receiver_cache, 0);
    nodes.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_coordinated_shutdown_stops_connector_after_live_route() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-bootstrap-shutdown-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("cluster-bootstrap-shutdown-receiver").unwrap();
    let sender_runtime = bind_runtime("cluster-bootstrap-shutdown-sender", 1, 11, &sender_kit);
    let receiver_runtime =
        bind_runtime("cluster-bootstrap-shutdown-receiver", 2, 22, &receiver_kit);
    let sender_cache = sender_runtime.association_cache().clone();
    let receiver_cache = receiver_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(&receiver_kit, "receiver-publisher", receiver_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let receiver_cluster = Cluster::new(receiver_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    )
    .with_shutdown_timeout(Duration::from_secs(1));

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
        .run_coordinated_shutdown("cluster bootstrap shutdown test", Duration::from_secs(1))
        .unwrap();
    assert!(sender_connector.wait_for_stop(Duration::from_secs(1)));
    await_cache_route_count(&sender_cache, 0);

    run_bootstrap_shutdown(&receiver_kit, receiver_bootstrap.connector());
    await_cache_route_count(&receiver_cache, 0);
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_coordinated_shutdown_clears_late_route_registered_during_connector_stop() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-bootstrap-late-route-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("cluster-bootstrap-late-route-receiver").unwrap();
    let sender_runtime = bind_runtime("cluster-bootstrap-late-route-sender", 1, 11, &sender_kit);
    let receiver_runtime = bind_runtime(
        "cluster-bootstrap-late-route-receiver",
        2,
        22,
        &receiver_kit,
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    )
    .with_shutdown_timeout(Duration::from_secs(1));

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

    sender_publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([
                Member::new(sender_node.clone(), Vec::new()).with_status(MemberStatus::Up),
                Member::new(receiver_node.clone(), Vec::new()).with_status(MemberStatus::Up),
            ]),
        ))
        .unwrap();

    await_connector_route(
        sender_bootstrap.connector(),
        &sender_snapshots,
        &receiver_node,
    );
    assert_eq!(sender_cache.route_count(), 1);
    let initial_address = RemoteAssociationAddress::new(
        "kairo",
        "cluster-bootstrap-late-initial",
        "127.0.0.1",
        Some(2552),
    )
    .unwrap();
    let late_address =
        RemoteAssociationAddress::new("kairo", "cluster-bootstrap-late", "127.0.0.1", Some(2553))
            .unwrap();
    sender_cache.insert_route(
        initial_address,
        Arc::new(LateRouteOnClose {
            cache: sender_cache.clone(),
            late_address,
        }),
    );
    assert_eq!(sender_cache.route_count(), 2);

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    await_cache_route_count(&sender_cache, 0);

    receiver_runtime.shutdown().unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_shutdown_clears_adopted_existing_peer_route() {
    let _guard = bootstrap_socket_test_lock();
    let nodes = MultiNodeTestKit::new([
        "cluster-bootstrap-adopt-sender",
        "cluster-bootstrap-adopt-receiver",
    ])
    .unwrap();
    let sender_kit = nodes.kit("cluster-bootstrap-adopt-sender").unwrap();
    let receiver_kit = nodes.kit("cluster-bootstrap-adopt-receiver").unwrap();
    let registry = registry();
    let sender_runtime = bind_runtime("cluster-bootstrap-adopt-sender", 1, 11, sender_kit);
    let sender_cache = sender_runtime.association_cache().clone();
    let (receiver_runtime, receiver_probes) =
        bind_runtime_with_probes("cluster-bootstrap-adopt-receiver", 2, 22, receiver_kit);
    let receiver_cache = receiver_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    sender_runtime
        .runtime()
        .dial(receiver_runtime.local_address().clone())
        .unwrap();
    await_cache_route_count(&sender_cache, 1);
    await_cache_route_count(&receiver_cache, 1);
    let sender_publisher = spawn_publisher(sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(receiver_kit, "receiver-publisher", receiver_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let receiver_cluster = Cluster::new(receiver_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
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
            "cluster-bootstrap-adopt-sender",
            "sender-snapshots",
        )
        .unwrap();
    let receiver_snapshots = nodes
        .create_probe_on::<ClusterTcpPeerConnectorSnapshot>(
            "cluster-bootstrap-adopt-receiver",
            "receiver-snapshots",
        )
        .unwrap();

    let gossip = Gossip::from_members([
        Member::new(sender_node.clone(), Vec::new()).with_status(MemberStatus::Up),
        Member::new(receiver_node.clone(), Vec::new()).with_status(MemberStatus::Up),
    ]);
    publish_gossip(&sender_publisher, gossip.clone());
    publish_gossip(&receiver_publisher, gossip);

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

    let sender_outbound = Arc::new(sender_cache.clone()) as Arc<dyn RemoteOutbound>;
    let receiver_membership_outbound = ClusterMembershipWireOutbound::new(
        receiver_node.clone(),
        registry,
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(sender_outbound),
    );
    send_join_until_received(
        &receiver_membership_outbound,
        &receiver_probes,
        Join {
            node: sender_node.clone(),
            roles: vec!["before-adopted-route-shutdown".to_string()],
        },
        Duration::from_secs(1),
    );

    run_bootstrap_shutdown(sender_kit, sender_bootstrap.connector());
    await_cache_route_count(&sender_cache, 0);

    let removed_route_error = receiver_membership_outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: sender_node,
                roles: vec!["after-adopted-route-shutdown".to_string()],
            },
            reply_to: None,
        })
        .expect_err("adopted peer route should reject sends after bootstrap shutdown");
    assert!(
        removed_route_error
            .to_string()
            .contains("no remote association route"),
        "unexpected removed-route send error: {removed_route_error:?}"
    );
    receiver_probes
        .membership
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    run_bootstrap_shutdown(receiver_kit, receiver_bootstrap.connector());
    await_cache_route_count(&receiver_cache, 0);
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
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
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
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
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
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
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
fn bootstrap_automatic_retry_timer_installs_pending_peer_route() {
    let _guard = bootstrap_socket_test_lock();
    let (sender_kit, time) =
        ActorSystemTestKit::with_manual_time("cluster-bootstrap-automatic-retry-sender").unwrap();
    let missing_kit = ActorSystemTestKit::new("cluster-bootstrap-automatic-retry-missing").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_runtime(
        "cluster-bootstrap-automatic-retry-sender",
        1,
        11,
        &sender_kit,
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let missing_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let missing_node = node("cluster-bootstrap-automatic-retry-missing", missing_port, 2);
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(ClusterTcpPeerConnectorSettings::new(retry_interval).unwrap())
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

    let missing_runtime = bind_association_runtime_on_port(
        "cluster-bootstrap-automatic-retry-missing",
        2,
        22,
        missing_port,
        &missing_kit,
        registry,
    );
    await_connector_route_without_manual_retry(
        &time,
        sender_bootstrap.connector(),
        &sender_snapshots,
        &missing_node,
    );
    await_cache_route_count(&sender_cache, 1);

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    await_cache_route_count(&sender_cache, 0);
    missing_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    missing_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_preserves_successful_route_when_later_snapshot_dial_fails() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-bootstrap-partial-sender").unwrap();
    let bound_kit = ActorSystemTestKit::new("cluster-bootstrap-partial-bound").unwrap();
    let missing_kit = ActorSystemTestKit::new("cluster-bootstrap-partial-missing").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_runtime("cluster-bootstrap-partial-sender", 1, 11, &sender_kit);
    let sender_cache = sender_runtime.association_cache().clone();
    let bound_port = unused_port();
    let missing_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let bound_node = node("cluster-bootstrap-partial-bound", bound_port, 2);
    let missing_node = node("cluster-bootstrap-partial-missing", missing_port, 3);
    let (bound_runtime, bound_probes) = bind_association_runtime_on_port_with_probes(
        "cluster-bootstrap-partial-bound",
        2,
        22,
        bound_port,
        &bound_kit,
        registry.clone(),
    );
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ClusterTcpPeerConnectorSettings::new(retry_interval)
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
        up_gossip([
            sender_node.clone(),
            bound_node.clone(),
            missing_node.clone(),
        ]),
    );
    await_connector_routes_and_pending_reconnect(
        sender_bootstrap.connector(),
        &sender_snapshots,
        std::slice::from_ref(&bound_node),
        &missing_node,
    );
    await_cache_route_count(&sender_cache, 1);

    let membership_outbound = ClusterMembershipWireOutbound::new(
        bound_node.clone(),
        registry.clone(),
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(
            Arc::new(sender_cache.clone()) as Arc<dyn RemoteOutbound>
        ),
    );
    send_join_until_received(
        &membership_outbound,
        &bound_probes,
        Join {
            node: sender_node.clone(),
            roles: vec!["partial-active-route".to_string()],
        },
        Duration::from_secs(1),
    );

    let missing_runtime = bind_association_runtime_on_port(
        "cluster-bootstrap-partial-missing",
        3,
        33,
        missing_port,
        &missing_kit,
        registry,
    );
    await_connector_routes(
        sender_bootstrap.connector(),
        &sender_snapshots,
        &[bound_node, missing_node],
    );
    await_cache_route_count(&sender_cache, 2);

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    await_cache_route_count(&sender_cache, 0);
    bound_runtime.shutdown().unwrap();
    missing_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    bound_kit.shutdown(Duration::from_secs(1)).unwrap();
    missing_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_keeps_route_and_clears_pending_reconnect_when_peer_leaves_membership() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-bootstrap-mixed-shrink-sender").unwrap();
    let bound_kit = ActorSystemTestKit::new("cluster-bootstrap-mixed-shrink-bound").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_runtime("cluster-bootstrap-mixed-shrink-sender", 1, 11, &sender_kit);
    let sender_cache = sender_runtime.association_cache().clone();
    let bound_port = unused_port();
    let missing_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let bound_node = node("cluster-bootstrap-mixed-shrink-bound", bound_port, 2);
    let missing_node = node("cluster-bootstrap-mixed-shrink-missing", missing_port, 3);
    let (bound_runtime, bound_probes) = bind_association_runtime_on_port_with_probes(
        "cluster-bootstrap-mixed-shrink-bound",
        2,
        22,
        bound_port,
        &bound_kit,
        registry.clone(),
    );
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ClusterTcpPeerConnectorSettings::new(retry_interval)
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
        up_gossip([
            sender_node.clone(),
            bound_node.clone(),
            missing_node.clone(),
        ]),
    );
    await_connector_routes_and_pending_reconnect(
        sender_bootstrap.connector(),
        &sender_snapshots,
        std::slice::from_ref(&bound_node),
        &missing_node,
    );
    await_cache_route_count(&sender_cache, 1);

    publish_gossip(
        &sender_publisher,
        up_gossip([sender_node.clone(), bound_node.clone()]),
    );
    let snapshot = await_connector_routes_without_pending(
        sender_bootstrap.connector(),
        &sender_snapshots,
        std::slice::from_ref(&bound_node),
    );
    assert_eq!(snapshot.active_targets.len(), 1);
    assert_eq!(snapshot.active_targets[0].node(), &bound_node);
    assert!(snapshot.last_error.is_none());
    let report = snapshot
        .last_report
        .expect("membership shrink should record a route report");
    assert!(report.removed.is_empty());
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(report.skipped[0].node(), &missing_node);
    await_cache_route_count(&sender_cache, 1);

    let membership_outbound = ClusterMembershipWireOutbound::new(
        bound_node.clone(),
        registry,
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(
            Arc::new(sender_cache.clone()) as Arc<dyn RemoteOutbound>
        ),
    );
    send_join_until_received(
        &membership_outbound,
        &bound_probes,
        Join {
            node: sender_node.clone(),
            roles: vec!["mixed-shrink-active-route".to_string()],
        },
        Duration::from_secs(1),
    );

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    await_cache_route_count(&sender_cache, 0);
    bound_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    bound_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_coordinated_shutdown_stops_connector_with_pending_reconnect() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-bootstrap-shutdown-pending-sender").unwrap();
    let sender_runtime = bind_runtime(
        "cluster-bootstrap-shutdown-pending-sender",
        1,
        11,
        &sender_kit,
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let missing_node = UniqueAddress::new(
        Address::new(
            "kairo",
            "cluster-bootstrap-shutdown-pending-missing",
            Some("127.0.0.1".to_string()),
            Some(unused_port()),
        ),
        2,
    );
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
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

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    await_cache_route_count(&sender_cache, 0);

    publish_gossip_and_wait(
        &sender_kit,
        &sender_publisher,
        up_gossip([sender_node, missing_node]),
        "sender-pending-after-shutdown-state",
    );
    assert!(sender_kit.system().dead_letters().is_empty());
    await_cache_route_count(&sender_cache, 0);

    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_coordinated_shutdown_clears_route_and_pending_reconnect() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-bootstrap-shutdown-mixed-sender").unwrap();
    let bound_kit = ActorSystemTestKit::new("cluster-bootstrap-shutdown-mixed-bound").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_runtime(
        "cluster-bootstrap-shutdown-mixed-sender",
        1,
        11,
        &sender_kit,
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let bound_port = unused_port();
    let missing_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let bound_node = node("cluster-bootstrap-shutdown-mixed-bound", bound_port, 2);
    let missing_node = node("cluster-bootstrap-shutdown-mixed-missing", missing_port, 3);
    let bound_runtime = bind_association_runtime_on_port(
        "cluster-bootstrap-shutdown-mixed-bound",
        2,
        22,
        bound_port,
        &bound_kit,
        registry,
    );
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ClusterTcpPeerConnectorSettings::new(retry_interval)
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
        up_gossip([
            sender_node.clone(),
            bound_node.clone(),
            missing_node.clone(),
        ]),
    );
    await_connector_routes_and_pending_reconnect(
        sender_bootstrap.connector(),
        &sender_snapshots,
        std::slice::from_ref(&bound_node),
        &missing_node,
    );
    await_cache_route_count(&sender_cache, 1);

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    await_cache_route_count(&sender_cache, 0);

    publish_gossip_and_wait(
        &sender_kit,
        &sender_publisher,
        up_gossip([sender_node, bound_node, missing_node]),
        "sender-mixed-after-shutdown-state",
    );
    assert!(sender_kit.system().dead_letters().is_empty());
    await_cache_route_count(&sender_cache, 0);

    bound_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    bound_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_reinstalls_peer_route_for_replacement_unique_address() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-bootstrap-replace-sender").unwrap();
    let old_receiver_kit = ActorSystemTestKit::new("cluster-bootstrap-replace-old").unwrap();
    let new_receiver_kit = ActorSystemTestKit::new("cluster-bootstrap-replace-new").unwrap();
    let registry = registry();
    let sender_runtime = bind_runtime("cluster-bootstrap-replace-sender", 1, 11, &sender_kit);
    let sender_cache = sender_runtime.association_cache().clone();
    let (old_receiver_runtime, old_receiver_probes) =
        bind_runtime_with_probes("cluster-bootstrap-replace-old", 2, 22, &old_receiver_kit);
    let (new_receiver_runtime, new_receiver_probes) =
        bind_runtime_with_probes("cluster-bootstrap-replace-new", 3, 33, &new_receiver_kit);
    let sender_node = sender_runtime.self_node().clone();
    let old_receiver_node = old_receiver_runtime.self_node().clone();
    let new_receiver_node = new_receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
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

    let sender_outbound = Arc::new(sender_cache.clone()) as Arc<dyn RemoteOutbound>;
    let old_membership_outbound = ClusterMembershipWireOutbound::new(
        old_receiver_node.clone(),
        registry.clone(),
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(sender_outbound.clone()),
    );
    send_join_until_received(
        &old_membership_outbound,
        &old_receiver_probes,
        Join {
            node: sender_node.clone(),
            roles: vec!["before-replacement".to_string()],
        },
        Duration::from_secs(1),
    );

    publish_gossip(&sender_publisher, up_gossip([sender_node.clone()]));
    await_connector_no_routes(sender_bootstrap.connector(), &sender_snapshots);

    let old_peer_error = old_membership_outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: sender_node.clone(),
                roles: vec!["after-old-removed".to_string()],
            },
            reply_to: None,
        })
        .expect_err("old peer route should reject sends after removal");
    assert!(
        old_peer_error
            .to_string()
            .contains("no remote association route"),
        "unexpected old-peer send error: {old_peer_error:?}"
    );
    old_receiver_probes
        .membership
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

    let new_membership_outbound = ClusterMembershipWireOutbound::new(
        new_receiver_node.clone(),
        registry,
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(sender_outbound),
    );
    send_join_until_received(
        &new_membership_outbound,
        &new_receiver_probes,
        Join {
            node: sender_node.clone(),
            roles: vec!["after-replacement".to_string()],
        },
        Duration::from_secs(1),
    );

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    old_receiver_runtime.shutdown().unwrap();
    new_receiver_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    old_receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    new_receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_sender_keeps_remaining_membership_route_delivering_after_peer_removed() {
    let _guard = bootstrap_socket_test_lock();
    let first_kit = ActorSystemTestKit::new("cluster-bootstrap-reduce-first").unwrap();
    let second_kit = ActorSystemTestKit::new("cluster-bootstrap-reduce-second").unwrap();
    let third_kit = ActorSystemTestKit::new("cluster-bootstrap-reduce-third").unwrap();
    let registry = registry();
    let first_runtime = bind_runtime("cluster-bootstrap-reduce-first", 1, 11, &first_kit);
    let first_cache = first_runtime.association_cache().clone();
    let (second_runtime, second_probes) =
        bind_runtime_with_probes("cluster-bootstrap-reduce-second", 2, 22, &second_kit);
    let (third_runtime, third_probes) =
        bind_runtime_with_probes("cluster-bootstrap-reduce-third", 3, 33, &third_kit);
    let first_node = first_runtime.self_node().clone();
    let second_node = second_runtime.self_node().clone();
    let third_node = third_runtime.self_node().clone();
    let first_publisher = spawn_publisher(&first_kit, "first-publisher", first_node.clone());
    let first_cluster = Cluster::new(first_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    );

    let first_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
        first_kit.system(),
        first_cluster,
        first_runtime,
        settings.with_connector_name("first-cluster-peer"),
    )
    .unwrap();
    let first_snapshots = first_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("first-snapshots")
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
    send_join_until_received(
        &second_membership_outbound,
        &second_probes,
        Join {
            node: first_node.clone(),
            roles: vec!["before-removal-second".to_string()],
        },
        Duration::from_secs(1),
    );
    send_join_until_received(
        &third_membership_outbound,
        &third_probes,
        Join {
            node: first_node.clone(),
            roles: vec!["before-removal-third".to_string()],
        },
        Duration::from_secs(1),
    );

    publish_gossip(
        &first_publisher,
        up_gossip([first_node.clone(), second_node.clone()]),
    );
    await_connector_route(first_bootstrap.connector(), &first_snapshots, &second_node);
    await_cache_route_count(&first_cache, 1);

    let removed_peer_error = third_membership_outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: first_node.clone(),
                roles: vec!["after-removal-third".to_string()],
            },
            reply_to: None,
        })
        .expect_err("removed peer route should reject sends");
    assert!(
        removed_peer_error
            .to_string()
            .contains("no remote association route"),
        "unexpected removed-peer send error: {removed_peer_error:?}"
    );
    third_probes
        .membership
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    send_join_until_received(
        &second_membership_outbound,
        &second_probes,
        Join {
            node: first_node.clone(),
            roles: vec!["after-removal".to_string()],
        },
        Duration::from_secs(1),
    );

    run_bootstrap_shutdown(&first_kit, first_bootstrap.connector());
    await_cache_route_count(&first_cache, 0);
    second_runtime.shutdown().unwrap();
    third_runtime.shutdown().unwrap();
    first_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_clears_peer_routes_when_self_member_is_removed() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-bootstrap-self-remove-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("cluster-bootstrap-self-remove-receiver").unwrap();
    let registry = registry();
    let sender_runtime = bind_runtime("cluster-bootstrap-self-remove-sender", 1, 11, &sender_kit);
    let sender_cache = sender_runtime.association_cache().clone();
    let (receiver_runtime, receiver_probes) = bind_runtime_with_probes(
        "cluster-bootstrap-self-remove-receiver",
        2,
        22,
        &receiver_kit,
    );
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
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
        up_gossip([sender_node.clone(), receiver_node.clone()]),
    );
    await_connector_route(
        sender_bootstrap.connector(),
        &sender_snapshots,
        &receiver_node,
    );
    await_cache_route_count(&sender_cache, 1);

    let membership_outbound = ClusterMembershipWireOutbound::new(
        receiver_node.clone(),
        registry,
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(
            Arc::new(sender_cache.clone()) as Arc<dyn RemoteOutbound>
        ),
    );
    send_join_until_received(
        &membership_outbound,
        &receiver_probes,
        Join {
            node: sender_node.clone(),
            roles: vec!["before-self-removal".to_string()],
        },
        Duration::from_secs(1),
    );

    publish_gossip(&sender_publisher, up_gossip([receiver_node]));
    await_connector_no_routes(sender_bootstrap.connector(), &sender_snapshots);
    await_cache_route_count(&sender_cache, 0);

    let removed_route_error = membership_outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: sender_node,
                roles: vec!["after-self-removal".to_string()],
            },
            reply_to: None,
        })
        .expect_err("self-removed bootstrap connector should clear outbound routes");
    assert!(
        removed_route_error
            .to_string()
            .contains("no remote association route"),
        "unexpected self-removal send error: {removed_route_error:?}"
    );
    receiver_probes
        .membership
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    await_cache_route_count(&sender_cache, 0);
    receiver_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership() {
    let _guard = bootstrap_socket_test_lock();
    let first_kit = ActorSystemTestKit::new("cluster-bootstrap-first").unwrap();
    let second_kit = ActorSystemTestKit::new("cluster-bootstrap-second").unwrap();
    let third_kit = ActorSystemTestKit::new("cluster-bootstrap-third").unwrap();
    let registry = registry();
    let (first_runtime, first_probes) =
        bind_runtime_with_probes("cluster-bootstrap-first", 1, 11, &first_kit);
    let first_cache = first_runtime.association_cache().clone();
    let (second_runtime, second_probes) =
        bind_runtime_with_probes("cluster-bootstrap-second", 2, 22, &second_kit);
    let second_cache = second_runtime.association_cache().clone();
    let (third_runtime, third_probes) =
        bind_runtime_with_probes("cluster-bootstrap-third", 3, 33, &third_kit);
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
    let settings = ClusterTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
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
    assert_eq!(first_cache.route_count(), 2);

    let first_outbound = Arc::new(first_cache.clone()) as Arc<dyn RemoteOutbound>;
    let second_membership_outbound = ClusterMembershipWireOutbound::new(
        second_node.clone(),
        registry.clone(),
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(first_outbound.clone()),
    );
    let third_membership_outbound = ClusterMembershipWireOutbound::new(
        third_node.clone(),
        registry.clone(),
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
    assert_eq!(second_cache.route_count(), 2);
    await_connector_routes(
        third_bootstrap.connector(),
        &third_snapshots,
        &[first_node.clone(), second_node.clone()],
    );
    assert_eq!(third_cache.route_count(), 2);

    let second_outbound = Arc::new(second_cache.clone()) as Arc<dyn RemoteOutbound>;
    let second_to_third_outbound = ClusterMembershipWireOutbound::new(
        third_node.clone(),
        registry.clone(),
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(second_outbound),
    );
    send_join_until_received(
        &second_to_third_outbound,
        &third_probes,
        Join {
            node: second_node.clone(),
            roles: vec!["second-to-third".to_string()],
        },
        Duration::from_secs(1),
    );

    let third_outbound = Arc::new(third_cache.clone()) as Arc<dyn RemoteOutbound>;
    let third_to_second_outbound = ClusterMembershipWireOutbound::new(
        second_node.clone(),
        registry.clone(),
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(third_outbound),
    );
    send_join_until_received(
        &third_to_second_outbound,
        &second_probes,
        Join {
            node: third_node.clone(),
            roles: vec!["third-to-second".to_string()],
        },
        Duration::from_secs(1),
    );

    let second_outbound = Arc::new(second_cache.clone()) as Arc<dyn RemoteOutbound>;
    let second_to_first_outbound = ClusterMembershipWireOutbound::new(
        first_node.clone(),
        registry.clone(),
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(second_outbound),
    );
    send_join_until_received(
        &second_to_first_outbound,
        &first_probes,
        Join {
            node: second_node.clone(),
            roles: vec!["second-to-first".to_string()],
        },
        Duration::from_secs(1),
    );

    let third_outbound = Arc::new(third_cache.clone()) as Arc<dyn RemoteOutbound>;
    let third_to_first_outbound = ClusterMembershipWireOutbound::new(
        first_node.clone(),
        registry.clone(),
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(third_outbound),
    );
    send_join_until_received(
        &third_to_first_outbound,
        &first_probes,
        Join {
            node: third_node.clone(),
            roles: vec!["third-to-first".to_string()],
        },
        Duration::from_secs(1),
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
        .tell(ClusterEventPublisherMsg::PublishChanges(
            reduced_gossip.clone(),
        ))
        .unwrap();
    third_publisher
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
    await_connector_no_routes(third_bootstrap.connector(), &third_snapshots);
    await_cache_route_count(&first_cache, 1);
    await_cache_route_count(&second_cache, 1);
    await_cache_route_count(&third_cache, 0);

    let removed_second_to_third_error = second_to_third_outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: second_node.clone(),
                roles: vec!["second-to-third-after-reduction".to_string()],
            },
            reply_to: None,
        })
        .expect_err("second-to-third route should reject sends after third is removed");
    assert!(
        removed_second_to_third_error
            .to_string()
            .contains("no remote association route"),
        "unexpected second-to-third send error: {removed_second_to_third_error:?}"
    );
    third_probes
        .membership
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    send_join_until_received(
        &second_membership_outbound,
        &second_probes,
        Join {
            node: first_node.clone(),
            roles: vec!["first-to-second-after-reduction".to_string()],
        },
        Duration::from_secs(1),
    );

    send_join_until_received(
        &second_to_first_outbound,
        &first_probes,
        Join {
            node: second_node.clone(),
            roles: vec!["second-to-first-after-reduction".to_string()],
        },
        Duration::from_secs(1),
    );

    run_bootstrap_shutdown(&first_kit, first_bootstrap.connector());
    await_cache_route_count(&first_cache, 0);
    run_bootstrap_shutdown(&second_kit, second_bootstrap.connector());
    await_cache_route_count(&second_cache, 0);
    run_bootstrap_shutdown(&third_kit, third_bootstrap.connector());
    await_cache_route_count(&third_cache, 0);
    first_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}
