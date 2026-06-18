mod support;

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorError, Address, Props, Recipient};
use kairo_cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus,
    UniqueAddress,
};
use kairo_remote::RemoteSettings;
use kairo_serialization::RemoteMessage;
use kairo_testkit::{ActorSystemTestKit, ManualTime, MultiNodeTestKit, await_assert};

use super::{
    ReplicatorTcpPeerBootstrap, ReplicatorTcpPeerBootstrapError,
    ReplicatorTcpPeerBootstrapIdentity, ReplicatorTcpPeerBootstrapSettings,
};
use crate::{
    ReplicaId, ReplicatorRead, ReplicatorRemoteReplyReceiver, ReplicatorRemoteRequestReceiver,
    ReplicatorTcpPeerConnectorSettings, ReplicatorTcpPeerConnectorSnapshot,
    replicator_actor_ref_for,
};

use support::*;

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

fn send_read_until_received(
    outbound: &impl Recipient<ReplicatorRead>,
    requests: &RecordingRequests,
    read: ReplicatorRead,
    timeout: Duration,
) -> Vec<(ReplicaId, kairo_serialization::RemoteEnvelope)> {
    send_read_until_count_received(outbound, requests, read, 1, timeout)
}

fn send_read_until_count_received(
    outbound: &impl Recipient<ReplicatorRead>,
    requests: &RecordingRequests,
    read: ReplicatorRead,
    expected_count: usize,
    timeout: Duration,
) -> Vec<(ReplicaId, kairo_serialization::RemoteEnvelope)> {
    let mut last_error = None;
    await_assert(
        timeout,
        Duration::from_millis(10),
        || -> Result<Vec<(ReplicaId, kairo_serialization::RemoteEnvelope)>, String> {
            if let Err(error) = outbound.tell(read.clone()) {
                last_error = Some(error.reason().to_string());
            }
            let received = requests.wait_for_len(expected_count, Duration::from_millis(50));
            if received.len() >= expected_count {
                Ok(received)
            } else {
                Err(format!(
                    "remote request count was {}, expected at least {expected_count}; last send error: {last_error:?}",
                    received.len()
                ))
            }
        },
    )
    .unwrap()
}

fn send_read_until_key_received(
    outbound: &impl Recipient<ReplicatorRead>,
    requests: &RecordingRequests,
    registry: &kairo_serialization::Registry,
    read: ReplicatorRead,
    key: &str,
    timeout: Duration,
) -> (
    ReplicaId,
    kairo_serialization::RemoteEnvelope,
    ReplicatorRead,
) {
    let mut last_error = None;
    await_assert(
        timeout,
        Duration::from_millis(10),
        || -> Result<
            (
                ReplicaId,
                kairo_serialization::RemoteEnvelope,
                ReplicatorRead,
            ),
            String,
        > {
            if let Err(error) = outbound.tell(read.clone()) {
                last_error = Some(error.reason().to_string());
            }
            for (from, envelope) in requests.wait_for_len(1, Duration::from_millis(50)) {
                if envelope.message.manifest.as_str() != ReplicatorRead::MANIFEST {
                    continue;
                }
                let decoded = registry
                    .deserialize::<ReplicatorRead>(envelope.message.clone())
                    .expect("recorded read request should decode");
                if decoded.key == key {
                    return Ok((from, envelope, decoded));
                }
            }
            Err(format!(
                "remote request `{key}` was not delivered yet; last send error: {last_error:?}"
            ))
        },
    )
    .unwrap()
}

fn await_connector_route_without_manual_retry(
    time: &ManualTime,
    connector: &kairo_actor::ActorRef<crate::ReplicatorTcpPeerConnectorMsg>,
    snapshots: &kairo_testkit::TestProbe<ReplicatorTcpPeerConnectorSnapshot>,
    expected_peer: &UniqueAddress,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(crate::ReplicatorTcpPeerConnectorMsg::Snapshot {
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
    let kit = ActorSystemTestKit::new("ddata-peer-bootstrap").unwrap();
    let publisher_node = UniqueAddress::new(Address::local("ddata-peer-bootstrap"), 1);
    let publisher = kit
        .system()
        .spawn(
            "publisher",
            Props::new(move || ClusterEventPublisher::new(publisher_node.clone())),
        )
        .unwrap();
    let cluster = Cluster::new(publisher);
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_name("ddata-peer")
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25)).unwrap(),
    )
    .with_shutdown_timeout(Duration::from_secs(1));
    let identity = ReplicatorTcpPeerBootstrapIdentity::new(1, 11, ReplicaId::new("remote"));

    let bootstrap = ReplicatorTcpPeerBootstrap::bind_and_spawn(
        kit.system(),
        cluster,
        identity,
        settings,
        Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
        Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
    )
    .unwrap();

    assert_eq!(bootstrap.self_node().uid, 1);
    assert_eq!(bootstrap.local_address().system(), "ddata-peer-bootstrap");
    assert!(
        bootstrap
            .connector()
            .path()
            .as_str()
            .starts_with("kairo://ddata-peer-bootstrap/system/ddata-peer#")
    );
    assert!(!bootstrap.connector().is_stopped());

    run_bootstrap_shutdown(&kit, bootstrap.connector());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_stops_connector_when_shutdown_registration_fails() {
    let _guard = bootstrap_socket_test_lock();
    let kit = ActorSystemTestKit::new("ddata-bootstrap-registration-failure").unwrap();
    let runtime = bind_runtime(
        "ddata-bootstrap-registration-failure",
        1,
        11,
        ReplicaId::new("sender"),
    );
    let sender_node = runtime.self_node().clone();
    let publisher = spawn_publisher(&kit, "sender-publisher", sender_node);
    let cluster = Cluster::new(publisher);
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_name("ddata-peer")
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    )
    .with_shutdown_task_name("");

    let error = match ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        kit.system(),
        cluster.clone(),
        runtime,
        settings,
    ) {
        Ok(_) => panic!("invalid shutdown task name should fail bootstrap"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        ReplicatorTcpPeerBootstrapError::Actor(ActorError::InvalidShutdownTaskName)
    ));
    let replacement_runtime = bind_runtime(
        "ddata-bootstrap-registration-failure",
        2,
        22,
        ReplicaId::new("sender-replacement"),
    );
    let replacement = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        kit.system(),
        cluster,
        replacement_runtime,
        ReplicatorTcpPeerBootstrapSettings::new(
            RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
        )
        .with_connector_name("ddata-peer")
        .with_connector_settings(
            ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
                .unwrap()
                .with_automatic_retry_ticks(false),
        ),
    )
    .expect("same connector name should be reusable after registration failure cleanup");

    run_bootstrap_shutdown(&kit, replacement.connector());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_two_nodes_install_peer_routes_from_cluster_membership() {
    let _guard = bootstrap_socket_test_lock();
    let nodes =
        MultiNodeTestKit::new(["ddata-bootstrap-sender", "ddata-bootstrap-receiver"]).unwrap();
    let sender_kit = nodes.kit("ddata-bootstrap-sender").unwrap();
    let receiver_kit = nodes.kit("ddata-bootstrap-receiver").unwrap();
    let sender_runtime = bind_runtime("ddata-bootstrap-sender", 1, 11, ReplicaId::new("receiver"));
    let receiver_runtime =
        bind_runtime("ddata-bootstrap-receiver", 2, 22, ReplicaId::new("sender"));
    let sender_cache = sender_runtime.association_cache().clone();
    let receiver_cache = receiver_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(receiver_kit, "receiver-publisher", receiver_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let receiver_cluster = Cluster::new(receiver_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    );

    let sender_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings.clone().with_connector_name("sender-ddata-peer"),
    )
    .unwrap();
    let receiver_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        receiver_kit.system(),
        receiver_cluster,
        receiver_runtime,
        settings.with_connector_name("receiver-ddata-peer"),
    )
    .unwrap();
    let sender_snapshots = nodes
        .create_probe_on::<ReplicatorTcpPeerConnectorSnapshot>(
            "ddata-bootstrap-sender",
            "sender-snapshots",
        )
        .unwrap();
    let receiver_snapshots = nodes
        .create_probe_on::<ReplicatorTcpPeerConnectorSnapshot>(
            "ddata-bootstrap-receiver",
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
    let sender_kit = ActorSystemTestKit::new("ddata-bootstrap-shutdown-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("ddata-bootstrap-shutdown-receiver").unwrap();
    let sender_runtime = bind_runtime(
        "ddata-bootstrap-shutdown-sender",
        1,
        11,
        ReplicaId::new("receiver"),
    );
    let receiver_runtime = bind_runtime(
        "ddata-bootstrap-shutdown-receiver",
        2,
        22,
        ReplicaId::new("sender"),
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
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    )
    .with_shutdown_timeout(Duration::from_secs(1));

    let sender_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings.clone().with_connector_name("sender-ddata-peer"),
    )
    .unwrap();
    let receiver_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        receiver_kit.system(),
        receiver_cluster,
        receiver_runtime,
        settings.with_connector_name("receiver-ddata-peer"),
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("sender-snapshots")
        .unwrap();
    let receiver_snapshots = receiver_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("receiver-snapshots")
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
        .run_coordinated_shutdown("ddata bootstrap shutdown test", Duration::from_secs(1))
        .unwrap();
    assert!(sender_connector.wait_for_stop(Duration::from_secs(1)));
    await_cache_route_count(&sender_cache, 0);

    run_bootstrap_shutdown(&receiver_kit, receiver_bootstrap.connector());
    await_cache_route_count(&receiver_cache, 0);
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_shutdown_clears_adopted_existing_peer_route() {
    let _guard = bootstrap_socket_test_lock();
    let registry = registry();
    let nodes = MultiNodeTestKit::new([
        "ddata-bootstrap-adopt-sender",
        "ddata-bootstrap-adopt-receiver",
    ])
    .unwrap();
    let sender_kit = nodes.kit("ddata-bootstrap-adopt-sender").unwrap();
    let receiver_kit = nodes.kit("ddata-bootstrap-adopt-receiver").unwrap();
    let receiver_requests = Arc::new(RecordingRequests::default());
    let sender_runtime = bind_runtime(
        "ddata-bootstrap-adopt-sender",
        1,
        11,
        ReplicaId::new("receiver"),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let sender_settings = sender_runtime.runtime().settings().clone();
    let receiver_runtime = bind_runtime_with_requests(
        "ddata-bootstrap-adopt-receiver",
        2,
        22,
        ReplicaId::new("sender"),
        receiver_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
    );
    let receiver_cache = receiver_runtime.association_cache().clone();
    let receiver_settings = receiver_runtime.runtime().settings().clone();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    sender_runtime
        .runtime()
        .dial(receiver_runtime.runtime().local_address().clone())
        .unwrap();
    await_cache_route_count(&sender_cache, 1);
    await_cache_route_count(&receiver_cache, 1);
    let sender_publisher = spawn_publisher(sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(receiver_kit, "receiver-publisher", receiver_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let receiver_cluster = Cluster::new(receiver_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    );

    let sender_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings.clone().with_connector_name("sender-ddata-peer"),
    )
    .unwrap();
    let receiver_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        receiver_kit.system(),
        receiver_cluster,
        receiver_runtime,
        settings.with_connector_name("receiver-ddata-peer"),
    )
    .unwrap();
    let sender_snapshots = nodes
        .create_probe_on::<ReplicatorTcpPeerConnectorSnapshot>(
            "ddata-bootstrap-adopt-sender",
            "sender-snapshots",
        )
        .unwrap();
    let receiver_snapshots = nodes
        .create_probe_on::<ReplicatorTcpPeerConnectorSnapshot>(
            "ddata-bootstrap-adopt-receiver",
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

    let sender_ref = replicator_actor_ref_for("ddata-bootstrap-adopt-sender", &sender_settings)
        .expect("sender ref should be serializable");
    let receiver_ref =
        replicator_actor_ref_for("ddata-bootstrap-adopt-receiver", &receiver_settings)
            .expect("receiver ref should be serializable");
    let to_receiver = outbound(
        ReplicaId::from(&receiver_node),
        receiver_ref.clone(),
        sender_ref.clone(),
        registry,
        sender_cache.clone(),
    );
    let received = send_read_until_received(
        &to_receiver,
        &receiver_requests,
        ReplicatorRead {
            key: "counter-before-adopted-route-shutdown".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        },
        Duration::from_secs(1),
    );
    assert_eq!(received[0].0, ReplicaId::from(&sender_node));
    assert_eq!(received[0].1.recipient, receiver_ref);
    assert_eq!(received[0].1.sender, Some(sender_ref));

    run_bootstrap_shutdown(sender_kit, sender_bootstrap.connector());
    await_cache_route_count(&sender_cache, 0);

    let removed_route_error = to_receiver
        .tell(ReplicatorRead {
            key: "counter-after-adopted-route-shutdown".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        })
        .expect_err("adopted peer route should reject sends after bootstrap shutdown");
    assert!(
        removed_route_error
            .reason()
            .contains("no remote association route"),
        "unexpected removed-route send error: {removed_route_error:?}"
    );
    assert_eq!(
        receiver_requests
            .wait_for_len(2, Duration::from_millis(100))
            .len(),
        1
    );

    run_bootstrap_shutdown(receiver_kit, receiver_bootstrap.connector());
    await_cache_route_count(&receiver_cache, 0);
    nodes.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_installed_peer_route_delivers_remote_request_to_receiver() {
    let _guard = bootstrap_socket_test_lock();
    let registry = registry();
    let nodes = MultiNodeTestKit::new([
        "ddata-bootstrap-deliver-sender",
        "ddata-bootstrap-deliver-receiver",
    ])
    .unwrap();
    let sender_kit = nodes.kit("ddata-bootstrap-deliver-sender").unwrap();
    let receiver_kit = nodes.kit("ddata-bootstrap-deliver-receiver").unwrap();
    let receiver_requests = Arc::new(RecordingRequests::default());
    let sender_runtime = bind_runtime(
        "ddata-bootstrap-deliver-sender",
        1,
        11,
        ReplicaId::new("receiver"),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let receiver_runtime = bind_runtime_with_requests(
        "ddata-bootstrap-deliver-receiver",
        2,
        22,
        ReplicaId::new("sender"),
        receiver_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
    );
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_settings = sender_runtime.runtime().settings().clone();
    let receiver_settings = receiver_runtime.runtime().settings().clone();
    let sender_publisher = spawn_publisher(sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(receiver_kit, "receiver-publisher", receiver_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let receiver_cluster = Cluster::new(receiver_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    );

    let sender_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings.clone().with_connector_name("sender-ddata-peer"),
    )
    .unwrap();
    let receiver_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        receiver_kit.system(),
        receiver_cluster,
        receiver_runtime,
        settings.with_connector_name("receiver-ddata-peer"),
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("sender-snapshots")
        .unwrap();
    let receiver_snapshots = receiver_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("receiver-snapshots")
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

    let sender_ref = replicator_actor_ref_for("ddata-bootstrap-deliver-sender", &sender_settings)
        .expect("sender ref should be serializable");
    let receiver_ref =
        replicator_actor_ref_for("ddata-bootstrap-deliver-receiver", &receiver_settings)
            .expect("receiver ref should be serializable");
    let outbound = outbound(
        ReplicaId::from(&receiver_node),
        receiver_ref.clone(),
        sender_ref.clone(),
        registry.clone(),
        sender_cache,
    );
    outbound
        .tell(ReplicatorRead {
            key: "counter".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        })
        .unwrap();

    let received = receiver_requests.wait_for_len(1, Duration::from_secs(1));
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].0, ReplicaId::from(&sender_node));
    assert_eq!(
        received[0].1.message.manifest.as_str(),
        ReplicatorRead::MANIFEST
    );
    let decoded = registry
        .deserialize::<ReplicatorRead>(received[0].1.message.clone())
        .unwrap();
    assert_eq!(decoded.from, Some(ReplicaId::from(&sender_node)));
    assert_eq!(decoded.key, "counter");
    assert_eq!(received[0].1.recipient, receiver_ref);
    assert_eq!(received[0].1.sender, Some(sender_ref));

    run_bootstrap_shutdown(sender_kit, sender_bootstrap.connector());
    run_bootstrap_shutdown(receiver_kit, receiver_bootstrap.connector());
    nodes.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_removes_peer_route_when_cluster_membership_drops_peer() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("ddata-bootstrap-remove-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("ddata-bootstrap-remove-receiver").unwrap();
    let sender_runtime = bind_runtime(
        "ddata-bootstrap-remove-sender",
        1,
        11,
        ReplicaId::new("receiver"),
    );
    let receiver_runtime = bind_runtime(
        "ddata-bootstrap-remove-receiver",
        2,
        22,
        ReplicaId::new("sender"),
    );
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(&receiver_kit, "receiver-publisher", receiver_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let receiver_cluster = Cluster::new(receiver_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    );

    let sender_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings.clone().with_connector_name("sender-ddata-peer"),
    )
    .unwrap();
    let receiver_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        receiver_kit.system(),
        receiver_cluster,
        receiver_runtime,
        settings.with_connector_name("receiver-ddata-peer"),
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("sender-snapshots")
        .unwrap();
    let receiver_snapshots = receiver_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("receiver-snapshots")
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
    let sender_kit = ActorSystemTestKit::new("ddata-bootstrap-remove-pending-sender").unwrap();
    let sender_runtime = bind_runtime(
        "ddata-bootstrap-remove-pending-sender",
        1,
        11,
        ReplicaId::new("sender"),
    );
    let sender_node = sender_runtime.self_node().clone();
    let missing_node = UniqueAddress::new(
        Address::new(
            "kairo",
            "ddata-bootstrap-remove-pending-missing",
            Some("127.0.0.1".to_string()),
            Some(unused_port()),
        ),
        2,
    );
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    )
    .with_connector_name("sender-ddata-peer");

    let sender_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings,
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("sender-snapshots")
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
        ActorSystemTestKit::with_manual_time("ddata-bootstrap-automatic-retry-sender").unwrap();
    let missing_kit = ActorSystemTestKit::new("ddata-bootstrap-automatic-retry-missing").unwrap();
    let retry_interval = Duration::from_millis(25);
    let missing_port = unused_port();
    let missing_node = node("ddata-bootstrap-automatic-retry-missing", missing_port, 2);
    let sender_runtime = bind_runtime(
        "ddata-bootstrap-automatic-retry-sender",
        1,
        11,
        ReplicaId::from(&missing_node),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(ReplicatorTcpPeerConnectorSettings::new(retry_interval).unwrap())
    .with_connector_name("sender-ddata-peer");

    let sender_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings,
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("sender-snapshots")
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
        "ddata-bootstrap-automatic-retry-missing",
        ReplicaId::from(&missing_node),
        ReplicaId::from(&sender_node),
        22,
        missing_port,
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
    let sender_kit = ActorSystemTestKit::new("ddata-bootstrap-partial-sender").unwrap();
    let bound_kit = ActorSystemTestKit::new("ddata-bootstrap-partial-bound").unwrap();
    let missing_kit = ActorSystemTestKit::new("ddata-bootstrap-partial-missing").unwrap();
    let retry_interval = Duration::from_millis(25);
    let registry = registry();
    let bound_port = unused_port();
    let missing_port = unused_port();
    let bound_node = node("ddata-bootstrap-partial-bound", bound_port, 2);
    let missing_node = node("ddata-bootstrap-partial-missing", missing_port, 3);
    let sender_runtime = bind_runtime(
        "ddata-bootstrap-partial-sender",
        1,
        11,
        ReplicaId::from(&bound_node),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let sender_settings = sender_runtime.runtime().settings().clone();
    let sender_node = sender_runtime.self_node().clone();
    let bound_requests = Arc::new(RecordingRequests::default());
    let bound_runtime = bind_association_runtime_on_port_with_requests(
        "ddata-bootstrap-partial-bound",
        ReplicaId::from(&bound_node),
        ReplicaId::from(&sender_node),
        22,
        bound_port,
        bound_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
    );
    let bound_settings = bound_runtime.settings().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(retry_interval)
            .unwrap()
            .with_automatic_retry_ticks(false),
    )
    .with_connector_name("sender-ddata-peer");

    let sender_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings,
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("sender-snapshots")
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

    let sender_ref = replicator_actor_ref_for("ddata-bootstrap-partial-sender", &sender_settings)
        .expect("sender ref should be serializable");
    let bound_ref = replicator_actor_ref_for("ddata-bootstrap-partial-bound", &bound_settings)
        .expect("bound ref should be serializable");
    let to_bound = outbound(
        ReplicaId::from(&bound_node),
        bound_ref.clone(),
        sender_ref.clone(),
        registry.clone(),
        sender_cache.clone(),
    );
    let received = send_read_until_received(
        &to_bound,
        bound_requests.as_ref(),
        ReplicatorRead {
            key: "partial-active-route".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        },
        Duration::from_secs(1),
    );
    assert_eq!(received[0].0, ReplicaId::from(&sender_node));
    let decoded = registry
        .deserialize::<ReplicatorRead>(received[0].1.message.clone())
        .unwrap();
    assert_eq!(decoded.key, "partial-active-route");
    assert_eq!(decoded.from, Some(ReplicaId::from(&sender_node)));
    assert_eq!(received[0].1.recipient, bound_ref);
    assert_eq!(received[0].1.sender, Some(sender_ref));

    let missing_runtime = bind_association_runtime_on_port(
        "ddata-bootstrap-partial-missing",
        ReplicaId::from(&missing_node),
        ReplicaId::from(&sender_node),
        33,
        missing_port,
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
fn bootstrap_coordinated_shutdown_stops_connector_with_pending_reconnect() {
    let _guard = bootstrap_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("ddata-bootstrap-shutdown-pending-sender").unwrap();
    let sender_runtime = bind_runtime(
        "ddata-bootstrap-shutdown-pending-sender",
        1,
        11,
        ReplicaId::new("sender"),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let missing_node = UniqueAddress::new(
        Address::new(
            "kairo",
            "ddata-bootstrap-shutdown-pending-missing",
            Some("127.0.0.1".to_string()),
            Some(unused_port()),
        ),
        2,
    );
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    )
    .with_connector_name("sender-ddata-peer");

    let sender_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings,
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("sender-snapshots")
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
fn bootstrap_reinstalls_peer_route_for_replacement_unique_address() {
    let _guard = bootstrap_socket_test_lock();
    let registry = registry();
    let sender_kit = ActorSystemTestKit::new("ddata-bootstrap-replace-sender").unwrap();
    let old_receiver_kit = ActorSystemTestKit::new("ddata-bootstrap-replace-old").unwrap();
    let new_receiver_kit = ActorSystemTestKit::new("ddata-bootstrap-replace-new").unwrap();
    let sender_runtime = bind_runtime(
        "ddata-bootstrap-replace-sender",
        1,
        11,
        ReplicaId::new("sender"),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let sender_settings = sender_runtime.runtime().settings().clone();
    let old_receiver_requests = Arc::new(RecordingRequests::default());
    let new_receiver_requests = Arc::new(RecordingRequests::default());
    let old_receiver_runtime = bind_runtime_with_requests(
        "ddata-bootstrap-replace-old",
        2,
        22,
        ReplicaId::from(sender_runtime.self_node()),
        old_receiver_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
    );
    let old_receiver_settings = old_receiver_runtime.runtime().settings().clone();
    let new_receiver_runtime = bind_runtime_with_requests(
        "ddata-bootstrap-replace-new",
        3,
        33,
        ReplicaId::from(sender_runtime.self_node()),
        new_receiver_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
    );
    let new_receiver_settings = new_receiver_runtime.runtime().settings().clone();
    let sender_node = sender_runtime.self_node().clone();
    let old_receiver_node = old_receiver_runtime.self_node().clone();
    let new_receiver_node = new_receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    );

    let sender_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        sender_kit.system(),
        sender_cluster,
        sender_runtime,
        settings.with_connector_name("sender-ddata-peer"),
    )
    .unwrap();
    let sender_snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("sender-snapshots")
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

    let sender_ref = replicator_actor_ref_for("ddata-bootstrap-replace-sender", &sender_settings)
        .expect("sender ref should be serializable");
    let old_receiver_ref =
        replicator_actor_ref_for("ddata-bootstrap-replace-old", &old_receiver_settings)
            .expect("old receiver ref should be serializable");
    let new_receiver_ref =
        replicator_actor_ref_for("ddata-bootstrap-replace-new", &new_receiver_settings)
            .expect("new receiver ref should be serializable");
    let to_old_receiver = outbound(
        ReplicaId::from(&old_receiver_node),
        old_receiver_ref,
        sender_ref.clone(),
        registry.clone(),
        sender_cache.clone(),
    );
    let old_received = send_read_until_received(
        &to_old_receiver,
        &old_receiver_requests,
        ReplicatorRead {
            key: "counter-before-replacement".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        },
        Duration::from_secs(1),
    );
    assert_eq!(old_received[0].0, ReplicaId::from(&sender_node));

    publish_gossip(&sender_publisher, up_gossip([sender_node.clone()]));
    await_connector_no_routes(sender_bootstrap.connector(), &sender_snapshots);

    let old_error = to_old_receiver
        .tell(ReplicatorRead {
            key: "counter-after-old-removed".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        })
        .expect_err("old receiver route should reject sends after removal");
    assert!(
        old_error.reason().contains("no remote association route"),
        "unexpected old receiver send error: {old_error:?}"
    );
    assert_eq!(
        old_receiver_requests
            .wait_for_len(2, Duration::from_millis(100))
            .len(),
        1
    );

    publish_gossip(
        &sender_publisher,
        up_gossip([sender_node.clone(), new_receiver_node.clone()]),
    );
    await_connector_route(
        sender_bootstrap.connector(),
        &sender_snapshots,
        &new_receiver_node,
    );

    let to_new_receiver = outbound(
        ReplicaId::from(&new_receiver_node),
        new_receiver_ref.clone(),
        sender_ref.clone(),
        registry.clone(),
        sender_cache.clone(),
    );
    let new_received = send_read_until_received(
        &to_new_receiver,
        &new_receiver_requests,
        ReplicatorRead {
            key: "counter-after-replacement".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        },
        Duration::from_secs(1),
    );
    assert_eq!(new_received.len(), 1);
    assert_eq!(new_received[0].0, ReplicaId::from(&sender_node));
    assert_eq!(
        new_received[0].1.message.manifest.as_str(),
        ReplicatorRead::MANIFEST
    );
    let new_read = registry
        .deserialize::<ReplicatorRead>(new_received[0].1.message.clone())
        .unwrap();
    assert_eq!(new_read.from, Some(ReplicaId::from(&sender_node)));
    assert_eq!(new_read.key, "counter-after-replacement");
    assert_eq!(new_received[0].1.recipient, new_receiver_ref);
    assert_eq!(new_received[0].1.sender, Some(sender_ref));

    run_bootstrap_shutdown(&sender_kit, sender_bootstrap.connector());
    old_receiver_runtime.shutdown().unwrap();
    new_receiver_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    old_receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    new_receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_sender_keeps_remaining_route_delivering_after_peer_removed() {
    let _guard = bootstrap_socket_test_lock();
    let registry = registry();
    let first_kit = ActorSystemTestKit::new("ddata-bootstrap-reduce-first").unwrap();
    let second_kit = ActorSystemTestKit::new("ddata-bootstrap-reduce-second").unwrap();
    let third_kit = ActorSystemTestKit::new("ddata-bootstrap-reduce-third").unwrap();
    let first_runtime = bind_runtime(
        "ddata-bootstrap-reduce-first",
        1,
        11,
        ReplicaId::new("first"),
    );
    let first_cache = first_runtime.association_cache().clone();
    let first_node = first_runtime.self_node().clone();
    let first_settings = first_runtime.runtime().settings().clone();
    let second_requests = Arc::new(RecordingRequests::default());
    let third_requests = Arc::new(RecordingRequests::default());
    let second_runtime = bind_runtime_with_requests(
        "ddata-bootstrap-reduce-second",
        2,
        22,
        ReplicaId::from(&first_node),
        second_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
    );
    let second_node = second_runtime.self_node().clone();
    let second_settings = second_runtime.runtime().settings().clone();
    let third_runtime = bind_runtime_with_requests(
        "ddata-bootstrap-reduce-third",
        3,
        33,
        ReplicaId::from(&first_node),
        third_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
    );
    let third_node = third_runtime.self_node().clone();
    let third_settings = third_runtime.runtime().settings().clone();
    let first_publisher = spawn_publisher(&first_kit, "first-publisher", first_node.clone());
    let first_cluster = Cluster::new(first_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    );

    let first_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        first_kit.system(),
        first_cluster,
        first_runtime,
        settings.with_connector_name("first-ddata-peer"),
    )
    .unwrap();
    let first_snapshots = first_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("first-snapshots")
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

    let first_ref = replicator_actor_ref_for("ddata-bootstrap-reduce-first", &first_settings)
        .expect("first ref should be serializable");
    let second_ref = replicator_actor_ref_for("ddata-bootstrap-reduce-second", &second_settings)
        .expect("second ref should be serializable");
    let third_ref = replicator_actor_ref_for("ddata-bootstrap-reduce-third", &third_settings)
        .expect("third ref should be serializable");
    let to_second = outbound(
        ReplicaId::from(&second_node),
        second_ref.clone(),
        first_ref.clone(),
        registry.clone(),
        first_cache.clone(),
    );
    let to_third = outbound(
        ReplicaId::from(&third_node),
        third_ref,
        first_ref.clone(),
        registry.clone(),
        first_cache.clone(),
    );

    let second_received = send_read_until_received(
        &to_second,
        &second_requests,
        ReplicatorRead {
            key: "counter-second-before-removal".to_string(),
            from: Some(ReplicaId::from(&first_node)),
        },
        Duration::from_secs(1),
    );
    let third_received = send_read_until_received(
        &to_third,
        &third_requests,
        ReplicatorRead {
            key: "counter-third-before-removal".to_string(),
            from: Some(ReplicaId::from(&first_node)),
        },
        Duration::from_secs(1),
    );
    assert_eq!(second_received[0].0, ReplicaId::from(&first_node));
    assert_eq!(third_received[0].0, ReplicaId::from(&first_node));

    publish_gossip(
        &first_publisher,
        up_gossip([first_node.clone(), second_node.clone()]),
    );
    await_connector_route(first_bootstrap.connector(), &first_snapshots, &second_node);
    await_cache_route_count(&first_cache, 1);

    let removed_peer_error = to_third
        .tell(ReplicatorRead {
            key: "counter-third-after-removal".to_string(),
            from: Some(ReplicaId::from(&first_node)),
        })
        .expect_err("removed peer route should reject sends");
    assert!(
        removed_peer_error
            .reason()
            .contains("no remote association route"),
        "unexpected removed-peer send error: {removed_peer_error:?}"
    );
    assert_eq!(
        third_requests
            .wait_for_len(2, Duration::from_millis(100))
            .len(),
        1
    );

    let second_received_after_removal = send_read_until_count_received(
        &to_second,
        &second_requests,
        ReplicatorRead {
            key: "counter-second-after-removal".to_string(),
            from: Some(ReplicaId::from(&first_node)),
        },
        2,
        Duration::from_secs(1),
    );
    assert_eq!(second_received_after_removal.len(), 2);
    assert_eq!(
        second_received_after_removal[1].0,
        ReplicaId::from(&first_node)
    );
    assert_eq!(
        second_received_after_removal[1].1.message.manifest.as_str(),
        ReplicatorRead::MANIFEST
    );
    let second_read_after_removal = registry
        .deserialize::<ReplicatorRead>(second_received_after_removal[1].1.message.clone())
        .unwrap();
    assert_eq!(
        second_read_after_removal.from,
        Some(ReplicaId::from(&first_node))
    );
    assert_eq!(
        second_read_after_removal.key,
        "counter-second-after-removal"
    );
    assert_eq!(second_received_after_removal[1].1.recipient, second_ref);
    assert_eq!(second_received_after_removal[1].1.sender, Some(first_ref));

    run_bootstrap_shutdown(&first_kit, first_bootstrap.connector());
    await_cache_route_count(&first_cache, 0);
    second_runtime.shutdown().unwrap();
    third_runtime.shutdown().unwrap();
    first_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership() {
    let _guard = bootstrap_socket_test_lock();
    let registry = registry();
    let first_kit = ActorSystemTestKit::new("ddata-bootstrap-first").unwrap();
    let second_kit = ActorSystemTestKit::new("ddata-bootstrap-second").unwrap();
    let third_kit = ActorSystemTestKit::new("ddata-bootstrap-third").unwrap();
    let first_requests = Arc::new(RecordingRequests::default());
    let first_runtime = bind_runtime_with_requests(
        "ddata-bootstrap-first",
        1,
        11,
        ReplicaId::new("first"),
        first_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
    );
    let first_cache = first_runtime.association_cache().clone();
    let first_node = first_runtime.self_node().clone();
    let first_settings = first_runtime.runtime().settings().clone();
    let second_requests = Arc::new(RecordingRequests::default());
    let third_requests = Arc::new(RecordingRequests::default());
    let second_runtime = bind_runtime_with_requests(
        "ddata-bootstrap-second",
        2,
        22,
        ReplicaId::from(&first_node),
        second_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
    );
    let second_cache = second_runtime.association_cache().clone();
    let second_node = second_runtime.self_node().clone();
    let second_settings = second_runtime.runtime().settings().clone();
    let third_runtime = bind_runtime_with_requests(
        "ddata-bootstrap-third",
        3,
        33,
        ReplicaId::from(&first_node),
        third_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
    );
    let third_cache = third_runtime.association_cache().clone();
    let third_node = third_runtime.self_node().clone();
    let third_settings = third_runtime.runtime().settings().clone();
    let first_publisher = spawn_publisher(&first_kit, "first-publisher", first_node.clone());
    let second_publisher = spawn_publisher(&second_kit, "second-publisher", second_node.clone());
    let third_publisher = spawn_publisher(&third_kit, "third-publisher", third_node.clone());
    let first_cluster = Cluster::new(first_publisher.clone());
    let second_cluster = Cluster::new(second_publisher.clone());
    let third_cluster = Cluster::new(third_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
    )
    .with_connector_settings(
        ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap()
            .with_automatic_retry_ticks(false),
    );

    let first_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        first_kit.system(),
        first_cluster,
        first_runtime,
        settings.clone().with_connector_name("first-ddata-peer"),
    )
    .unwrap();
    let second_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        second_kit.system(),
        second_cluster,
        second_runtime,
        settings.clone().with_connector_name("second-ddata-peer"),
    )
    .unwrap();
    let third_bootstrap = ReplicatorTcpPeerBootstrap::spawn_with_runtime(
        third_kit.system(),
        third_cluster,
        third_runtime,
        settings.with_connector_name("third-ddata-peer"),
    )
    .unwrap();
    let first_snapshots = first_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("first-snapshots")
        .unwrap();
    let second_snapshots = second_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("second-snapshots")
        .unwrap();
    let third_snapshots = third_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("third-snapshots")
        .unwrap();

    let gossip = Gossip::from_members([
        Member::new(first_node.clone(), Vec::new()).with_status(MemberStatus::Up),
        Member::new(second_node.clone(), Vec::new()).with_status(MemberStatus::Up),
        Member::new(third_node.clone(), Vec::new()).with_status(MemberStatus::Up),
    ]);
    first_publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(gossip.clone()))
        .unwrap();

    await_connector_routes(
        first_bootstrap.connector(),
        &first_snapshots,
        &[second_node.clone(), third_node.clone()],
    );
    assert_eq!(first_cache.route_count(), 2);

    let first_ref = replicator_actor_ref_for("ddata-bootstrap-first", &first_settings)
        .expect("first ref should be serializable");
    let second_ref = replicator_actor_ref_for("ddata-bootstrap-second", &second_settings)
        .expect("second ref should be serializable");
    let third_ref = replicator_actor_ref_for("ddata-bootstrap-third", &third_settings)
        .expect("third ref should be serializable");
    let to_second = outbound(
        ReplicaId::from(&second_node),
        second_ref.clone(),
        first_ref.clone(),
        registry.clone(),
        first_cache.clone(),
    );
    let to_third = outbound(
        ReplicaId::from(&third_node),
        third_ref.clone(),
        first_ref.clone(),
        registry.clone(),
        first_cache.clone(),
    );

    let second_received = send_read_until_received(
        &to_second,
        &second_requests,
        ReplicatorRead {
            key: "counter-second".to_string(),
            from: Some(ReplicaId::from(&first_node)),
        },
        Duration::from_secs(1),
    );
    let third_received = send_read_until_received(
        &to_third,
        &third_requests,
        ReplicatorRead {
            key: "counter-third".to_string(),
            from: Some(ReplicaId::from(&first_node)),
        },
        Duration::from_secs(1),
    );

    assert_eq!(second_received.len(), 1);
    assert_eq!(second_received[0].0, ReplicaId::from(&first_node));
    assert_eq!(
        second_received[0].1.message.manifest.as_str(),
        ReplicatorRead::MANIFEST
    );
    let second_read = registry
        .deserialize::<ReplicatorRead>(second_received[0].1.message.clone())
        .unwrap();
    assert_eq!(second_read.from, Some(ReplicaId::from(&first_node)));
    assert_eq!(second_read.key, "counter-second");
    assert_eq!(second_received[0].1.recipient, second_ref);
    assert_eq!(second_received[0].1.sender, Some(first_ref.clone()));

    assert_eq!(third_received.len(), 1);
    assert_eq!(third_received[0].0, ReplicaId::from(&first_node));
    assert_eq!(
        third_received[0].1.message.manifest.as_str(),
        ReplicatorRead::MANIFEST
    );
    let third_read = registry
        .deserialize::<ReplicatorRead>(third_received[0].1.message.clone())
        .unwrap();
    assert_eq!(third_read.from, Some(ReplicaId::from(&first_node)));
    assert_eq!(third_read.key, "counter-third");
    assert_eq!(third_received[0].1.recipient, third_ref);
    assert_eq!(third_received[0].1.sender, Some(first_ref.clone()));

    for publisher in [&second_publisher, &third_publisher] {
        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(gossip.clone()))
            .unwrap();
    }
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

    let second_to_third = outbound(
        ReplicaId::from(&third_node),
        third_ref.clone(),
        second_ref.clone(),
        registry.clone(),
        second_cache.clone(),
    );
    let (third_from_second, third_envelope_from_second, third_read_from_second) =
        send_read_until_key_received(
            &second_to_third,
            &third_requests,
            &registry,
            ReplicatorRead {
                key: "counter-third-from-second".to_string(),
                from: Some(ReplicaId::from(&second_node)),
            },
            "counter-third-from-second",
            Duration::from_secs(1),
        );
    assert_eq!(third_from_second, ReplicaId::from(&second_node));
    assert_eq!(
        third_envelope_from_second.message.manifest.as_str(),
        ReplicatorRead::MANIFEST
    );
    assert_eq!(
        third_read_from_second.from,
        Some(ReplicaId::from(&second_node))
    );
    assert_eq!(third_read_from_second.key, "counter-third-from-second");
    assert_eq!(third_envelope_from_second.recipient, third_ref.clone());
    assert_eq!(third_envelope_from_second.sender, Some(second_ref.clone()));

    let third_to_second = outbound(
        ReplicaId::from(&second_node),
        second_ref.clone(),
        third_ref.clone(),
        registry.clone(),
        third_cache.clone(),
    );
    let (second_from_third, second_envelope_from_third, second_read_from_third) =
        send_read_until_key_received(
            &third_to_second,
            &second_requests,
            &registry,
            ReplicatorRead {
                key: "counter-second-from-third".to_string(),
                from: Some(ReplicaId::from(&third_node)),
            },
            "counter-second-from-third",
            Duration::from_secs(1),
        );
    assert_eq!(second_from_third, ReplicaId::from(&third_node));
    assert_eq!(
        second_envelope_from_third.message.manifest.as_str(),
        ReplicatorRead::MANIFEST
    );
    assert_eq!(
        second_read_from_third.from,
        Some(ReplicaId::from(&third_node))
    );
    assert_eq!(second_read_from_third.key, "counter-second-from-third");
    assert_eq!(second_envelope_from_third.recipient, second_ref.clone());
    assert_eq!(second_envelope_from_third.sender, Some(third_ref.clone()));

    let second_to_first = outbound(
        ReplicaId::from(&first_node),
        first_ref.clone(),
        second_ref.clone(),
        registry.clone(),
        second_cache.clone(),
    );
    let (first_from_second, first_envelope_from_second, first_read_from_second) =
        send_read_until_key_received(
            &second_to_first,
            &first_requests,
            &registry,
            ReplicatorRead {
                key: "counter-first-from-second".to_string(),
                from: Some(ReplicaId::from(&second_node)),
            },
            "counter-first-from-second",
            Duration::from_secs(1),
        );
    assert_eq!(first_from_second, ReplicaId::from(&second_node));
    assert_eq!(
        first_envelope_from_second.message.manifest.as_str(),
        ReplicatorRead::MANIFEST
    );
    assert_eq!(
        first_read_from_second.from,
        Some(ReplicaId::from(&second_node))
    );
    assert_eq!(first_read_from_second.key, "counter-first-from-second");
    assert_eq!(first_envelope_from_second.recipient, first_ref.clone());
    assert_eq!(first_envelope_from_second.sender, Some(second_ref.clone()));

    let third_to_first = outbound(
        ReplicaId::from(&first_node),
        first_ref.clone(),
        third_ref.clone(),
        registry.clone(),
        third_cache.clone(),
    );
    let (first_from_third, first_envelope_from_third, first_read_from_third) =
        send_read_until_key_received(
            &third_to_first,
            &first_requests,
            &registry,
            ReplicatorRead {
                key: "counter-first-from-third".to_string(),
                from: Some(ReplicaId::from(&third_node)),
            },
            "counter-first-from-third",
            Duration::from_secs(1),
        );
    assert_eq!(first_from_third, ReplicaId::from(&third_node));
    assert_eq!(
        first_envelope_from_third.message.manifest.as_str(),
        ReplicatorRead::MANIFEST
    );
    assert_eq!(
        first_read_from_third.from,
        Some(ReplicaId::from(&third_node))
    );
    assert_eq!(first_read_from_third.key, "counter-first-from-third");
    assert_eq!(first_envelope_from_third.recipient, first_ref.clone());
    assert_eq!(first_envelope_from_third.sender, Some(third_ref.clone()));

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
    assert_eq!(third_cache.route_count(), 2);

    let removed_second_to_third_error = second_to_third
        .tell(ReplicatorRead {
            key: "counter-third-after-reduction".to_string(),
            from: Some(ReplicaId::from(&second_node)),
        })
        .expect_err("second-to-third route should reject sends after third is removed");
    assert!(
        removed_second_to_third_error
            .reason()
            .contains("no remote association route"),
        "unexpected second-to-third send error: {removed_second_to_third_error:?}"
    );

    let (second_after_reduction_from_first, _, second_after_reduction_read) =
        send_read_until_key_received(
            &to_second,
            &second_requests,
            &registry,
            ReplicatorRead {
                key: "counter-second-after-reduction".to_string(),
                from: Some(ReplicaId::from(&first_node)),
            },
            "counter-second-after-reduction",
            Duration::from_secs(1),
        );
    assert_eq!(
        second_after_reduction_from_first,
        ReplicaId::from(&first_node)
    );
    assert_eq!(
        second_after_reduction_read.from,
        Some(ReplicaId::from(&first_node))
    );

    let (first_after_reduction_from_second, _, first_after_reduction_read) =
        send_read_until_key_received(
            &second_to_first,
            &first_requests,
            &registry,
            ReplicatorRead {
                key: "counter-first-after-reduction".to_string(),
                from: Some(ReplicaId::from(&second_node)),
            },
            "counter-first-after-reduction",
            Duration::from_secs(1),
        );
    assert_eq!(
        first_after_reduction_from_second,
        ReplicaId::from(&second_node)
    );
    assert_eq!(
        first_after_reduction_read.from,
        Some(ReplicaId::from(&second_node))
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
