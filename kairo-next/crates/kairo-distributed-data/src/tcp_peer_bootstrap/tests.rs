use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorRef, Address, PHASE_BEFORE_CLUSTER_SHUTDOWN, Props};
use kairo_cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus,
    UniqueAddress,
};
use kairo_remote::RemoteSettings;
use kairo_serialization::RemoteEnvelope;
use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

use super::{
    ReplicatorTcpPeerBootstrap, ReplicatorTcpPeerBootstrapIdentity,
    ReplicatorTcpPeerBootstrapSettings,
};
use crate::{
    ReplicaId, ReplicatorRemoteReplyError, ReplicatorRemoteReplyReceiver,
    ReplicatorRemoteRequestError, ReplicatorRemoteRequestReceiver, ReplicatorTcpPeerConnectorMsg,
    ReplicatorTcpPeerConnectorSettings, ReplicatorTcpPeerConnectorSnapshot,
    ReplicatorTcpPeerRuntime,
};

#[derive(Default)]
struct IgnoreRequests;

impl ReplicatorRemoteRequestReceiver for IgnoreRequests {
    fn receive_request_from(
        &self,
        _from: ReplicaId,
        _envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteRequestError> {
        Ok(())
    }
}

#[derive(Default)]
struct IgnoreReplies;

impl ReplicatorRemoteReplyReceiver for IgnoreReplies {
    fn receive_reply_from(
        &self,
        _from: ReplicaId,
        _envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        Ok(())
    }
}

#[test]
fn bootstrap_binds_connector_and_registers_coordinated_shutdown_stop() {
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
    let settings = ReplicatorTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
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
    assert!(!bootstrap.connector().is_stopped());

    kit.system()
        .coordinated_shutdown()
        .run_from("test", Some(PHASE_BEFORE_CLUSTER_SHUTDOWN))
        .unwrap();

    assert!(bootstrap.connector().wait_for_stop(Duration::from_secs(1)));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_two_nodes_install_peer_routes_from_cluster_membership() {
    let sender_kit = ActorSystemTestKit::new("ddata-bootstrap-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("ddata-bootstrap-receiver").unwrap();
    let sender_runtime = bind_runtime("ddata-bootstrap-sender", 1, 11, ReplicaId::new("receiver"));
    let receiver_runtime =
        bind_runtime("ddata-bootstrap-receiver", 2, 22, ReplicaId::new("sender"));
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = receiver_runtime.self_node().clone();
    let sender_publisher = spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
    let receiver_publisher =
        spawn_publisher(&receiver_kit, "receiver-publisher", receiver_node.clone());
    let sender_cluster = Cluster::new(sender_publisher.clone());
    let receiver_cluster = Cluster::new(receiver_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
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

    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership() {
    let first_kit = ActorSystemTestKit::new("ddata-bootstrap-first").unwrap();
    let second_kit = ActorSystemTestKit::new("ddata-bootstrap-second").unwrap();
    let third_kit = ActorSystemTestKit::new("ddata-bootstrap-third").unwrap();
    let first_runtime = bind_runtime("ddata-bootstrap-first", 1, 11, ReplicaId::new("first"));
    let second_runtime = bind_runtime("ddata-bootstrap-second", 2, 22, ReplicaId::new("second"));
    let third_runtime = bind_runtime("ddata-bootstrap-third", 3, 33, ReplicaId::new("third"));
    let first_node = first_runtime.self_node().clone();
    let second_node = second_runtime.self_node().clone();
    let third_node = third_runtime.self_node().clone();
    let first_publisher = spawn_publisher(&first_kit, "first-publisher", first_node.clone());
    let second_publisher = spawn_publisher(&second_kit, "second-publisher", second_node.clone());
    let third_publisher = spawn_publisher(&third_kit, "third-publisher", third_node.clone());
    let first_cluster = Cluster::new(first_publisher.clone());
    let second_cluster = Cluster::new(second_publisher.clone());
    let third_cluster = Cluster::new(third_publisher.clone());
    let settings = ReplicatorTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
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

    first_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}

fn bind_runtime(
    system: &str,
    node_uid: u64,
    system_uid: u64,
    remote_replica: ReplicaId,
) -> ReplicatorTcpPeerRuntime {
    ReplicatorTcpPeerRuntime::bind(
        system,
        node_uid,
        system_uid,
        remote_replica,
        RemoteSettings::new("127.0.0.1", 0),
        Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
        Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
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
    connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ReplicatorTcpPeerConnectorSnapshot>,
    expected_peer: &UniqueAddress,
) {
    await_connector_routes(connector, snapshots, std::slice::from_ref(expected_peer));
}

fn await_connector_routes(
    connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ReplicatorTcpPeerConnectorSnapshot>,
    expected_peers: &[UniqueAddress],
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ReplicatorTcpPeerConnectorMsg::Snapshot {
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
                Err(format!("unexpected connector snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap();
}
