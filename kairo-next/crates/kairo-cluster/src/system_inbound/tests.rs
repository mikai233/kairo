use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::Address;
use kairo_serialization::{ActorRefWireData, Manifest, Registry, RemoteMessage, SerializedMessage};
use kairo_testkit::ActorSystemTestKit;

use super::*;
use crate::{
    ClusterGossipProcessMsg, ClusterGossipWireInbound, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
    DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH, DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH, GossipStatus,
    Heartbeat, HeartbeatRsp, InitJoin, Join, UniqueAddress, VectorClock,
    register_cluster_protocol_codecs,
};

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_cluster_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

fn node(system: &str, port: u16, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
        uid,
    )
}

fn wire_for(node: &UniqueAddress, path: &str) -> ActorRefWireData {
    ActorRefWireData::new(format!("{}{}", node.address, path)).unwrap()
}

#[test]
fn cluster_system_manifest_helper_matches_seed_membership_and_heartbeat_protocols() {
    assert!(is_cluster_system_manifest(InitJoin::MANIFEST));
    assert!(is_cluster_system_manifest(Join::MANIFEST));
    assert!(is_cluster_system_manifest(GossipStatus::MANIFEST));
    assert!(is_cluster_system_manifest(Heartbeat::MANIFEST));
    assert!(is_cluster_system_manifest(HeartbeatRsp::MANIFEST));
    assert!(!is_cluster_system_manifest(
        "kairo.cluster-tools.pubsub.status"
    ));
}

#[test]
fn system_inbound_routes_gossip_status_to_process() {
    let kit = ActorSystemTestKit::new("system-inbound-gossip").unwrap();
    let self_node = node("receiver", 2552, 2);
    let sender = node("sender", 2551, 1);
    let registry = registry();
    let process = kit
        .create_probe::<ClusterGossipProcessMsg>("gossip-process")
        .unwrap();
    let status = GossipStatus {
        from: sender,
        version: VectorClock::new().increment("sender"),
        seen_digest: Bytes::from_static(b"seen"),
    };
    let envelope = kairo_serialization::RemoteEnvelope::new(
        wire_for(&self_node, DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH),
        None,
        registry.serialize(&status).unwrap(),
    );

    ClusterSystemInbound::new(self_node)
        .with_gossip(ClusterGossipWireInbound::new(registry, process.actor_ref()))
        .receive(envelope)
        .unwrap();

    assert!(matches!(
        process.expect_msg(Duration::from_secs(1)).unwrap(),
        ClusterGossipProcessMsg::Status(actual) if actual == status
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn system_inbound_reports_missing_seed_join_handler_after_recipient_validation() {
    let self_node = node("receiver", 2552, 2);
    let sender = node("sender", 2551, 1);
    let registry = registry();
    let envelope = kairo_serialization::RemoteEnvelope::new(
        wire_for(&self_node, DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH),
        Some(wire_for(&sender, DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH)),
        registry
            .serialize(&InitJoin {
                joining_config_digest: Bytes::from_static(b"digest"),
            })
            .unwrap(),
    );
    let error = ClusterSystemInbound::new(self_node)
        .receive(envelope)
        .unwrap_err();

    assert!(matches!(
        error,
        ClusterSystemInboundError::MissingHandler("seed join")
    ));
}

#[test]
fn system_inbound_reports_missing_membership_handler_after_recipient_validation() {
    let self_node = node("receiver", 2552, 2);
    let sender = node("sender", 2551, 1);
    let registry = registry();
    let envelope = kairo_serialization::RemoteEnvelope::new(
        wire_for(&self_node, DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH),
        None,
        registry
            .serialize(&Join {
                node: sender,
                roles: vec!["backend".to_string()],
            })
            .unwrap(),
    );
    let error = ClusterSystemInbound::new(self_node)
        .receive(envelope)
        .unwrap_err();

    assert!(matches!(
        error,
        ClusterSystemInboundError::MissingHandler("membership")
    ));
}

#[test]
fn system_inbound_reports_missing_heartbeat_handlers_after_recipient_validation() {
    let self_node = node("receiver", 2552, 2);
    let sender = node("sender", 2551, 1);
    let registry = registry();

    let heartbeat = kairo_serialization::RemoteEnvelope::new(
        wire_for(&self_node, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH),
        Some(wire_for(&sender, DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH)),
        registry
            .serialize(&Heartbeat {
                from: sender.clone(),
                sequence_nr: 1,
                creation_time_nanos: 2,
            })
            .unwrap(),
    );
    let error = ClusterSystemInbound::new(self_node.clone())
        .receive(heartbeat)
        .unwrap_err();
    assert!(matches!(
        error,
        ClusterSystemInboundError::MissingHandler("heartbeat receiver")
    ));

    let heartbeat_response = kairo_serialization::RemoteEnvelope::new(
        wire_for(&self_node, DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH),
        Some(wire_for(&sender, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH)),
        registry
            .serialize(&HeartbeatRsp {
                from: sender,
                sequence_nr: 1,
                creation_time_nanos: 2,
            })
            .unwrap(),
    );
    let error = ClusterSystemInbound::new(self_node)
        .receive(heartbeat_response)
        .unwrap_err();
    assert!(matches!(
        error,
        ClusterSystemInboundError::MissingHandler("heartbeat response")
    ));
}

#[test]
fn system_inbound_rejects_wrong_cluster_recipient() {
    let self_node = node("receiver", 2552, 2);
    let wrong_node = node("other", 2553, 3);
    let sender = node("sender", 2551, 1);
    let registry = registry();
    let envelope = kairo_serialization::RemoteEnvelope::new(
        wire_for(&wrong_node, DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH),
        None,
        registry
            .serialize(&Join {
                node: sender,
                roles: vec![],
            })
            .unwrap(),
    );
    let error = ClusterSystemInbound::new(self_node)
        .receive(envelope)
        .unwrap_err();

    assert!(matches!(
        error,
        ClusterSystemInboundError::WrongRecipient { .. }
    ));
}

#[test]
fn system_inbound_rejects_unknown_manifest() {
    let self_node = node("receiver", 2552, 2);
    let envelope = kairo_serialization::RemoteEnvelope::new(
        wire_for(&self_node, DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH),
        None,
        SerializedMessage::new(
            9_999,
            Manifest::new("kairo.cluster.unknown-system"),
            1,
            Bytes::new(),
        ),
    );
    let error = ClusterSystemInbound::new(self_node)
        .receive(envelope)
        .unwrap_err();

    assert!(matches!(
        error,
        ClusterSystemInboundError::UnsupportedManifest(_)
    ));
}
