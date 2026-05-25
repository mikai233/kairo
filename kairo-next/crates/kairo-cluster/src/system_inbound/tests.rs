use std::sync::Arc;

use kairo_actor::Address;
use kairo_serialization::{ActorRefWireData, Registry, RemoteMessage};

use super::*;
use crate::{
    DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH, Heartbeat, HeartbeatRsp, Join, UniqueAddress,
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
fn cluster_system_manifest_helper_matches_membership_and_heartbeat_protocols() {
    assert!(is_cluster_system_manifest(Join::MANIFEST));
    assert!(is_cluster_system_manifest(Heartbeat::MANIFEST));
    assert!(is_cluster_system_manifest(HeartbeatRsp::MANIFEST));
    assert!(!is_cluster_system_manifest(
        "kairo.cluster-tools.pubsub.status"
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
