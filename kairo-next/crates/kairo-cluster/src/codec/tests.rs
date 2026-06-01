use kairo_actor::Address;
use kairo_serialization::{Manifest, Registry, RemoteMessage, SerializedMessage};

use super::*;
use crate::{
    Gossip, GossipEnvelope, Heartbeat, HeartbeatRsp, Join, Member, MemberStatus, Reachability,
    UniqueAddress, VectorClock, VectorClockNode, Welcome,
};

fn registry() -> Registry {
    let mut registry = Registry::new();
    register_cluster_protocol_codecs(&mut registry).unwrap();
    registry
}

fn unique(uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", "sys", Some("127.0.0.1".to_string()), Some(25520)),
        uid,
    )
}

fn rich_gossip() -> Gossip {
    let node_a = unique(1);
    let node_b = unique(2);
    let node_c = unique(3);
    let members = vec![
        Member::new(node_a.clone(), vec!["backend".to_string()])
            .with_status(MemberStatus::Up)
            .with_up_number(1),
        Member::new(node_b.clone(), vec!["frontend".to_string()])
            .with_status(MemberStatus::Leaving)
            .with_up_number(2),
    ];
    let reachability = Reachability::new()
        .unreachable(node_a.clone(), node_b.clone())
        .terminated(node_b.clone(), node_c.clone());
    let version = VectorClock::new()
        .increment(VectorClockNode::new("node-a"))
        .increment(VectorClockNode::new("node-b"))
        .increment(VectorClockNode::new("node-b"));

    Gossip::from_parts(
        members,
        vec![node_a.clone(), node_b.clone()],
        reachability,
        version,
        vec![(node_c, 99)],
    )
}

#[test]
fn cluster_control_codecs_round_trip_heartbeat_messages() {
    let registry = registry();
    let heartbeat = Heartbeat {
        from: unique(7),
        sequence_nr: 42,
        creation_time_nanos: 1234,
    };
    let response = HeartbeatRsp {
        from: unique(8),
        sequence_nr: 42,
        creation_time_nanos: 1234,
    };

    let serialized_heartbeat = registry.serialize(&heartbeat).unwrap();
    let serialized_response = registry.serialize(&response).unwrap();

    assert_eq!(serialized_heartbeat.serializer_id, HEARTBEAT_SERIALIZER_ID);
    assert_eq!(
        serialized_response.serializer_id,
        HEARTBEAT_RSP_SERIALIZER_ID
    );
    assert_eq!(
        registry
            .deserialize::<Heartbeat>(serialized_heartbeat)
            .unwrap(),
        heartbeat
    );
    assert_eq!(
        registry
            .deserialize::<HeartbeatRsp>(serialized_response)
            .unwrap(),
        response
    );
}

#[test]
fn cluster_control_codecs_round_trip_join() {
    let registry = registry();
    let join = Join {
        node: unique(9),
        roles: vec!["backend".to_string(), "blue".to_string()],
    };

    let serialized = registry.serialize(&join).unwrap();

    assert_eq!(serialized.serializer_id, JOIN_SERIALIZER_ID);
    assert_eq!(serialized.manifest.as_str(), Join::MANIFEST);
    assert_eq!(registry.deserialize::<Join>(serialized).unwrap(), join);
}

#[test]
fn cluster_control_codecs_reject_unknown_versions() {
    let registry = registry();
    let wire = SerializedMessage::new(
        JOIN_SERIALIZER_ID,
        Manifest::new(Join::MANIFEST),
        Join::VERSION + 1,
        registry
            .serialize(&Join {
                node: unique(1),
                roles: vec![],
            })
            .unwrap()
            .payload,
    );

    let error = registry
        .deserialize::<Join>(wire)
        .expect_err("unknown version should fail");

    assert!(error.to_string().contains("unsupported"));
}

#[test]
fn cluster_protocol_codecs_round_trip_welcome_with_gossip() {
    let registry = registry();
    let welcome = Welcome {
        from: unique(1),
        gossip: rich_gossip(),
    };

    let serialized = registry.serialize(&welcome).unwrap();

    assert_eq!(serialized.serializer_id, WELCOME_SERIALIZER_ID);
    assert_eq!(serialized.manifest.as_str(), Welcome::MANIFEST);
    assert_eq!(
        registry.deserialize::<Welcome>(serialized).unwrap(),
        welcome
    );
}

#[test]
fn cluster_protocol_codecs_round_trip_gossip_envelope() {
    let registry = registry();
    let envelope = GossipEnvelope {
        from: unique(1),
        to: unique(2),
        sequence_nr: 77,
        gossip: rich_gossip(),
    };

    let serialized = registry.serialize(&envelope).unwrap();

    assert_eq!(serialized.serializer_id, GOSSIP_ENVELOPE_SERIALIZER_ID);
    assert_eq!(serialized.manifest.as_str(), GossipEnvelope::MANIFEST);
    assert_eq!(
        registry.deserialize::<GossipEnvelope>(serialized).unwrap(),
        envelope
    );
}
