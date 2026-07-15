use bytes::Bytes;
use kairo_actor::Address;
use kairo_serialization::{Manifest, Registry, RemoteMessage, SerializedMessage};

use super::*;
use crate::{
    ClusterConfigCheck, Down, ExitingConfirmed, Gossip, GossipEnvelope, GossipStatus, Heartbeat,
    HeartbeatRsp, InitJoin, InitJoinAck, InitJoinNack, Join, Leave, Member, MemberStatus,
    Reachability, UniqueAddress, VectorClock, VectorClockNode, Welcome,
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
fn cluster_daemon_codecs_round_trip_seed_contact_and_member_actions() {
    let registry = registry();
    let address = unique(9).address;
    let init = InitJoin {
        joining_config_digest: Bytes::from_static(b"cluster-config-v1"),
    };
    let ack = InitJoinAck {
        address: address.clone(),
        config_check: ClusterConfigCheck::Compatible,
    };
    let nack = InitJoinNack {
        address: address.clone(),
    };
    let leave = Leave {
        address: address.clone(),
    };
    let down = Down {
        address: address.clone(),
    };
    let exiting = ExitingConfirmed { node: unique(9) };

    let init_wire = registry.serialize(&init).unwrap();
    let ack_wire = registry.serialize(&ack).unwrap();
    let nack_wire = registry.serialize(&nack).unwrap();
    let leave_wire = registry.serialize(&leave).unwrap();
    let down_wire = registry.serialize(&down).unwrap();
    let exiting_wire = registry.serialize(&exiting).unwrap();

    assert_eq!(init_wire.serializer_id, INIT_JOIN_SERIALIZER_ID);
    assert_eq!(ack_wire.serializer_id, INIT_JOIN_ACK_SERIALIZER_ID);
    assert_eq!(nack_wire.serializer_id, INIT_JOIN_NACK_SERIALIZER_ID);
    assert_eq!(leave_wire.serializer_id, LEAVE_SERIALIZER_ID);
    assert_eq!(down_wire.serializer_id, DOWN_SERIALIZER_ID);
    assert_eq!(exiting_wire.serializer_id, EXITING_CONFIRMED_SERIALIZER_ID);
    assert_eq!(registry.deserialize::<InitJoin>(init_wire).unwrap(), init);
    assert_eq!(registry.deserialize::<InitJoinAck>(ack_wire).unwrap(), ack);
    assert_eq!(
        registry.deserialize::<InitJoinNack>(nack_wire).unwrap(),
        nack
    );
    assert_eq!(registry.deserialize::<Leave>(leave_wire).unwrap(), leave);
    assert_eq!(registry.deserialize::<Down>(down_wire).unwrap(), down);
    assert_eq!(
        registry
            .deserialize::<ExitingConfirmed>(exiting_wire)
            .unwrap(),
        exiting
    );
}

#[test]
fn cluster_daemon_codec_round_trips_gossip_status() {
    let registry = registry();
    let status = GossipStatus {
        from: unique(4),
        version: VectorClock::new()
            .increment(VectorClockNode::new("node-a"))
            .increment(VectorClockNode::new("node-b")),
        seen_digest: Bytes::from_static(&[0xaa, 0xbb, 0xcc]),
    };

    let wire = registry.serialize(&status).unwrap();

    assert_eq!(wire.serializer_id, GOSSIP_STATUS_SERIALIZER_ID);
    assert_eq!(wire.manifest.as_str(), GossipStatus::MANIFEST);
    assert_eq!(registry.deserialize::<GossipStatus>(wire).unwrap(), status);
}

#[test]
fn cluster_daemon_codecs_reject_unknown_versions_and_trailing_bytes() {
    let registry = registry();
    let init = registry
        .serialize(&InitJoin {
            joining_config_digest: Bytes::from_static(b"digest"),
        })
        .unwrap();
    let wrong_version = SerializedMessage::new(
        INIT_JOIN_SERIALIZER_ID,
        Manifest::new(InitJoin::MANIFEST),
        InitJoin::VERSION + 1,
        init.payload,
    );
    assert!(
        registry
            .deserialize::<InitJoin>(wrong_version)
            .unwrap_err()
            .to_string()
            .contains("unsupported")
    );

    let status = registry
        .serialize(&GossipStatus {
            from: unique(4),
            version: VectorClock::new(),
            seen_digest: Bytes::new(),
        })
        .unwrap();
    let mut payload = status.payload.to_vec();
    payload.push(0xff);
    let trailing = SerializedMessage::new(
        GOSSIP_STATUS_SERIALIZER_ID,
        Manifest::new(GossipStatus::MANIFEST),
        GossipStatus::VERSION,
        Bytes::from(payload),
    );
    assert!(
        registry
            .deserialize::<GossipStatus>(trailing)
            .unwrap_err()
            .to_string()
            .contains("trailing byte")
    );

    let ack = registry
        .serialize(&InitJoinAck {
            address: unique(5).address,
            config_check: ClusterConfigCheck::Compatible,
        })
        .unwrap();
    let mut invalid_check_payload = ack.payload.to_vec();
    *invalid_check_payload
        .last_mut()
        .expect("ack payload includes config-check code") = 0xff;
    let invalid_check = SerializedMessage::new(
        INIT_JOIN_ACK_SERIALIZER_ID,
        Manifest::new(InitJoinAck::MANIFEST),
        InitJoinAck::VERSION,
        Bytes::from(invalid_check_payload),
    );
    assert!(
        registry
            .deserialize::<InitJoinAck>(invalid_check)
            .unwrap_err()
            .to_string()
            .contains("config-check code")
    );
}

#[test]
fn cluster_control_codecs_reject_trailing_payload_bytes() {
    let registry = registry();
    let join = registry
        .serialize(&Join {
            node: unique(1),
            roles: vec!["backend".to_string()],
        })
        .unwrap();
    let mut payload = join.payload.to_vec();
    payload.push(0xff);
    let wire = SerializedMessage::new(
        JOIN_SERIALIZER_ID,
        Manifest::new(Join::MANIFEST),
        Join::VERSION,
        Bytes::from(payload),
    );

    let error = registry
        .deserialize::<Join>(wire)
        .expect_err("trailing join payload byte should fail");

    assert!(error.to_string().contains("trailing byte"));
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

#[test]
fn cluster_gossip_codecs_reject_trailing_payload_bytes() {
    let registry = registry();
    let envelope = registry
        .serialize(&GossipEnvelope {
            from: unique(1),
            to: unique(2),
            sequence_nr: 77,
            gossip: rich_gossip(),
        })
        .unwrap();
    let mut payload = envelope.payload.to_vec();
    payload.push(0xff);
    let wire = SerializedMessage::new(
        GOSSIP_ENVELOPE_SERIALIZER_ID,
        Manifest::new(GossipEnvelope::MANIFEST),
        GossipEnvelope::VERSION,
        Bytes::from(payload),
    );

    let error = registry
        .deserialize::<GossipEnvelope>(wire)
        .expect_err("trailing gossip payload byte should fail");

    assert!(error.to_string().contains("trailing byte"));
}
