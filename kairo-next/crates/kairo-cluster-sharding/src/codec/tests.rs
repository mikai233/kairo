use bytes::Bytes;
use kairo_serialization::{ActorRefWireData, Manifest, Registry, RemoteMessage, SerializedMessage};

use super::*;
use crate::{
    BeginHandOff, BeginHandOffAck, GetShardHome, GracefulShutdownReq, HandOff, HostShard,
    RegionStopped, Register, RegisterAck, RoutedShardEnvelope, ShardHome, ShardStarted,
    ShardStopped,
};

fn registry() -> Registry {
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    registry
}

#[test]
fn sharding_protocol_codecs_round_trip_registration_messages() {
    let registry = registry();
    let register = Register {
        region: ActorRefWireData::new("kairo://sys@127.0.0.1:25520/user/region#1").unwrap(),
    };
    let ack = RegisterAck {
        coordinator: ActorRefWireData::new("kairo://sys@127.0.0.1:25520/system/sharding#2")
            .unwrap(),
    };

    let serialized_register = registry.serialize(&register).unwrap();
    let serialized_ack = registry.serialize(&ack).unwrap();

    assert_eq!(serialized_register.serializer_id, REGISTER_SERIALIZER_ID);
    assert_eq!(serialized_ack.serializer_id, REGISTER_ACK_SERIALIZER_ID);
    assert_eq!(
        registry
            .deserialize::<Register>(serialized_register)
            .unwrap(),
        register
    );
    assert_eq!(
        registry.deserialize::<RegisterAck>(serialized_ack).unwrap(),
        ack
    );
}

#[test]
fn sharding_protocol_codecs_round_trip_shard_home_messages() {
    let registry = registry();
    let get = GetShardHome {
        shard_id: "12".to_string(),
    };
    let home = ShardHome {
        shard_id: "12".to_string(),
        region: ActorRefWireData::new("kairo://sys@127.0.0.1:25521/user/region#3").unwrap(),
    };

    let serialized_get = registry.serialize(&get).unwrap();
    let serialized_home = registry.serialize(&home).unwrap();

    assert_eq!(serialized_get.serializer_id, GET_SHARD_HOME_SERIALIZER_ID);
    assert_eq!(serialized_home.serializer_id, SHARD_HOME_SERIALIZER_ID);
    assert_eq!(
        registry
            .deserialize::<GetShardHome>(serialized_get)
            .unwrap(),
        get
    );
    assert_eq!(
        registry.deserialize::<ShardHome>(serialized_home).unwrap(),
        home
    );
}

#[test]
fn sharding_protocol_codecs_round_trip_handoff_messages() {
    let registry = registry();
    let host = HostShard {
        shard_id: "42".to_string(),
    };
    let started = ShardStarted {
        shard_id: "42".to_string(),
    };
    let begin = BeginHandOff {
        shard_id: "42".to_string(),
    };
    let begin_ack = BeginHandOffAck {
        shard_id: "42".to_string(),
    };
    let handoff = HandOff {
        shard_id: "42".to_string(),
    };
    let stopped = ShardStopped {
        shard_id: "42".to_string(),
    };
    let shutdown = GracefulShutdownReq {
        region: ActorRefWireData::new("kairo://sys@127.0.0.1:25521/user/region#4").unwrap(),
    };
    let region_stopped = RegionStopped {
        region: ActorRefWireData::new("kairo://sys@127.0.0.1:25521/user/region#4").unwrap(),
    };
    let routed = RoutedShardEnvelope {
        shard_id: "42".to_string(),
        entity_id: "entity-1".to_string(),
        message: SerializedMessage::new(
            777,
            kairo_serialization::Manifest::new("kairo.test.message"),
            3,
            Bytes::from_static(b"payload"),
        ),
    };

    assert_eq!(
        registry
            .deserialize::<HostShard>(registry.serialize(&host).unwrap())
            .unwrap(),
        host
    );
    assert_eq!(
        registry
            .deserialize::<ShardStarted>(registry.serialize(&started).unwrap())
            .unwrap(),
        started
    );
    assert_eq!(
        registry
            .deserialize::<BeginHandOff>(registry.serialize(&begin).unwrap())
            .unwrap(),
        begin
    );
    assert_eq!(
        registry
            .deserialize::<BeginHandOffAck>(registry.serialize(&begin_ack).unwrap())
            .unwrap(),
        begin_ack
    );
    assert_eq!(
        registry
            .deserialize::<HandOff>(registry.serialize(&handoff).unwrap())
            .unwrap(),
        handoff
    );
    assert_eq!(
        registry
            .deserialize::<ShardStopped>(registry.serialize(&stopped).unwrap())
            .unwrap(),
        stopped
    );
    assert_eq!(
        registry
            .deserialize::<GracefulShutdownReq>(registry.serialize(&shutdown).unwrap())
            .unwrap(),
        shutdown
    );
    assert_eq!(
        registry
            .deserialize::<RegionStopped>(registry.serialize(&region_stopped).unwrap())
            .unwrap(),
        region_stopped
    );
    let serialized_routed = registry.serialize(&routed).unwrap();
    assert_eq!(
        serialized_routed.serializer_id,
        ROUTED_SHARD_ENVELOPE_SERIALIZER_ID
    );
    assert_eq!(
        registry
            .deserialize::<RoutedShardEnvelope>(serialized_routed)
            .unwrap(),
        routed
    );
}

#[test]
fn sharding_protocol_codecs_reject_unknown_versions() {
    let registry = registry();
    let wire = SerializedMessage::new(
        GET_SHARD_HOME_SERIALIZER_ID,
        Manifest::new(GetShardHome::MANIFEST),
        GetShardHome::VERSION + 1,
        Bytes::from_static(&[0, 0, 0, 2, b'4', b'2']),
    );

    let error = registry
        .deserialize::<GetShardHome>(wire)
        .expect_err("unknown version should fail");

    assert!(error.to_string().contains("unsupported"));
}
