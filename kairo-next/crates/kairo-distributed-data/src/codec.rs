mod client;
mod delta;
mod direct;
mod gossip;
mod helpers;

use kairo_serialization::{Registry, SerializationRegistry};

use crate::{
    ReplicatorChanged, ReplicatorDeltaAck, ReplicatorDeltaNack, ReplicatorDeltaPropagation,
    ReplicatorGet, ReplicatorGossip, ReplicatorGossipStatus, ReplicatorRead, ReplicatorReadResult,
    ReplicatorSubscribe, ReplicatorUpdate, ReplicatorWrite, ReplicatorWriteAck,
    ReplicatorWriteNack,
};

pub use client::{
    ReplicatorChangedCodec, ReplicatorGetCodec, ReplicatorSubscribeCodec, ReplicatorUpdateCodec,
};
pub use delta::{
    ReplicatorDeltaAckCodec, ReplicatorDeltaNackCodec, ReplicatorDeltaPropagationCodec,
};
pub use direct::{
    ReplicatorReadCodec, ReplicatorReadResultCodec, ReplicatorWriteAckCodec, ReplicatorWriteCodec,
    ReplicatorWriteNackCodec,
};
pub use gossip::{ReplicatorGossipCodec, ReplicatorGossipStatusCodec};

pub const REPLICATOR_GET_SERIALIZER_ID: u32 = 3_000;
pub const REPLICATOR_UPDATE_SERIALIZER_ID: u32 = 3_001;
pub const REPLICATOR_SUBSCRIBE_SERIALIZER_ID: u32 = 3_002;
pub const REPLICATOR_CHANGED_SERIALIZER_ID: u32 = 3_003;
pub const REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID: u32 = 3_004;
pub const REPLICATOR_DELTA_ACK_SERIALIZER_ID: u32 = 3_005;
pub const REPLICATOR_DELTA_NACK_SERIALIZER_ID: u32 = 3_006;
pub const REPLICATOR_WRITE_SERIALIZER_ID: u32 = 3_007;
pub const REPLICATOR_WRITE_ACK_SERIALIZER_ID: u32 = 3_008;
pub const REPLICATOR_WRITE_NACK_SERIALIZER_ID: u32 = 3_009;
pub const REPLICATOR_READ_SERIALIZER_ID: u32 = 3_010;
pub const REPLICATOR_READ_RESULT_SERIALIZER_ID: u32 = 3_011;
pub const REPLICATOR_GOSSIP_STATUS_SERIALIZER_ID: u32 = 3_012;
pub const REPLICATOR_GOSSIP_SERIALIZER_ID: u32 = 3_013;

const DATA_ENVELOPE_PRUNING_WIRE_VERSION: u16 = 2;

pub fn register_ddata_protocol_codecs(registry: &mut Registry) -> kairo_serialization::Result<()> {
    registry.register::<ReplicatorGet, _>(ReplicatorGetCodec)?;
    registry.register::<ReplicatorUpdate, _>(ReplicatorUpdateCodec)?;
    registry.register::<ReplicatorSubscribe, _>(ReplicatorSubscribeCodec)?;
    registry.register::<ReplicatorChanged, _>(ReplicatorChangedCodec)?;
    registry.register::<ReplicatorDeltaPropagation, _>(ReplicatorDeltaPropagationCodec)?;
    registry.register::<ReplicatorDeltaAck, _>(ReplicatorDeltaAckCodec)?;
    registry.register::<ReplicatorDeltaNack, _>(ReplicatorDeltaNackCodec)?;
    registry.register::<ReplicatorWrite, _>(ReplicatorWriteCodec)?;
    registry.register::<ReplicatorWriteAck, _>(ReplicatorWriteAckCodec)?;
    registry.register::<ReplicatorWriteNack, _>(ReplicatorWriteNackCodec)?;
    registry.register::<ReplicatorRead, _>(ReplicatorReadCodec)?;
    registry.register::<ReplicatorReadResult, _>(ReplicatorReadResultCodec)?;
    registry.register::<ReplicatorGossipStatus, _>(ReplicatorGossipStatusCodec)?;
    registry.register::<ReplicatorGossip, _>(ReplicatorGossipCodec)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kairo_serialization::{ActorRefWireData, Manifest, RemoteMessage, SerializedMessage};

    use super::*;
    use crate::{
        ReplicaId, ReplicatorDataEnvelope, ReplicatorDelta, ReplicatorGossipDigest,
        ReplicatorGossipEntry, ReplicatorPruningEntry, ReplicatorPruningState,
    };

    fn registry() -> Registry {
        let mut registry = Registry::new();
        register_ddata_protocol_codecs(&mut registry).unwrap();
        registry
    }

    fn with_trailing_byte(message: SerializedMessage) -> SerializedMessage {
        let mut payload = message.payload.to_vec();
        payload.push(0xff);
        SerializedMessage::new(
            message.serializer_id,
            message.manifest,
            message.version,
            Bytes::from(payload),
        )
    }

    #[test]
    fn ddata_protocol_codecs_round_trip_get_and_update() {
        let registry = registry();
        let get = ReplicatorGet {
            key: "counter-a".to_string(),
            request_id: 17,
        };
        let update = ReplicatorUpdate {
            key: "counter-a".to_string(),
            request_id: 18,
        };

        let serialized_get = registry.serialize(&get).unwrap();
        let serialized_update = registry.serialize(&update).unwrap();

        assert_eq!(serialized_get.serializer_id, REPLICATOR_GET_SERIALIZER_ID);
        assert_eq!(serialized_get.manifest.as_str(), ReplicatorGet::MANIFEST);
        assert_eq!(
            serialized_update.serializer_id,
            REPLICATOR_UPDATE_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorGet>(serialized_get)
                .unwrap(),
            get
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorUpdate>(serialized_update)
                .unwrap(),
            update
        );
    }

    #[test]
    fn ddata_protocol_codecs_round_trip_subscribe_and_changed() {
        let registry = registry();
        let subscribe = ReplicatorSubscribe {
            key: "state/*".to_string(),
            subscriber: ActorRefWireData::new("kairo://sys@127.0.0.1:25520/user/sub#1").unwrap(),
        };
        let changed = ReplicatorChanged {
            key: "state/a".to_string(),
        };

        let serialized_subscribe = registry.serialize(&subscribe).unwrap();
        let serialized_changed = registry.serialize(&changed).unwrap();

        assert_eq!(
            serialized_subscribe.serializer_id,
            REPLICATOR_SUBSCRIBE_SERIALIZER_ID
        );
        assert_eq!(
            serialized_changed.serializer_id,
            REPLICATOR_CHANGED_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorSubscribe>(serialized_subscribe)
                .unwrap(),
            subscribe
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorChanged>(serialized_changed)
                .unwrap(),
            changed
        );
    }

    #[test]
    fn ddata_protocol_codecs_round_trip_delta_propagation() {
        let registry = registry();
        let propagation = ReplicatorDeltaPropagation {
            from: ReplicaId::new("kairo://sys@127.0.0.1:25520#7"),
            reply: true,
            deltas: vec![
                ReplicatorDelta {
                    key: "counter-a".to_string(),
                    crdt_manifest: crate::GCOUNTER_MANIFEST.to_string(),
                    crdt_version: crate::CRDT_CODEC_VERSION,
                    from_version: 3,
                    to_version: 5,
                    payload: Bytes::from_static(&[0, 1, 2, 3]),
                },
                ReplicatorDelta {
                    key: "set-b".to_string(),
                    crdt_manifest: crate::GSET_STRING_MANIFEST.to_string(),
                    crdt_version: crate::CRDT_CODEC_VERSION,
                    from_version: 6,
                    to_version: 6,
                    payload: Bytes::from_static(&[4, 5, 6]),
                },
            ],
        };

        let serialized = registry.serialize(&propagation).unwrap();

        assert_eq!(
            serialized.serializer_id,
            REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID
        );
        assert_eq!(
            serialized.manifest.as_str(),
            ReplicatorDeltaPropagation::MANIFEST
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorDeltaPropagation>(serialized)
                .unwrap(),
            propagation
        );
    }

    #[test]
    fn ddata_protocol_codecs_round_trip_delta_ack_and_nack() {
        let registry = registry();

        let ack = registry.serialize(&ReplicatorDeltaAck).unwrap();
        let nack = registry.serialize(&ReplicatorDeltaNack).unwrap();

        assert_eq!(ack.serializer_id, REPLICATOR_DELTA_ACK_SERIALIZER_ID);
        assert_eq!(nack.serializer_id, REPLICATOR_DELTA_NACK_SERIALIZER_ID);
        assert_eq!(
            registry.deserialize::<ReplicatorDeltaAck>(ack).unwrap(),
            ReplicatorDeltaAck
        );
        assert_eq!(
            registry.deserialize::<ReplicatorDeltaNack>(nack).unwrap(),
            ReplicatorDeltaNack
        );
    }

    #[test]
    fn ddata_protocol_codecs_round_trip_write_and_read_messages() {
        let registry = registry();
        let envelope = ReplicatorDataEnvelope {
            crdt_manifest: crate::GCOUNTER_MANIFEST.to_string(),
            crdt_version: crate::CRDT_CODEC_VERSION,
            payload: Bytes::from_static(&[9, 8, 7]),
            pruning: vec![
                ReplicatorPruningEntry {
                    removed: ReplicaId::new("removed-a"),
                    state: ReplicatorPruningState::Initialized {
                        owner: ReplicaId::new("node-a"),
                        seen: vec![ReplicaId::new("node-b")],
                    },
                },
                ReplicatorPruningEntry {
                    removed: ReplicaId::new("removed-b"),
                    state: ReplicatorPruningState::Performed {
                        obsolete_at_millis: 1234,
                    },
                },
            ],
        };
        let write = ReplicatorWrite {
            key: "counter-a".to_string(),
            from: Some(ReplicaId::new("node-a")),
            envelope: envelope.clone(),
        };
        let read = ReplicatorRead {
            key: "counter-a".to_string(),
            from: Some(ReplicaId::new("node-b")),
        };
        let read_result = ReplicatorReadResult {
            envelope: Some(envelope),
        };
        let not_found = ReplicatorReadResult { envelope: None };

        let serialized_write = registry.serialize(&write).unwrap();
        let serialized_read = registry.serialize(&read).unwrap();
        let serialized_read_result = registry.serialize(&read_result).unwrap();
        let serialized_not_found = registry.serialize(&not_found).unwrap();

        assert_eq!(
            serialized_write.serializer_id,
            REPLICATOR_WRITE_SERIALIZER_ID
        );
        assert_eq!(serialized_read.serializer_id, REPLICATOR_READ_SERIALIZER_ID);
        assert_eq!(
            serialized_read_result.serializer_id,
            REPLICATOR_READ_RESULT_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorWrite>(serialized_write)
                .unwrap(),
            write
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorRead>(serialized_read)
                .unwrap(),
            read
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorReadResult>(serialized_read_result)
                .unwrap(),
            read_result
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorReadResult>(serialized_not_found)
                .unwrap(),
            not_found
        );
    }

    #[test]
    fn ddata_protocol_codecs_round_trip_gossip_status_and_full_state() {
        let registry = registry();
        let envelope = ReplicatorDataEnvelope {
            crdt_manifest: crate::GCOUNTER_MANIFEST.to_string(),
            crdt_version: crate::CRDT_CODEC_VERSION,
            payload: Bytes::from_static(&[1, 2, 3]),
            pruning: vec![ReplicatorPruningEntry {
                removed: ReplicaId::new("removed-a"),
                state: ReplicatorPruningState::Initialized {
                    owner: ReplicaId::new("node-a"),
                    seen: vec![ReplicaId::new("node-b")],
                },
            }],
        };
        let status = ReplicatorGossipStatus {
            entries: vec![ReplicatorGossipDigest {
                key: "counter-a".to_string(),
                digest: 42,
                used_timestamp_millis: 123,
            }],
            chunk: 1,
            total_chunks: 3,
            to_system_uid: Some(11),
            from_system_uid: Some(22),
        };
        let gossip = ReplicatorGossip {
            entries: vec![ReplicatorGossipEntry {
                key: "counter-a".to_string(),
                envelope,
                used_timestamp_millis: 456,
            }],
            send_back: true,
            to_system_uid: Some(22),
            from_system_uid: Some(11),
        };

        let serialized_status = registry.serialize(&status).unwrap();
        let serialized_gossip = registry.serialize(&gossip).unwrap();

        assert_eq!(
            serialized_status.serializer_id,
            REPLICATOR_GOSSIP_STATUS_SERIALIZER_ID
        );
        assert_eq!(
            serialized_gossip.serializer_id,
            REPLICATOR_GOSSIP_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorGossipStatus>(serialized_status)
                .unwrap(),
            status
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorGossip>(serialized_gossip)
                .unwrap(),
            gossip
        );
    }

    #[test]
    fn ddata_protocol_codecs_round_trip_write_ack_and_nack() {
        let registry = registry();

        let ack = registry.serialize(&ReplicatorWriteAck).unwrap();
        let nack = registry.serialize(&ReplicatorWriteNack).unwrap();

        assert_eq!(ack.serializer_id, REPLICATOR_WRITE_ACK_SERIALIZER_ID);
        assert_eq!(nack.serializer_id, REPLICATOR_WRITE_NACK_SERIALIZER_ID);
        assert_eq!(
            registry.deserialize::<ReplicatorWriteAck>(ack).unwrap(),
            ReplicatorWriteAck
        );
        assert_eq!(
            registry.deserialize::<ReplicatorWriteNack>(nack).unwrap(),
            ReplicatorWriteNack
        );
    }

    #[test]
    fn ddata_protocol_codecs_reject_unknown_versions() {
        let registry = registry();
        let wire = SerializedMessage::new(
            REPLICATOR_GET_SERIALIZER_ID,
            Manifest::new(ReplicatorGet::MANIFEST),
            ReplicatorGet::VERSION + 1,
            Bytes::from_static(&[0, 0, 0, 1, b'a', 0, 0, 0, 0, 0, 0, 0, 1]),
        );

        let error = registry
            .deserialize::<ReplicatorGet>(wire)
            .expect_err("unknown version should fail");

        assert!(error.to_string().contains("unsupported"));
    }

    #[test]
    fn ddata_client_protocol_codecs_reject_trailing_payload_bytes() {
        let registry = registry();
        let serialized = registry
            .serialize(&ReplicatorGet {
                key: "counter-a".to_string(),
                request_id: 17,
            })
            .unwrap();

        let error = registry
            .deserialize::<ReplicatorGet>(with_trailing_byte(serialized))
            .expect_err("trailing client payload byte should fail");

        assert!(error.to_string().contains("trailing byte"));
    }

    #[test]
    fn ddata_direct_protocol_codecs_reject_trailing_payload_bytes() {
        let registry = registry();
        let serialized = registry
            .serialize(&ReplicatorRead {
                key: "counter-a".to_string(),
                from: Some(ReplicaId::new("node-a")),
            })
            .unwrap();

        let error = registry
            .deserialize::<ReplicatorRead>(with_trailing_byte(serialized))
            .expect_err("trailing direct payload byte should fail");

        assert!(error.to_string().contains("trailing byte"));
    }

    #[test]
    fn ddata_delta_protocol_rejects_unknown_versions() {
        let registry = registry();
        let wire = SerializedMessage::new(
            REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID,
            Manifest::new(ReplicatorDeltaPropagation::MANIFEST),
            ReplicatorDeltaPropagation::VERSION + 1,
            Bytes::new(),
        );

        let error = registry
            .deserialize::<ReplicatorDeltaPropagation>(wire)
            .expect_err("unknown version should fail");

        assert!(error.to_string().contains("unsupported"));
    }

    #[test]
    fn ddata_delta_protocol_codecs_reject_trailing_payload_bytes() {
        let registry = registry();
        let serialized = registry
            .serialize(&ReplicatorDeltaPropagation {
                from: ReplicaId::new("kairo://sys@127.0.0.1:25520#7"),
                reply: false,
                deltas: vec![],
            })
            .unwrap();

        let error = registry
            .deserialize::<ReplicatorDeltaPropagation>(with_trailing_byte(serialized))
            .expect_err("trailing delta payload byte should fail");

        assert!(error.to_string().contains("trailing byte"));
    }

    #[test]
    fn ddata_gossip_protocol_codecs_reject_trailing_payload_bytes() {
        let registry = registry();
        let serialized = registry
            .serialize(&ReplicatorGossipStatus {
                entries: vec![ReplicatorGossipDigest {
                    key: "counter-a".to_string(),
                    digest: 42,
                    used_timestamp_millis: 123,
                }],
                chunk: 1,
                total_chunks: 1,
                to_system_uid: Some(11),
                from_system_uid: Some(22),
            })
            .unwrap();

        let error = registry
            .deserialize::<ReplicatorGossipStatus>(with_trailing_byte(serialized))
            .expect_err("trailing gossip payload byte should fail");

        assert!(error.to_string().contains("trailing byte"));
    }
}
