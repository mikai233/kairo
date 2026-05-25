use bytes::Bytes;
use kairo_serialization::{
    ActorRefWireData, MessageCodec, Registry, RemoteMessage, SerializationError,
    SerializationRegistry, WireReader, WireWriter,
};

use crate::{
    ReplicaId, ReplicatorChanged, ReplicatorDataEnvelope, ReplicatorDelta, ReplicatorDeltaAck,
    ReplicatorDeltaNack, ReplicatorDeltaPropagation, ReplicatorGet, ReplicatorPruningEntry,
    ReplicatorPruningState, ReplicatorRead, ReplicatorReadResult, ReplicatorSubscribe,
    ReplicatorUpdate, ReplicatorWrite, ReplicatorWriteAck, ReplicatorWriteNack,
};

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
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorGetCodec;

impl MessageCodec<ReplicatorGet> for ReplicatorGetCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_GET_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorGet) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.key)?;
        writer.write_u64(message.request_id);
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<ReplicatorGet> {
        ensure_version::<ReplicatorGet>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(ReplicatorGet {
            key: reader.read_string()?,
            request_id: reader.read_u64()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorUpdateCodec;

impl MessageCodec<ReplicatorUpdate> for ReplicatorUpdateCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_UPDATE_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorUpdate) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.key)?;
        writer.write_u64(message.request_id);
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorUpdate> {
        ensure_version::<ReplicatorUpdate>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(ReplicatorUpdate {
            key: reader.read_string()?,
            request_id: reader.read_u64()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorSubscribeCodec;

impl MessageCodec<ReplicatorSubscribe> for ReplicatorSubscribeCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_SUBSCRIBE_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorSubscribe) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.key)?;
        writer.write_string(message.subscriber.path())?;
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorSubscribe> {
        ensure_version::<ReplicatorSubscribe>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(ReplicatorSubscribe {
            key: reader.read_string()?,
            subscriber: ActorRefWireData::new(reader.read_string()?)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorChangedCodec;

impl MessageCodec<ReplicatorChanged> for ReplicatorChangedCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_CHANGED_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorChanged) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.key)?;
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorChanged> {
        ensure_version::<ReplicatorChanged>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(ReplicatorChanged {
            key: reader.read_string()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorDeltaPropagationCodec;

impl MessageCodec<ReplicatorDeltaPropagation> for ReplicatorDeltaPropagationCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorDeltaPropagation) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(message.from.as_str())?;
        writer.write_bool(message.reply);
        writer.write_u64(len_to_u64(message.deltas.len())?);
        for delta in &message.deltas {
            write_delta(&mut writer, delta)?;
        }
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorDeltaPropagation> {
        ensure_version::<ReplicatorDeltaPropagation>(version)?;
        let mut reader = WireReader::new(&payload);
        let from = ReplicaId::new(reader.read_string()?);
        let reply = reader.read_bool()?;
        let len = u64_to_len(reader.read_u64()?)?;
        let mut deltas = Vec::with_capacity(len);
        for _ in 0..len {
            deltas.push(read_delta(&mut reader)?);
        }
        Ok(ReplicatorDeltaPropagation {
            from,
            reply,
            deltas,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorDeltaAckCodec;

impl MessageCodec<ReplicatorDeltaAck> for ReplicatorDeltaAckCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_DELTA_ACK_SERIALIZER_ID
    }

    fn encode(&self, _message: &ReplicatorDeltaAck) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::new())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorDeltaAck> {
        ensure_version::<ReplicatorDeltaAck>(version)?;
        ensure_empty_payload(&payload, ReplicatorDeltaAck::MANIFEST)?;
        Ok(ReplicatorDeltaAck)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorDeltaNackCodec;

impl MessageCodec<ReplicatorDeltaNack> for ReplicatorDeltaNackCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_DELTA_NACK_SERIALIZER_ID
    }

    fn encode(&self, _message: &ReplicatorDeltaNack) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::new())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorDeltaNack> {
        ensure_version::<ReplicatorDeltaNack>(version)?;
        ensure_empty_payload(&payload, ReplicatorDeltaNack::MANIFEST)?;
        Ok(ReplicatorDeltaNack)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorWriteCodec;

impl MessageCodec<ReplicatorWrite> for ReplicatorWriteCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_WRITE_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorWrite) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.key)?;
        writer.write_optional_string(message.from.as_ref().map(ReplicaId::as_str))?;
        write_data_envelope(&mut writer, &message.envelope)?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<ReplicatorWrite> {
        ensure_version_range(
            ReplicatorWrite::MANIFEST,
            version,
            1,
            ReplicatorWrite::VERSION,
        )?;
        let mut reader = WireReader::new(&payload);
        Ok(ReplicatorWrite {
            key: reader.read_string()?,
            from: reader.read_optional_string()?.map(ReplicaId::new),
            envelope: read_data_envelope(&mut reader, version)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorWriteAckCodec;

impl MessageCodec<ReplicatorWriteAck> for ReplicatorWriteAckCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_WRITE_ACK_SERIALIZER_ID
    }

    fn encode(&self, _message: &ReplicatorWriteAck) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::new())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorWriteAck> {
        ensure_version::<ReplicatorWriteAck>(version)?;
        ensure_empty_payload(&payload, ReplicatorWriteAck::MANIFEST)?;
        Ok(ReplicatorWriteAck)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorWriteNackCodec;

impl MessageCodec<ReplicatorWriteNack> for ReplicatorWriteNackCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_WRITE_NACK_SERIALIZER_ID
    }

    fn encode(&self, _message: &ReplicatorWriteNack) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::new())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorWriteNack> {
        ensure_version::<ReplicatorWriteNack>(version)?;
        ensure_empty_payload(&payload, ReplicatorWriteNack::MANIFEST)?;
        Ok(ReplicatorWriteNack)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorReadCodec;

impl MessageCodec<ReplicatorRead> for ReplicatorReadCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_READ_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorRead) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.key)?;
        writer.write_optional_string(message.from.as_ref().map(ReplicaId::as_str))?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<ReplicatorRead> {
        ensure_version::<ReplicatorRead>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(ReplicatorRead {
            key: reader.read_string()?,
            from: reader.read_optional_string()?.map(ReplicaId::new),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorReadResultCodec;

impl MessageCodec<ReplicatorReadResult> for ReplicatorReadResultCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_READ_RESULT_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorReadResult) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        match &message.envelope {
            Some(envelope) => {
                writer.write_bool(true);
                write_data_envelope(&mut writer, envelope)?;
            }
            None => writer.write_bool(false),
        }
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorReadResult> {
        ensure_version_range(
            ReplicatorReadResult::MANIFEST,
            version,
            1,
            ReplicatorReadResult::VERSION,
        )?;
        let mut reader = WireReader::new(&payload);
        let envelope = if reader.read_bool()? {
            Some(read_data_envelope(&mut reader, version)?)
        } else {
            None
        };
        Ok(ReplicatorReadResult { envelope })
    }
}

fn ensure_version<M>(version: u16) -> kairo_serialization::Result<()>
where
    M: kairo_serialization::RemoteMessage,
{
    ensure_version_range(M::MANIFEST, version, M::VERSION, M::VERSION)
}

fn ensure_version_range(
    manifest: &str,
    version: u16,
    min_version: u16,
    max_version: u16,
) -> kairo_serialization::Result<()> {
    if (min_version..=max_version).contains(&version) {
        Ok(())
    } else {
        Err(SerializationError::Message(format!(
            "unsupported {manifest} version {version}"
        )))
    }
}

fn write_delta(
    writer: &mut WireWriter,
    delta: &ReplicatorDelta,
) -> kairo_serialization::Result<()> {
    writer.write_string(&delta.key)?;
    writer.write_string(&delta.crdt_manifest)?;
    writer.write_u16(delta.crdt_version);
    writer.write_u64(delta.from_version);
    writer.write_u64(delta.to_version);
    writer.write_bytes(&delta.payload)
}

fn read_delta(reader: &mut WireReader<'_>) -> kairo_serialization::Result<ReplicatorDelta> {
    Ok(ReplicatorDelta {
        key: reader.read_string()?,
        crdt_manifest: reader.read_string()?,
        crdt_version: reader.read_u16()?,
        from_version: reader.read_u64()?,
        to_version: reader.read_u64()?,
        payload: reader.read_bytes()?,
    })
}

fn write_data_envelope(
    writer: &mut WireWriter,
    envelope: &ReplicatorDataEnvelope,
) -> kairo_serialization::Result<()> {
    writer.write_string(&envelope.crdt_manifest)?;
    writer.write_u16(envelope.crdt_version);
    writer.write_bytes(&envelope.payload)?;
    writer.write_u64(len_to_u64(envelope.pruning.len())?);
    for entry in &envelope.pruning {
        write_pruning_entry(writer, entry)?;
    }
    Ok(())
}

fn read_data_envelope(
    reader: &mut WireReader<'_>,
    version: u16,
) -> kairo_serialization::Result<ReplicatorDataEnvelope> {
    let crdt_manifest = reader.read_string()?;
    let crdt_version = reader.read_u16()?;
    let payload = reader.read_bytes()?;
    let pruning = if version >= 2 {
        let count = u64_to_len(reader.read_u64()?)?;
        (0..count)
            .map(|_| read_pruning_entry(reader))
            .collect::<kairo_serialization::Result<Vec<_>>>()?
    } else {
        Vec::new()
    };

    Ok(ReplicatorDataEnvelope {
        crdt_manifest,
        crdt_version,
        payload,
        pruning,
    })
}

fn write_pruning_entry(
    writer: &mut WireWriter,
    entry: &ReplicatorPruningEntry,
) -> kairo_serialization::Result<()> {
    writer.write_string(entry.removed.as_str())?;
    match &entry.state {
        ReplicatorPruningState::Initialized { owner, seen } => {
            writer.write_u8(1);
            writer.write_string(owner.as_str())?;
            writer.write_u64(len_to_u64(seen.len())?);
            for seen_by in seen {
                writer.write_string(seen_by.as_str())?;
            }
        }
        ReplicatorPruningState::Performed { obsolete_at_millis } => {
            writer.write_u8(2);
            writer.write_u64(*obsolete_at_millis);
        }
    }
    Ok(())
}

fn read_pruning_entry(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<ReplicatorPruningEntry> {
    let removed = ReplicaId::new(reader.read_string()?);
    let state = match reader.read_u8()? {
        1 => {
            let owner = ReplicaId::new(reader.read_string()?);
            let seen_count = u64_to_len(reader.read_u64()?)?;
            let mut seen = Vec::with_capacity(seen_count);
            for _ in 0..seen_count {
                seen.push(ReplicaId::new(reader.read_string()?));
            }
            ReplicatorPruningState::Initialized { owner, seen }
        }
        2 => ReplicatorPruningState::Performed {
            obsolete_at_millis: reader.read_u64()?,
        },
        other => {
            return Err(SerializationError::Message(format!(
                "unknown ddata pruning state tag {other}"
            )));
        }
    };
    Ok(ReplicatorPruningEntry { removed, state })
}

fn ensure_empty_payload(payload: &Bytes, manifest: &str) -> kairo_serialization::Result<()> {
    if payload.is_empty() {
        Ok(())
    } else {
        Err(SerializationError::Message(format!(
            "{manifest} payload must be empty"
        )))
    }
}

fn len_to_u64(len: usize) -> kairo_serialization::Result<u64> {
    u64::try_from(len)
        .map_err(|_| SerializationError::Message("replicator delta count exceeds u64".to_string()))
}

fn u64_to_len(len: u64) -> kairo_serialization::Result<usize> {
    usize::try_from(len).map_err(|_| {
        SerializationError::Message("replicator delta count exceeds usize".to_string())
    })
}

#[cfg(test)]
mod tests {
    use kairo_serialization::{Manifest, RemoteMessage, SerializedMessage};

    use super::*;

    fn registry() -> Registry {
        let mut registry = Registry::new();
        register_ddata_protocol_codecs(&mut registry).unwrap();
        registry
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
}
