use std::collections::BTreeMap;

use bytes::Bytes;
use kairo_serialization::{SerializationError, WireReader, WireWriter};

use crate::{
    GCounter, GSet, LWWRegister, ORMap, ORMapDelta, ORSet, ORSetDelta, ORSetRemoveDelta, PNCounter,
    ReplicaId,
};

pub const GSET_STRING_MANIFEST: &str = "kairo.ddata.gset-string";
pub const GSET_STRING_DELTA_MANIFEST: &str = "kairo.ddata.gset-string-delta";
pub const GCOUNTER_MANIFEST: &str = "kairo.ddata.gcounter";
pub const PNCOUNTER_MANIFEST: &str = "kairo.ddata.pncounter";
pub const LWW_REGISTER_STRING_MANIFEST: &str = "kairo.ddata.lww-register-string";
pub const ORSET_STRING_MANIFEST: &str = "kairo.ddata.orset-string";
pub const ORSET_STRING_DELTA_MANIFEST: &str = "kairo.ddata.orset-string-delta";
pub const ORMAP_STRING_GSET_MANIFEST: &str = "kairo.ddata.ormap-string-gset";
pub const ORMAP_STRING_GSET_DELTA_MANIFEST: &str = "kairo.ddata.ormap-string-gset-delta";
pub const CRDT_CODEC_VERSION: u16 = 1;

const ORSET_DELTA_ADD_TAG: u8 = 1;
const ORSET_DELTA_REMOVE_TAG: u8 = 2;
const ORSET_DELTA_GROUP_TAG: u8 = 3;
const ORMAP_DELTA_PUT_TAG: u8 = 1;
const ORMAP_DELTA_UPDATE_TAG: u8 = 2;
const ORMAP_DELTA_REMOVE_TAG: u8 = 3;
const ORMAP_DELTA_GROUP_TAG: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerializedCrdt {
    manifest: &'static str,
    version: u16,
    payload: Bytes,
}

impl SerializedCrdt {
    pub fn new(manifest: &'static str, version: u16, payload: Bytes) -> Self {
        Self {
            manifest,
            version,
            payload,
        }
    }

    pub fn manifest(&self) -> &'static str {
        self.manifest
    }

    pub fn version(&self) -> u16 {
        self.version
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn into_payload(self) -> Bytes {
        self.payload
    }
}

pub trait CrdtDataCodec<D> {
    fn manifest(&self) -> &'static str;

    fn version(&self) -> u16 {
        CRDT_CODEC_VERSION
    }

    fn encode_payload(&self, data: &D) -> kairo_serialization::Result<Bytes>;

    fn decode_payload(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<D>;

    fn serialize(&self, data: &D) -> kairo_serialization::Result<SerializedCrdt> {
        Ok(SerializedCrdt::new(
            self.manifest(),
            self.version(),
            self.encode_payload(data)?,
        ))
    }

    fn deserialize(&self, data: SerializedCrdt) -> kairo_serialization::Result<D> {
        if data.manifest() != self.manifest() {
            return Err(SerializationError::Message(format!(
                "expected CRDT manifest {}, got {}",
                self.manifest(),
                data.manifest()
            )));
        }
        let version = data.version();
        self.decode_payload(data.into_payload(), version)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GSetStringCodec;

impl CrdtDataCodec<GSet<String>> for GSetStringCodec {
    fn manifest(&self) -> &'static str {
        GSET_STRING_MANIFEST
    }

    fn encode_payload(&self, data: &GSet<String>) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_u64(len_to_u64(data.len())?);
        for element in data.elements() {
            writer.write_string(element)?;
        }
        Ok(writer.finish())
    }

    fn decode_payload(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<GSet<String>> {
        ensure_version(self.manifest(), version)?;
        let mut reader = WireReader::new(&payload);
        let len = reader.read_u64()?;
        let mut elements = Vec::with_capacity(u64_to_len(len)?);
        for _ in 0..len {
            elements.push(reader.read_string()?);
        }
        reader.ensure_finished()?;
        Ok(GSet::from_elements(elements))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GSetStringDeltaCodec;

impl CrdtDataCodec<GSet<String>> for GSetStringDeltaCodec {
    fn manifest(&self) -> &'static str {
        GSET_STRING_DELTA_MANIFEST
    }

    fn encode_payload(&self, data: &GSet<String>) -> kairo_serialization::Result<Bytes> {
        GSetStringCodec.encode_payload(data)
    }

    fn decode_payload(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<GSet<String>> {
        ensure_version(self.manifest(), version)?;
        GSetStringCodec.decode_payload(payload, CRDT_CODEC_VERSION)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GCounterCodec;

impl CrdtDataCodec<GCounter> for GCounterCodec {
    fn manifest(&self) -> &'static str {
        GCOUNTER_MANIFEST
    }

    fn encode_payload(&self, data: &GCounter) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_u64(len_to_u64(data.state().len())?);
        for (replica, value) in data.state() {
            writer.write_string(replica.as_str())?;
            writer.write_u128(*value);
        }
        Ok(writer.finish())
    }

    fn decode_payload(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<GCounter> {
        ensure_version(self.manifest(), version)?;
        let mut reader = WireReader::new(&payload);
        let len = reader.read_u64()?;
        let mut state = Vec::with_capacity(u64_to_len(len)?);
        for _ in 0..len {
            state.push((ReplicaId::new(reader.read_string()?), reader.read_u128()?));
        }
        reader.ensure_finished()?;
        Ok(GCounter::from_state(state))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PNCounterCodec;

impl CrdtDataCodec<PNCounter> for PNCounterCodec {
    fn manifest(&self) -> &'static str {
        PNCOUNTER_MANIFEST
    }

    fn encode_payload(&self, data: &PNCounter) -> kairo_serialization::Result<Bytes> {
        let increments = GCounterCodec.encode_payload(data.increments())?;
        let decrements = GCounterCodec.encode_payload(data.decrements())?;
        let mut writer = WireWriter::new();
        writer.write_u64(len_to_u64(increments.len())?);
        writer.write_u64(len_to_u64(decrements.len())?);
        let mut bytes = writer.finish().to_vec();
        bytes.extend_from_slice(&increments);
        bytes.extend_from_slice(&decrements);
        Ok(Bytes::from(bytes))
    }

    fn decode_payload(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<PNCounter> {
        ensure_version(self.manifest(), version)?;
        let mut reader = WireReader::new(&payload);
        let increments_len = u64_to_len(reader.read_u64()?)?;
        let decrements_len = u64_to_len(reader.read_u64()?)?;
        let increments = Bytes::copy_from_slice(reader.read_exact(increments_len)?);
        let decrements = Bytes::copy_from_slice(reader.read_exact(decrements_len)?);
        reader.ensure_finished()?;
        Ok(PNCounter::from_counters(
            GCounterCodec.decode_payload(increments, CRDT_CODEC_VERSION)?,
            GCounterCodec.decode_payload(decrements, CRDT_CODEC_VERSION)?,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LWWRegisterStringCodec;

impl CrdtDataCodec<LWWRegister<String>> for LWWRegisterStringCodec {
    fn manifest(&self) -> &'static str {
        LWW_REGISTER_STRING_MANIFEST
    }

    fn encode_payload(&self, data: &LWWRegister<String>) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(data.node().as_str())?;
        writer.write_u64(timestamp_to_wire(data.timestamp()));
        writer.write_string(data.value())?;
        Ok(writer.finish())
    }

    fn decode_payload(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<LWWRegister<String>> {
        ensure_version(self.manifest(), version)?;
        let mut reader = WireReader::new(&payload);
        let node = ReplicaId::new(reader.read_string()?);
        let timestamp = timestamp_from_wire(reader.read_u64()?);
        let value = reader.read_string()?;
        reader.ensure_finished()?;
        Ok(LWWRegister::new(node, value, timestamp))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ORSetStringCodec;

impl CrdtDataCodec<ORSet<String>> for ORSetStringCodec {
    fn manifest(&self) -> &'static str {
        ORSET_STRING_MANIFEST
    }

    fn encode_payload(&self, data: &ORSet<String>) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_version_vector(&mut writer, data.version_vector_entries())?;
        writer.write_u64(len_to_u64(data.len())?);
        for (element, dots) in data.element_dots() {
            writer.write_string(element)?;
            write_version_vector(&mut writer, dots)?;
        }
        Ok(writer.finish())
    }

    fn decode_payload(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ORSet<String>> {
        ensure_version(self.manifest(), version)?;
        let mut reader = WireReader::new(&payload);
        let version_vector = read_version_vector(&mut reader)?;
        let len = reader.read_u64()?;
        let mut elements = Vec::with_capacity(u64_to_len(len)?);
        for _ in 0..len {
            let element = reader.read_string()?;
            let dots = read_version_vector(&mut reader)?;
            elements.push((element, dots));
        }
        reader.ensure_finished()?;
        Ok(ORSet::from_wire_state(elements, version_vector))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ORSetStringDeltaCodec;

impl CrdtDataCodec<ORSetDelta<String>> for ORSetStringDeltaCodec {
    fn manifest(&self) -> &'static str {
        ORSET_STRING_DELTA_MANIFEST
    }

    fn encode_payload(&self, data: &ORSetDelta<String>) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_orset_delta(&mut writer, data)?;
        Ok(writer.finish())
    }

    fn decode_payload(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ORSetDelta<String>> {
        ensure_version(self.manifest(), version)?;
        let mut reader = WireReader::new(&payload);
        let delta = read_orset_delta(&mut reader)?;
        reader.ensure_finished()?;
        Ok(delta)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ORMapStringGSetCodec;

impl CrdtDataCodec<ORMap<String, GSet<String>>> for ORMapStringGSetCodec {
    fn manifest(&self) -> &'static str {
        ORMAP_STRING_GSET_MANIFEST
    }

    fn encode_payload(
        &self,
        data: &ORMap<String, GSet<String>>,
    ) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_bytes(&ORSetStringCodec.encode_payload(data.key_set())?)?;
        writer.write_u64(len_to_u64(data.entries().len())?);
        for (key, value) in data.entries() {
            writer.write_string(key)?;
            writer.write_bytes(&GSetStringCodec.encode_payload(value)?)?;
        }
        Ok(writer.finish())
    }

    fn decode_payload(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ORMap<String, GSet<String>>> {
        ensure_version(self.manifest(), version)?;
        let mut reader = WireReader::new(&payload);
        let keys = ORSetStringCodec.decode_payload(reader.read_bytes()?, CRDT_CODEC_VERSION)?;
        let len = reader.read_u64()?;
        let mut values = BTreeMap::new();
        for _ in 0..len {
            let key = reader.read_string()?;
            let value = GSetStringCodec.decode_payload(reader.read_bytes()?, CRDT_CODEC_VERSION)?;
            values.insert(key, value);
        }
        reader.ensure_finished()?;
        Ok(ORMap::from_wire_state(keys, values))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ORMapStringGSetDeltaCodec;

impl CrdtDataCodec<ORMapDelta<String, GSet<String>>> for ORMapStringGSetDeltaCodec {
    fn manifest(&self) -> &'static str {
        ORMAP_STRING_GSET_DELTA_MANIFEST
    }

    fn encode_payload(
        &self,
        data: &ORMapDelta<String, GSet<String>>,
    ) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_ormap_delta(&mut writer, data)?;
        Ok(writer.finish())
    }

    fn decode_payload(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ORMapDelta<String, GSet<String>>> {
        ensure_version(self.manifest(), version)?;
        let mut reader = WireReader::new(&payload);
        let delta = read_ormap_delta(&mut reader)?;
        reader.ensure_finished()?;
        Ok(delta)
    }
}

fn ensure_version(manifest: &str, version: u16) -> kairo_serialization::Result<()> {
    if version == CRDT_CODEC_VERSION {
        Ok(())
    } else {
        Err(SerializationError::Message(format!(
            "unsupported {manifest} version {version}"
        )))
    }
}

fn len_to_u64(len: usize) -> kairo_serialization::Result<u64> {
    u64::try_from(len)
        .map_err(|_| SerializationError::Message("CRDT collection length exceeds u64".to_string()))
}

fn u64_to_len(len: u64) -> kairo_serialization::Result<usize> {
    usize::try_from(len).map_err(|_| {
        SerializationError::Message("CRDT collection length exceeds usize".to_string())
    })
}

fn write_ormap_delta(
    writer: &mut WireWriter,
    delta: &ORMapDelta<String, GSet<String>>,
) -> kairo_serialization::Result<()> {
    match delta {
        ORMapDelta::Put { keys, key, value } => {
            writer.write_u8(ORMAP_DELTA_PUT_TAG);
            writer.write_bytes(&ORSetStringDeltaCodec.encode_payload(keys)?)?;
            writer.write_string(key)?;
            writer.write_bytes(&GSetStringCodec.encode_payload(value)?)?;
        }
        ORMapDelta::Update { keys, values } => {
            writer.write_u8(ORMAP_DELTA_UPDATE_TAG);
            writer.write_bytes(&ORSetStringDeltaCodec.encode_payload(keys)?)?;
            writer.write_u64(len_to_u64(values.len())?);
            for (key, value_delta) in values {
                writer.write_string(key)?;
                writer.write_bytes(&GSetStringDeltaCodec.encode_payload(value_delta)?)?;
            }
        }
        ORMapDelta::Remove { keys } => {
            writer.write_u8(ORMAP_DELTA_REMOVE_TAG);
            writer.write_bytes(&ORSetStringDeltaCodec.encode_payload(keys)?)?;
        }
        ORMapDelta::Group(ops) => {
            writer.write_u8(ORMAP_DELTA_GROUP_TAG);
            writer.write_u64(len_to_u64(ops.len())?);
            for op in ops {
                write_ormap_delta(writer, op)?;
            }
        }
    }
    Ok(())
}

fn read_ormap_delta(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<ORMapDelta<String, GSet<String>>> {
    match reader.read_u8()? {
        ORMAP_DELTA_PUT_TAG => {
            let keys =
                ORSetStringDeltaCodec.decode_payload(reader.read_bytes()?, CRDT_CODEC_VERSION)?;
            let key = reader.read_string()?;
            let value = GSetStringCodec.decode_payload(reader.read_bytes()?, CRDT_CODEC_VERSION)?;
            Ok(ORMapDelta::Put { keys, key, value })
        }
        ORMAP_DELTA_UPDATE_TAG => {
            let keys =
                ORSetStringDeltaCodec.decode_payload(reader.read_bytes()?, CRDT_CODEC_VERSION)?;
            let len = reader.read_u64()?;
            let mut values = BTreeMap::new();
            for _ in 0..len {
                values.insert(
                    reader.read_string()?,
                    GSetStringDeltaCodec
                        .decode_payload(reader.read_bytes()?, CRDT_CODEC_VERSION)?,
                );
            }
            Ok(ORMapDelta::Update { keys, values })
        }
        ORMAP_DELTA_REMOVE_TAG => {
            let keys =
                ORSetStringDeltaCodec.decode_payload(reader.read_bytes()?, CRDT_CODEC_VERSION)?;
            Ok(ORMapDelta::Remove { keys })
        }
        ORMAP_DELTA_GROUP_TAG => {
            let len = reader.read_u64()?;
            let mut ops = Vec::with_capacity(u64_to_len(len)?);
            for _ in 0..len {
                ops.push(read_ormap_delta(reader)?);
            }
            Ok(ORMapDelta::Group(ops))
        }
        other => Err(SerializationError::Message(format!(
            "unknown ORMap delta operation tag {other}"
        ))),
    }
}

fn write_orset_delta(
    writer: &mut WireWriter,
    delta: &ORSetDelta<String>,
) -> kairo_serialization::Result<()> {
    match delta {
        ORSetDelta::Add(add) => {
            writer.write_u8(ORSET_DELTA_ADD_TAG);
            writer.write_bytes(&ORSetStringCodec.encode_payload(add)?)?;
        }
        ORSetDelta::Remove(remove) => {
            writer.write_u8(ORSET_DELTA_REMOVE_TAG);
            writer.write_string(remove.element())?;
            write_version_vector(writer, remove.seen_entries())?;
            write_version_vector(writer, remove.remove_dot_entries())?;
        }
        ORSetDelta::Group(ops) => {
            writer.write_u8(ORSET_DELTA_GROUP_TAG);
            writer.write_u64(len_to_u64(ops.len())?);
            for op in ops {
                write_orset_delta(writer, op)?;
            }
        }
    }
    Ok(())
}

fn read_orset_delta(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<ORSetDelta<String>> {
    match reader.read_u8()? {
        ORSET_DELTA_ADD_TAG => {
            let payload = reader.read_bytes()?;
            Ok(ORSetDelta::Add(
                ORSetStringCodec.decode_payload(payload, CRDT_CODEC_VERSION)?,
            ))
        }
        ORSET_DELTA_REMOVE_TAG => {
            let element = reader.read_string()?;
            let seen = read_version_vector(reader)?;
            let remove_dot = read_version_vector(reader)?;
            Ok(ORSetDelta::Remove(ORSetRemoveDelta::from_wire_state(
                element, seen, remove_dot,
            )))
        }
        ORSET_DELTA_GROUP_TAG => {
            let len = reader.read_u64()?;
            let mut ops = Vec::with_capacity(u64_to_len(len)?);
            for _ in 0..len {
                ops.push(read_orset_delta(reader)?);
            }
            Ok(ORSetDelta::Group(ops))
        }
        other => Err(SerializationError::Message(format!(
            "unknown ORSet delta operation tag {other}"
        ))),
    }
}

fn write_version_vector(
    writer: &mut WireWriter,
    entries: &BTreeMap<ReplicaId, u64>,
) -> kairo_serialization::Result<()> {
    writer.write_u64(len_to_u64(entries.len())?);
    for (replica, version) in entries {
        writer.write_string(replica.as_str())?;
        writer.write_u64(*version);
    }
    Ok(())
}

fn read_version_vector(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<BTreeMap<ReplicaId, u64>> {
    let len = reader.read_u64()?;
    let mut entries = BTreeMap::new();
    for _ in 0..len {
        entries.insert(ReplicaId::new(reader.read_string()?), reader.read_u64()?);
    }
    Ok(entries)
}

fn timestamp_to_wire(timestamp: i64) -> u64 {
    u64::from_be_bytes(timestamp.to_be_bytes())
}

fn timestamp_from_wire(timestamp: u64) -> i64 {
    i64::from_be_bytes(timestamp.to_be_bytes())
}
