use bytes::Bytes;
use kairo_serialization::{SerializationError, WireReader, WireWriter};

use crate::{GCounter, GSet, LWWRegister, PNCounter, ReplicaId};

pub const GSET_STRING_MANIFEST: &str = "kairo.ddata.gset-string";
pub const GCOUNTER_MANIFEST: &str = "kairo.ddata.gcounter";
pub const PNCOUNTER_MANIFEST: &str = "kairo.ddata.pncounter";
pub const LWW_REGISTER_STRING_MANIFEST: &str = "kairo.ddata.lww-register-string";
pub const CRDT_CODEC_VERSION: u16 = 1;

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

fn timestamp_to_wire(timestamp: i64) -> u64 {
    u64::from_be_bytes(timestamp.to_be_bytes())
}

fn timestamp_from_wire(timestamp: u64) -> i64 {
    i64::from_be_bytes(timestamp.to_be_bytes())
}
