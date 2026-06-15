use bytes::Bytes;

use crate::{ActorRefWireData, Manifest, Result, SerializerId, WireReader, WireWriter};

/// Serialized user or system message payload.
///
/// This is the stable metadata tuple carried inside remote envelopes:
/// serializer id, manifest, version, and payload bytes. The tuple is explicit
/// so remote delivery never depends on Rust implementation details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerializedMessage {
    /// Serializer id used to select the codec.
    pub serializer_id: SerializerId,
    /// Stable message manifest used with the serializer id to select the codec.
    pub manifest: Manifest,
    /// Wire schema version emitted by the sender.
    pub version: u16,
    /// Codec-owned payload bytes.
    pub payload: Bytes,
}

impl SerializedMessage {
    /// Creates a serialized message from explicit wire metadata and bytes.
    pub fn new(
        serializer_id: SerializerId,
        manifest: Manifest,
        version: u16,
        payload: Bytes,
    ) -> Self {
        Self {
            serializer_id,
            manifest,
            version,
            payload,
        }
    }

    /// Encodes the serialized message metadata tuple into deterministic bytes.
    ///
    /// The layout is explicit and stable for Kairo system protocols:
    /// serializer id, manifest, version, and payload bytes. It does not embed
    /// Rust type names, enum discriminants, or memory layout.
    pub fn encode_wire(&self) -> Result<Bytes> {
        let mut writer = WireWriter::new();
        self.write_wire(&mut writer)?;
        Ok(writer.finish())
    }

    /// Decodes a serialized message from bytes produced by [`Self::encode_wire`].
    pub fn decode_wire(bytes: &Bytes) -> Result<Self> {
        let mut reader = WireReader::new(bytes);
        let message = Self::read_wire(&mut reader)?;
        reader.ensure_finished()?;
        Ok(message)
    }

    pub(crate) fn write_wire(&self, writer: &mut WireWriter) -> Result<()> {
        writer.write_u32(self.serializer_id);
        writer.write_string(self.manifest.as_str())?;
        writer.write_u16(self.version);
        writer.write_bytes(&self.payload)
    }

    pub(crate) fn read_wire(reader: &mut WireReader<'_>) -> Result<Self> {
        let serializer_id = reader.read_u32()?;
        let manifest = Manifest::try_new(reader.read_string()?)?;
        let version = reader.read_u16()?;
        let payload = reader.read_bytes()?;
        Ok(Self::new(serializer_id, manifest, version, payload))
    }
}

/// Remote delivery envelope with stable actor-ref wire data.
///
/// The envelope carries an addressed recipient, an optional sender, and the
/// already serialized message payload. It is the transport-neutral boundary
/// consumed by remoting, cluster, distributed-data, sharding, and tools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteEnvelope {
    /// Remote or local actor path that should receive the message.
    pub recipient: ActorRefWireData,
    /// Optional sender actor path for diagnostics and protocol replies.
    pub sender: Option<ActorRefWireData>,
    /// Serialized message and its stable metadata.
    pub message: SerializedMessage,
}

impl RemoteEnvelope {
    /// Creates a remote envelope from validated actor-ref wire data.
    pub fn new(
        recipient: ActorRefWireData,
        sender: Option<ActorRefWireData>,
        message: SerializedMessage,
    ) -> Self {
        Self {
            recipient,
            sender,
            message,
        }
    }

    /// Creates a remote envelope from actor-ref path strings.
    ///
    /// This validates recipient and sender paths through [`ActorRefWireData`]
    /// before constructing the envelope.
    pub fn from_paths(
        recipient: impl Into<String>,
        sender: Option<String>,
        message: SerializedMessage,
    ) -> Result<Self> {
        Ok(Self {
            recipient: ActorRefWireData::new(recipient)?,
            sender: sender.map(ActorRefWireData::new).transpose()?,
            message,
        })
    }

    /// Encodes the remote envelope into deterministic bytes.
    ///
    /// The actor refs are encoded as canonical path strings and validated on
    /// decode through [`ActorRefWireData`]. The message payload remains the
    /// explicit stable [`SerializedMessage`] tuple.
    pub fn encode_wire(&self) -> Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(self.recipient.path())?;
        writer.write_optional_string(self.sender.as_ref().map(ActorRefWireData::path))?;
        self.message.write_wire(&mut writer)?;
        Ok(writer.finish())
    }

    /// Decodes a remote envelope from bytes produced by [`Self::encode_wire`].
    pub fn decode_wire(bytes: &Bytes) -> Result<Self> {
        let mut reader = WireReader::new(bytes);
        let recipient = ActorRefWireData::new(reader.read_string()?)?;
        let sender = reader
            .read_optional_string()?
            .map(ActorRefWireData::new)
            .transpose()?;
        let message = SerializedMessage::read_wire(&mut reader)?;
        reader.ensure_finished()?;
        Ok(Self::new(recipient, sender, message))
    }
}
