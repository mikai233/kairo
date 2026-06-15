use bytes::Bytes;

use crate::{ActorRefWireData, Manifest, Result, SerializerId};

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
}
