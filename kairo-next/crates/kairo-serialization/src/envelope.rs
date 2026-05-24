use bytes::Bytes;

use crate::{ActorRefWireData, Manifest, Result, SerializerId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerializedMessage {
    pub serializer_id: SerializerId,
    pub manifest: Manifest,
    pub version: u16,
    pub payload: Bytes,
}

impl SerializedMessage {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteEnvelope {
    pub recipient: ActorRefWireData,
    pub sender: Option<ActorRefWireData>,
    pub message: SerializedMessage,
}

impl RemoteEnvelope {
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
