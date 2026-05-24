use bytes::Bytes;

use crate::{Manifest, SerializerId};

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
    pub recipient: String,
    pub sender: Option<String>,
    pub message: SerializedMessage,
}

impl RemoteEnvelope {
    pub fn new(
        recipient: impl Into<String>,
        sender: Option<String>,
        message: SerializedMessage,
    ) -> Self {
        Self {
            recipient: recipient.into(),
            sender,
            message,
        }
    }
}
