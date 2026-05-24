//! Stable serialization contracts for remote and persistent messages.

use std::any::{Any, TypeId};

use bytes::Bytes;

pub type SerializerId = u32;
pub type Result<T> = std::result::Result<T, SerializationError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Manifest(String);

impl Manifest {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct SerializedMessage {
    pub serializer_id: SerializerId,
    pub manifest: Manifest,
    pub version: u16,
    pub payload: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum SerializationError {
    #[error("{0}")]
    Message(String),
}

pub trait RemoteMessage: Send + 'static {
    const MANIFEST: &'static str;
    const VERSION: u16;
}

pub trait MessageCodec<M>: Send + Sync + 'static
where
    M: RemoteMessage,
{
    fn serializer_id(&self) -> SerializerId;

    fn encode(&self, message: &M) -> Result<Bytes>;

    fn decode(&self, payload: Bytes, version: u16) -> Result<M>;
}

pub trait DynCodec: Send + Sync + 'static {
    fn serializer_id(&self) -> SerializerId;

    fn manifest(&self) -> &'static str;

    fn message_type_id(&self) -> TypeId;

    fn encode_dyn(&self, value: &dyn Any) -> Result<Bytes>;

    fn decode_dyn(&self, payload: Bytes, version: u16) -> Result<Box<dyn Any + Send>>;
}

pub trait SerializationRegistry {
    fn register<M, C>(&mut self, codec: C) -> Result<()>
    where
        M: RemoteMessage,
        C: MessageCodec<M>;

    fn codec_for_type<M>(&self) -> Result<&dyn DynCodec>
    where
        M: RemoteMessage;

    fn codec_for_wire(
        &self,
        serializer_id: SerializerId,
        manifest: &Manifest,
    ) -> Result<&dyn DynCodec>;
}
