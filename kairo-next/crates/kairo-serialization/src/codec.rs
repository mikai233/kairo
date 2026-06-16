use std::any::{Any, TypeId, type_name};
use std::marker::PhantomData;
use std::panic::{self, AssertUnwindSafe};

use bytes::Bytes;

use crate::{Manifest, RemoteMessage, Result, SerializationError, SerializerId};

/// Typed codec for one [`RemoteMessage`] protocol.
///
/// A codec owns the concrete payload format for `M`. The framework supplies
/// stable metadata through `RemoteMessage`; the codec supplies the serializer
/// id plus encode/decode logic for the payload bytes.
pub trait MessageCodec<M>: Send + Sync + 'static
where
    M: RemoteMessage,
{
    /// Serializer id written into outbound wire payloads.
    fn serializer_id(&self) -> SerializerId;

    /// Encodes a typed message into payload bytes.
    fn encode(&self, message: &M) -> Result<Bytes>;

    /// Decodes payload bytes for the supplied wire version.
    ///
    /// `version` is the value read from [`crate::SerializedMessage`], not
    /// necessarily the current `M::VERSION`, so codecs can support rolling
    /// compatibility.
    fn decode(&self, payload: Bytes, version: u16) -> Result<M>;
}

/// Type-erased codec boundary used by registries and remote inbound paths.
///
/// User code normally implements [`MessageCodec`]. The registry wraps that
/// typed codec behind `DynCodec` so it can resolve payloads by wire metadata
/// before downcasting back to the expected message type.
pub trait DynCodec: Send + Sync + 'static {
    /// Serializer id owned by this codec.
    fn serializer_id(&self) -> SerializerId;

    /// Stable manifest owned by this codec.
    fn manifest(&self) -> &'static str;

    /// Rust type id for the typed message handled by this codec.
    fn message_type_id(&self) -> TypeId;

    /// Encodes a dynamic value after verifying it has the expected Rust type.
    fn encode_dyn(&self, value: &dyn Any) -> Result<Bytes>;

    /// Decodes payload bytes into an owned dynamic message value.
    fn decode_dyn(&self, payload: Bytes, version: u16) -> Result<Box<dyn Any + Send>>;
}

pub(crate) struct TypedCodec<M, C> {
    codec: C,
    _message: PhantomData<fn(M)>,
}

impl<M, C> TypedCodec<M, C>
where
    M: RemoteMessage,
    C: MessageCodec<M>,
{
    pub(crate) fn new(codec: C) -> Self {
        Self {
            codec,
            _message: PhantomData,
        }
    }

    pub(crate) fn manifest() -> Result<Manifest> {
        Manifest::try_new(M::MANIFEST)
    }
}

impl<M, C> DynCodec for TypedCodec<M, C>
where
    M: RemoteMessage,
    C: MessageCodec<M>,
{
    fn serializer_id(&self) -> SerializerId {
        self.codec.serializer_id()
    }

    fn manifest(&self) -> &'static str {
        M::MANIFEST
    }

    fn message_type_id(&self) -> TypeId {
        TypeId::of::<M>()
    }

    fn encode_dyn(&self, value: &dyn Any) -> Result<Bytes> {
        let Some(message) = value.downcast_ref::<M>() else {
            return Err(SerializationError::TypeMismatch {
                expected: type_name::<M>(),
            });
        };
        panic::catch_unwind(AssertUnwindSafe(|| self.codec.encode(message)))
            .unwrap_or_else(|panic| Err(codec_panic_to_error("encode", panic)))
    }

    fn decode_dyn(&self, payload: Bytes, version: u16) -> Result<Box<dyn Any + Send>> {
        let message = panic::catch_unwind(AssertUnwindSafe(|| self.codec.decode(payload, version)))
            .unwrap_or_else(|panic| Err(codec_panic_to_error("decode", panic)))?;
        Ok(Box::new(message) as Box<dyn Any + Send>)
    }
}

fn codec_panic_to_error(operation: &str, panic: Box<dyn Any + Send>) -> SerializationError {
    let message = if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    };
    SerializationError::Message(format!("codec {operation} panicked: {message}"))
}
