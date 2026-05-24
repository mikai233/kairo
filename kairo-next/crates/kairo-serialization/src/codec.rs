use std::any::{Any, TypeId, type_name};
use std::marker::PhantomData;

use bytes::Bytes;

use crate::{Manifest, RemoteMessage, Result, SerializationError, SerializerId};

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
        self.codec.encode(message)
    }

    fn decode_dyn(&self, payload: Bytes, version: u16) -> Result<Box<dyn Any + Send>> {
        self.codec
            .decode(payload, version)
            .map(|message| Box::new(message) as Box<dyn Any + Send>)
    }
}
