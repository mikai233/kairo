use crate::SerializerId;

pub type Result<T> = std::result::Result<T, SerializationError>;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SerializationError {
    #[error("{0}")]
    Message(String),
    #[error("remote message manifest must not be empty: `{0}`")]
    InvalidManifest(String),
    #[error("serializer id `{0}` is already registered")]
    DuplicateSerializerId(SerializerId),
    #[error("manifest `{0}` is already registered")]
    DuplicateManifest(String),
    #[error("remote message type `{0}` already has a registered codec")]
    DuplicateTypeCodec(&'static str),
    #[error("no codec registered for remote message type `{0}`")]
    MissingTypeCodec(&'static str),
    #[error("no codec registered for serializer id `{serializer_id}` and manifest `{manifest}`")]
    MissingWireCodec {
        serializer_id: SerializerId,
        manifest: String,
    },
    #[error("codec expected remote message type `{expected}`")]
    TypeMismatch { expected: &'static str },
}
