use crate::SerializerId;

/// Result type used by serialization APIs.
pub type Result<T> = std::result::Result<T, SerializationError>;

/// Errors returned by stable serialization metadata, codec, and wire helpers.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SerializationError {
    /// General serialization error with a human-readable message.
    #[error("{0}")]
    Message(String),
    /// A remote message manifest was empty or whitespace-only.
    #[error("remote message manifest must not be empty: `{0}`")]
    InvalidManifest(String),
    /// An actor-ref path could not be parsed into stable wire data.
    #[error("actor ref path `{0}` is invalid")]
    InvalidActorRefPath(String),
    /// A serializer id was registered more than once.
    #[error("serializer id `{0}` is already registered")]
    DuplicateSerializerId(SerializerId),
    /// A manifest was registered more than once.
    #[error("manifest `{0}` is already registered")]
    DuplicateManifest(String),
    /// A Rust message type already has an outbound codec.
    #[error("remote message type `{0}` already has a registered codec")]
    DuplicateTypeCodec(&'static str),
    /// No outbound codec is registered for the Rust message type.
    #[error("no codec registered for remote message type `{0}`")]
    MissingTypeCodec(&'static str),
    /// No inbound codec is registered for the wire metadata pair.
    #[error("no codec registered for serializer id `{serializer_id}` and manifest `{manifest}`")]
    MissingWireCodec {
        /// Serializer id read from the wire payload.
        serializer_id: SerializerId,
        /// Manifest read from the wire payload.
        manifest: String,
    },
    /// A typed deserialize call received a manifest for another message type.
    #[error("expected remote message manifest `{expected}`, got `{actual}`")]
    UnexpectedManifest {
        /// Manifest required by the requested Rust message type.
        expected: &'static str,
        /// Manifest carried by the serialized message.
        actual: String,
    },
    /// A dynamic codec boundary received or produced the wrong Rust type.
    #[error("codec expected remote message type `{expected}`")]
    TypeMismatch {
        /// Rust type name expected at the dynamic boundary.
        expected: &'static str,
    },
}
