use std::any::{Any, TypeId, type_name};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{
    DynCodec, Manifest, MessageCodec, RemoteMessage, Result, SerializationError, SerializedMessage,
    SerializerId, codec::TypedCodec,
};

/// Codec registry API used by serialization-aware runtime boundaries.
///
/// The registry maps Rust message types to outbound codecs and maps stable
/// wire metadata to inbound codecs. Implementations must reject duplicate
/// serializer ids, duplicate manifests, and duplicate codecs for one Rust
/// message type.
pub trait SerializationRegistry {
    /// Registers one typed codec for `M`.
    fn register<M, C>(&mut self, codec: C) -> Result<()>
    where
        M: RemoteMessage,
        C: MessageCodec<M>;

    /// Resolves the outbound codec for a typed message.
    fn codec_for_type<M>(&self) -> Result<&dyn DynCodec>
    where
        M: RemoteMessage;

    /// Resolves the inbound codec for a serializer id and manifest pair.
    fn codec_for_wire(
        &self,
        serializer_id: SerializerId,
        manifest: &Manifest,
    ) -> Result<&dyn DynCodec>;

    /// Deserializes a wire message into a dynamic Rust message value.
    fn deserialize_dyn(&self, message: SerializedMessage) -> Result<Box<dyn Any + Send>>;
}

/// In-memory serialization registry.
///
/// `Registry` is the default registry implementation for local tests and
/// runtime construction. It stores codecs by Rust `TypeId` for outbound sends
/// and by `(serializer_id, manifest)` for inbound wire payloads.
#[derive(Default)]
pub struct Registry {
    by_type: HashMap<TypeId, Arc<dyn DynCodec>>,
    by_wire: HashMap<(SerializerId, Manifest), Arc<dyn DynCodec>>,
    serializer_ids: HashSet<SerializerId>,
    manifests: HashSet<Manifest>,
}

impl Registry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Serializes a typed remote message with its stable metadata.
    pub fn serialize<M>(&self, message: &M) -> Result<SerializedMessage>
    where
        M: RemoteMessage,
    {
        let codec = self.codec_for_type::<M>()?;
        let manifest = Manifest::try_new(M::MANIFEST)?;
        Ok(SerializedMessage::new(
            codec.serializer_id(),
            manifest,
            M::VERSION,
            codec.encode_dyn(message)?,
        ))
    }

    /// Deserializes a wire message as the expected typed message.
    ///
    /// The manifest is checked before payload decode so a registered codec for
    /// another message type is not accidentally used as the typed result.
    pub fn deserialize<M>(&self, message: SerializedMessage) -> Result<M>
    where
        M: RemoteMessage,
    {
        let expected_manifest = Manifest::try_new(M::MANIFEST)?;
        if message.manifest != expected_manifest {
            return Err(SerializationError::UnexpectedManifest {
                expected: M::MANIFEST,
                actual: message.manifest.as_str().to_string(),
            });
        }

        self.deserialize_dyn(message)?
            .downcast::<M>()
            .map(|message| *message)
            .map_err(|_| SerializationError::TypeMismatch {
                expected: type_name::<M>(),
            })
    }

    /// Deserializes a wire message to the dynamic inbound boundary.
    pub fn deserialize_dyn(&self, message: SerializedMessage) -> Result<Box<dyn Any + Send>> {
        let codec = self.codec_for_wire(message.serializer_id, &message.manifest)?;
        codec.decode_dyn(message.payload, message.version)
    }
}

impl SerializationRegistry for Registry {
    fn register<M, C>(&mut self, codec: C) -> Result<()>
    where
        M: RemoteMessage,
        C: MessageCodec<M>,
    {
        let manifest = TypedCodec::<M, C>::manifest()?;
        let serializer_id = codec.serializer_id();
        let type_id = TypeId::of::<M>();

        if self.serializer_ids.contains(&serializer_id) {
            return Err(SerializationError::DuplicateSerializerId(serializer_id));
        }
        if self.by_type.contains_key(&type_id) {
            return Err(SerializationError::DuplicateTypeCodec(type_name::<M>()));
        }
        if self.manifests.contains(&manifest) {
            return Err(SerializationError::DuplicateManifest(
                manifest.as_str().to_string(),
            ));
        }

        let codec: Arc<dyn DynCodec> = Arc::new(TypedCodec::<M, C>::new(codec));
        self.serializer_ids.insert(serializer_id);
        self.manifests.insert(manifest.clone());
        self.by_wire
            .insert((serializer_id, manifest), Arc::clone(&codec));
        self.by_type.insert(type_id, codec);
        Ok(())
    }

    fn codec_for_type<M>(&self) -> Result<&dyn DynCodec>
    where
        M: RemoteMessage,
    {
        self.by_type
            .get(&TypeId::of::<M>())
            .map(|codec| codec.as_ref())
            .ok_or(SerializationError::MissingTypeCodec(type_name::<M>()))
    }

    fn codec_for_wire(
        &self,
        serializer_id: SerializerId,
        manifest: &Manifest,
    ) -> Result<&dyn DynCodec> {
        self.by_wire
            .get(&(serializer_id, manifest.clone()))
            .map(|codec| codec.as_ref())
            .ok_or_else(|| SerializationError::MissingWireCodec {
                serializer_id,
                manifest: manifest.as_str().to_string(),
            })
    }

    fn deserialize_dyn(&self, message: SerializedMessage) -> Result<Box<dyn Any + Send>> {
        Registry::deserialize_dyn(self, message)
    }
}
