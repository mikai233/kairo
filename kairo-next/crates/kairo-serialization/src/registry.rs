use std::any::{TypeId, type_name};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{
    DynCodec, Manifest, MessageCodec, RemoteMessage, Result, SerializationError, SerializedMessage,
    SerializerId, codec::TypedCodec,
};

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

#[derive(Default)]
pub struct Registry {
    by_type: HashMap<TypeId, Arc<dyn DynCodec>>,
    by_wire: HashMap<(SerializerId, Manifest), Arc<dyn DynCodec>>,
    serializer_ids: HashSet<SerializerId>,
    manifests: HashSet<Manifest>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

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

    pub fn deserialize<M>(&self, message: SerializedMessage) -> Result<M>
    where
        M: RemoteMessage,
    {
        let codec = self.codec_for_wire(message.serializer_id, &message.manifest)?;
        let decoded = codec.decode_dyn(message.payload, message.version)?;
        decoded
            .downcast::<M>()
            .map(|message| *message)
            .map_err(|_| SerializationError::TypeMismatch {
                expected: type_name::<M>(),
            })
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
}
