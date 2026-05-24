use bytes::Bytes;

use crate::{
    ActorRefResolver, ActorRefWireData, Manifest, MessageCodec, Registry, RemoteEnvelope,
    RemoteMessage, SerializationError, SerializationRegistry, SerializedMessage,
};

#[derive(Debug, PartialEq, Eq)]
struct CounterCommand {
    amount: u8,
}

impl RemoteMessage for CounterCommand {
    const MANIFEST: &'static str = "kairo.test.CounterCommand";
    const VERSION: u16 = 7;
}

#[derive(Debug, PartialEq, Eq)]
struct OtherCommand {
    amount: u8,
}

impl RemoteMessage for OtherCommand {
    const MANIFEST: &'static str = "kairo.test.OtherCommand";
    const VERSION: u16 = 1;
}

#[derive(Debug, PartialEq, Eq)]
struct DuplicateManifestCommand;

impl RemoteMessage for DuplicateManifestCommand {
    const MANIFEST: &'static str = CounterCommand::MANIFEST;
    const VERSION: u16 = 1;
}

#[derive(Debug, PartialEq, Eq)]
struct EmptyManifestCommand;

impl RemoteMessage for EmptyManifestCommand {
    const MANIFEST: &'static str = " ";
    const VERSION: u16 = 1;
}

#[derive(Debug, PartialEq, Eq)]
#[repr(u8)]
enum StableEnumCommand {
    Increment = 99,
}

impl RemoteMessage for StableEnumCommand {
    const MANIFEST: &'static str = "kairo.test.StableEnumCommand";
    const VERSION: u16 = 3;
}

#[derive(Debug, Clone, Copy)]
struct SingleByteCodec {
    serializer_id: u32,
}

impl MessageCodec<CounterCommand> for SingleByteCodec {
    fn serializer_id(&self) -> u32 {
        self.serializer_id
    }

    fn encode(&self, message: &CounterCommand) -> crate::Result<Bytes> {
        Ok(Bytes::from(vec![message.amount]))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> crate::Result<CounterCommand> {
        Ok(CounterCommand { amount: payload[0] })
    }
}

impl MessageCodec<OtherCommand> for SingleByteCodec {
    fn serializer_id(&self) -> u32 {
        self.serializer_id
    }

    fn encode(&self, message: &OtherCommand) -> crate::Result<Bytes> {
        Ok(Bytes::from(vec![message.amount]))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> crate::Result<OtherCommand> {
        Ok(OtherCommand { amount: payload[0] })
    }
}

impl MessageCodec<DuplicateManifestCommand> for SingleByteCodec {
    fn serializer_id(&self) -> u32 {
        self.serializer_id
    }

    fn encode(&self, _message: &DuplicateManifestCommand) -> crate::Result<Bytes> {
        Ok(Bytes::new())
    }

    fn decode(&self, _payload: Bytes, _version: u16) -> crate::Result<DuplicateManifestCommand> {
        Ok(DuplicateManifestCommand)
    }
}

impl MessageCodec<EmptyManifestCommand> for SingleByteCodec {
    fn serializer_id(&self) -> u32 {
        self.serializer_id
    }

    fn encode(&self, _message: &EmptyManifestCommand) -> crate::Result<Bytes> {
        Ok(Bytes::new())
    }

    fn decode(&self, _payload: Bytes, _version: u16) -> crate::Result<EmptyManifestCommand> {
        Ok(EmptyManifestCommand)
    }
}

impl MessageCodec<StableEnumCommand> for SingleByteCodec {
    fn serializer_id(&self) -> u32 {
        self.serializer_id
    }

    fn encode(&self, message: &StableEnumCommand) -> crate::Result<Bytes> {
        let value = match message {
            StableEnumCommand::Increment => 1,
        };
        Ok(Bytes::from(vec![value]))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> crate::Result<StableEnumCommand> {
        match payload[0] {
            1 => Ok(StableEnumCommand::Increment),
            other => Err(SerializationError::Message(format!(
                "unknown command byte {other}"
            ))),
        }
    }
}

#[test]
fn registry_serializes_with_stable_wire_metadata() {
    let mut registry = Registry::new();
    registry
        .register::<CounterCommand, _>(SingleByteCodec { serializer_id: 41 })
        .unwrap();

    let serialized = registry
        .serialize(&CounterCommand { amount: 5 })
        .expect("message should serialize");

    assert_eq!(serialized.serializer_id, 41);
    assert_eq!(serialized.manifest.as_str(), "kairo.test.CounterCommand");
    assert_eq!(serialized.version, 7);
    assert_eq!(serialized.payload, Bytes::from_static(&[5]));
    assert!(
        !serialized
            .manifest
            .as_str()
            .contains(std::any::type_name::<CounterCommand>())
    );

    let decoded: CounterCommand = registry.deserialize(serialized).unwrap();
    assert_eq!(decoded, CounterCommand { amount: 5 });
}

#[test]
fn registry_rejects_duplicate_serializer_ids() {
    let mut registry = Registry::new();
    registry
        .register::<CounterCommand, _>(SingleByteCodec { serializer_id: 41 })
        .unwrap();

    let error = registry
        .register::<OtherCommand, _>(SingleByteCodec { serializer_id: 41 })
        .expect_err("duplicate serializer id should fail");

    assert_eq!(error, SerializationError::DuplicateSerializerId(41));
}

#[test]
fn registry_rejects_duplicate_manifests() {
    let mut registry = Registry::new();
    registry
        .register::<CounterCommand, _>(SingleByteCodec { serializer_id: 41 })
        .unwrap();

    let error = registry
        .register::<DuplicateManifestCommand, _>(SingleByteCodec { serializer_id: 42 })
        .expect_err("duplicate manifest should fail");

    assert_eq!(
        error,
        SerializationError::DuplicateManifest("kairo.test.CounterCommand".to_string())
    );
}

#[test]
fn registry_rejects_duplicate_type_codecs() {
    let mut registry = Registry::new();
    registry
        .register::<CounterCommand, _>(SingleByteCodec { serializer_id: 41 })
        .unwrap();

    let error = registry
        .register::<CounterCommand, _>(SingleByteCodec { serializer_id: 42 })
        .expect_err("duplicate type codec should fail");

    assert!(matches!(error, SerializationError::DuplicateTypeCodec(_)));
}

#[test]
fn registry_rejects_empty_manifest() {
    let mut registry = Registry::new();

    let error = registry
        .register::<EmptyManifestCommand, _>(SingleByteCodec { serializer_id: 42 })
        .expect_err("empty manifest should fail");

    assert_eq!(error, SerializationError::InvalidManifest(" ".to_string()));
}

#[test]
fn codec_for_wire_uses_serializer_id_and_manifest_pair() {
    let mut registry = Registry::new();
    registry
        .register::<CounterCommand, _>(SingleByteCodec { serializer_id: 41 })
        .unwrap();

    let codec = registry
        .codec_for_wire(41, &Manifest::new("kairo.test.CounterCommand"))
        .expect("wire codec should resolve");

    assert_eq!(codec.serializer_id(), 41);
    assert_eq!(codec.manifest(), "kairo.test.CounterCommand");
}

#[test]
fn enum_discriminants_are_not_wire_contracts() {
    let mut registry = Registry::new();
    registry
        .register::<StableEnumCommand, _>(SingleByteCodec { serializer_id: 50 })
        .unwrap();

    let serialized = registry.serialize(&StableEnumCommand::Increment).unwrap();

    assert_eq!(StableEnumCommand::Increment as u8, 99);
    assert_eq!(serialized.payload, Bytes::from_static(&[1]));
    assert_eq!(serialized.manifest.as_str(), "kairo.test.StableEnumCommand");
    assert_eq!(serialized.version, 3);
}

#[test]
fn actor_ref_wire_data_keeps_path_and_address_parts() {
    let wire = ActorRefWireData::new("kairo://system@127.0.0.1:25520/user/counter#9").unwrap();

    assert_eq!(wire.path(), "kairo://system@127.0.0.1:25520/user/counter#9");
    assert_eq!(wire.protocol(), "kairo");
    assert_eq!(wire.system(), "system");
    assert_eq!(wire.host(), Some("127.0.0.1"));
    assert_eq!(wire.port(), Some(25520));
}

#[test]
fn actor_ref_wire_data_rejects_invalid_paths() {
    let error = ActorRefWireData::new("/user/counter").expect_err("path should be invalid");

    assert_eq!(
        error,
        SerializationError::InvalidActorRefPath("/user/counter".to_string())
    );
}

#[test]
fn remote_envelope_uses_actor_ref_wire_data() {
    let message = SerializedMessage::new(
        1,
        Manifest::new("kairo.test.CounterCommand"),
        1,
        Bytes::from_static(&[1]),
    );

    let envelope = RemoteEnvelope::from_paths(
        "kairo://system@127.0.0.1:25520/user/counter#9",
        Some("kairo://system/user/sender#10".to_string()),
        message,
    )
    .unwrap();

    assert_eq!(envelope.recipient.system(), "system");
    assert_eq!(envelope.recipient.host(), Some("127.0.0.1"));
    assert_eq!(
        envelope.sender.as_ref().map(ActorRefWireData::path),
        Some("kairo://system/user/sender#10")
    );
}

#[test]
fn actor_ref_resolution_goes_through_provider_trait() {
    struct Resolver;

    impl ActorRefResolver for Resolver {
        type Ref = String;

        fn resolve_actor_ref(&self, wire: &ActorRefWireData) -> crate::Result<Self::Ref> {
            Ok(format!(
                "{}:{}:{}",
                wire.protocol(),
                wire.system(),
                wire.path()
            ))
        }
    }

    let wire = ActorRefWireData::new("kairo://system/user/counter#9").unwrap();

    assert_eq!(
        Resolver.resolve_actor_ref(&wire).unwrap(),
        "kairo:system:kairo://system/user/counter#9"
    );
}
