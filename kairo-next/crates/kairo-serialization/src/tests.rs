use bytes::Bytes;

use crate::{
    ActorRefResolver, ActorRefWireData, Manifest, MessageCodec, Registry, RemoteEnvelope,
    RemoteMessage, SerializationError, SerializationRegistry, SerializedMessage, WireReader,
    WireWriter,
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

#[derive(Debug, PartialEq, Eq)]
struct RollingCommand {
    amount: u8,
    tag: u8,
}

impl RemoteMessage for RollingCommand {
    const MANIFEST: &'static str = "kairo.test.RollingCommand";
    const VERSION: u16 = 2;
}

#[derive(Debug, Clone, Copy)]
struct SingleByteCodec {
    serializer_id: u32,
}

#[derive(Debug, Clone, Copy)]
struct PanickingCodec {
    serializer_id: u32,
    panic_on_encode: bool,
    panic_on_decode: bool,
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

impl MessageCodec<CounterCommand> for PanickingCodec {
    fn serializer_id(&self) -> u32 {
        self.serializer_id
    }

    fn encode(&self, message: &CounterCommand) -> crate::Result<Bytes> {
        if self.panic_on_encode {
            panic!("encode boom");
        }
        Ok(Bytes::from(vec![message.amount]))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> crate::Result<CounterCommand> {
        if self.panic_on_decode {
            panic!("decode boom");
        }
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

impl MessageCodec<RollingCommand> for SingleByteCodec {
    fn serializer_id(&self) -> u32 {
        self.serializer_id
    }

    fn encode(&self, message: &RollingCommand) -> crate::Result<Bytes> {
        Ok(Bytes::from(vec![message.amount, message.tag]))
    }

    fn decode(&self, payload: Bytes, version: u16) -> crate::Result<RollingCommand> {
        match version {
            1 => Ok(RollingCommand {
                amount: payload[0],
                tag: 0,
            }),
            2 => Ok(RollingCommand {
                amount: payload[0],
                tag: payload[1],
            }),
            other => Err(SerializationError::Message(format!(
                "unsupported RollingCommand version {other}"
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
fn registry_reports_missing_outbound_type_codec() {
    let registry = Registry::new();

    let error = registry
        .serialize(&CounterCommand { amount: 5 })
        .expect_err("unregistered outbound message type should fail");

    assert!(matches!(error, SerializationError::MissingTypeCodec(_)));
}

#[test]
fn registry_reports_missing_inbound_wire_codec_with_metadata() {
    let registry = Registry::new();

    let wire = SerializedMessage::new(
        41,
        Manifest::new("kairo.test.CounterCommand"),
        CounterCommand::VERSION,
        Bytes::from_static(&[5]),
    );
    let error = registry
        .deserialize_dyn(wire)
        .expect_err("unregistered inbound wire metadata should fail");

    assert_eq!(
        error,
        SerializationError::MissingWireCodec {
            serializer_id: 41,
            manifest: "kairo.test.CounterCommand".to_string(),
        }
    );
}

#[test]
fn registry_reports_codec_encode_panics_as_serialization_errors() {
    let mut registry = Registry::new();
    registry
        .register::<CounterCommand, _>(PanickingCodec {
            serializer_id: 41,
            panic_on_encode: true,
            panic_on_decode: false,
        })
        .unwrap();

    let error = registry
        .serialize(&CounterCommand { amount: 5 })
        .expect_err("codec panic should become a serialization error");

    assert_eq!(
        error,
        SerializationError::Message("codec encode panicked: encode boom".to_string())
    );
}

#[test]
fn registry_reports_codec_decode_panics_as_serialization_errors() {
    let mut registry = Registry::new();
    registry
        .register::<CounterCommand, _>(PanickingCodec {
            serializer_id: 41,
            panic_on_encode: false,
            panic_on_decode: true,
        })
        .unwrap();
    let wire = SerializedMessage::new(
        41,
        Manifest::new("kairo.test.CounterCommand"),
        CounterCommand::VERSION,
        Bytes::from_static(&[5]),
    );

    let error = registry
        .deserialize_dyn(wire)
        .expect_err("codec panic should become a serialization error");

    assert_eq!(
        error,
        SerializationError::Message("codec decode panicked: decode boom".to_string())
    );
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
fn registry_deserializes_wire_message_to_dynamic_boundary() {
    let mut registry = Registry::new();
    registry
        .register::<CounterCommand, _>(SingleByteCodec { serializer_id: 41 })
        .unwrap();

    let wire = SerializedMessage::new(
        41,
        Manifest::new("kairo.test.CounterCommand"),
        CounterCommand::VERSION,
        Bytes::from_static(&[9]),
    );
    let decoded = registry
        .deserialize_dyn(wire)
        .expect("wire message should decode dynamically");

    assert_eq!(
        *decoded
            .downcast::<CounterCommand>()
            .expect("dynamic value should be CounterCommand"),
        CounterCommand { amount: 9 }
    );
}

#[test]
fn typed_deserialize_rejects_unexpected_manifest_before_decoding() {
    let mut registry = Registry::new();
    registry
        .register::<CounterCommand, _>(SingleByteCodec { serializer_id: 41 })
        .unwrap();
    registry
        .register::<OtherCommand, _>(SingleByteCodec { serializer_id: 42 })
        .unwrap();

    let other = registry.serialize(&OtherCommand { amount: 3 }).unwrap();
    let error = registry
        .deserialize::<CounterCommand>(other)
        .expect_err("typed deserialize should reject the wrong manifest");

    assert_eq!(
        error,
        SerializationError::UnexpectedManifest {
            expected: CounterCommand::MANIFEST,
            actual: OtherCommand::MANIFEST.to_string(),
        }
    );
}

#[test]
fn dynamic_deserialize_receives_wire_version_for_rolling_compatibility() {
    let mut registry = Registry::new();
    registry
        .register::<RollingCommand, _>(SingleByteCodec { serializer_id: 51 })
        .unwrap();

    let old_wire = SerializedMessage::new(
        51,
        Manifest::new("kairo.test.RollingCommand"),
        1,
        Bytes::from_static(&[8]),
    );
    let decoded = registry
        .deserialize_dyn(old_wire)
        .expect("old wire message should decode dynamically");

    assert_eq!(
        *decoded
            .downcast::<RollingCommand>()
            .expect("dynamic value should be RollingCommand"),
        RollingCommand { amount: 8, tag: 0 }
    );
}

#[test]
fn rolling_decode_rejects_unsupported_wire_version() {
    let mut registry = Registry::new();
    registry
        .register::<RollingCommand, _>(SingleByteCodec { serializer_id: 51 })
        .unwrap();

    let typed_error = registry
        .deserialize::<RollingCommand>(SerializedMessage::new(
            51,
            Manifest::new("kairo.test.RollingCommand"),
            9,
            Bytes::from_static(&[8, 2]),
        ))
        .expect_err("typed decode should reject unsupported rolling versions");
    assert_eq!(
        typed_error,
        SerializationError::Message("unsupported RollingCommand version 9".to_string())
    );

    let dynamic_error = registry
        .deserialize_dyn(SerializedMessage::new(
            51,
            Manifest::new("kairo.test.RollingCommand"),
            9,
            Bytes::from_static(&[8, 2]),
        ))
        .expect_err("dynamic decode should reject unsupported rolling versions");
    assert_eq!(
        dynamic_error,
        SerializationError::Message("unsupported RollingCommand version 9".to_string())
    );
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
fn decode_receives_wire_version_for_rolling_compatibility() {
    let mut registry = Registry::new();
    registry
        .register::<RollingCommand, _>(SingleByteCodec { serializer_id: 51 })
        .unwrap();

    let current = registry
        .serialize(&RollingCommand { amount: 8, tag: 2 })
        .unwrap();
    assert_eq!(current.version, 2);
    assert_eq!(
        registry.deserialize::<RollingCommand>(current).unwrap(),
        RollingCommand { amount: 8, tag: 2 }
    );

    let old_wire = SerializedMessage::new(
        51,
        Manifest::new("kairo.test.RollingCommand"),
        1,
        Bytes::from_static(&[8]),
    );
    assert_eq!(
        registry.deserialize::<RollingCommand>(old_wire).unwrap(),
        RollingCommand { amount: 8, tag: 0 }
    );
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
fn actor_ref_wire_data_rejects_addressed_paths_without_ports() {
    let error = ActorRefWireData::new("kairo://system@127.0.0.1/user/counter#9")
        .expect_err("addressed remote path without port should be invalid");

    assert_eq!(
        error,
        SerializationError::InvalidActorRefPath(
            "kairo://system@127.0.0.1/user/counter#9".to_string()
        )
    );
}

#[test]
fn actor_ref_wire_data_rejects_mismatched_host_and_port_parts() {
    let missing_port = ActorRefWireData::from_parts(
        "kairo",
        "system",
        Some("127.0.0.1".to_string()),
        None,
        "kairo://system@127.0.0.1/user/counter#9",
    )
    .expect_err("host without port should be invalid");
    assert_eq!(
        missing_port,
        SerializationError::InvalidActorRefPath(
            "kairo://system@127.0.0.1/user/counter#9".to_string()
        )
    );

    let missing_host = ActorRefWireData::from_parts(
        "kairo",
        "system",
        None,
        Some(25520),
        "kairo://system/user/counter#9",
    )
    .expect_err("port without host should be invalid");
    assert_eq!(
        missing_host,
        SerializationError::InvalidActorRefPath("kairo://system/user/counter#9".to_string())
    );
}

#[test]
fn actor_ref_wire_data_from_parts_requires_matching_path_metadata() {
    let addressed = ActorRefWireData::from_parts(
        "kairo",
        "system",
        Some("127.0.0.1".to_string()),
        Some(25520),
        "kairo://system@127.0.0.1:25520/user/counter#9",
    )
    .expect("matching addressed path parts should build");
    assert_eq!(addressed.system(), "system");
    assert_eq!(addressed.host(), Some("127.0.0.1"));
    assert_eq!(addressed.port(), Some(25520));

    let local = ActorRefWireData::from_parts(
        "kairo",
        "system",
        None,
        None,
        "kairo://system/user/counter#9",
    )
    .expect("matching local path parts should build");
    assert_eq!(local.system(), "system");
    assert_eq!(local.host(), None);
    assert_eq!(local.port(), None);

    for (protocol, system, host, port, path) in [
        (
            "other",
            "system",
            Some("127.0.0.1".to_string()),
            Some(25520),
            "kairo://system@127.0.0.1:25520/user/counter#9",
        ),
        (
            "kairo",
            "other-system",
            Some("127.0.0.1".to_string()),
            Some(25520),
            "kairo://system@127.0.0.1:25520/user/counter#9",
        ),
        (
            "kairo",
            "system",
            Some("10.0.0.1".to_string()),
            Some(25520),
            "kairo://system@127.0.0.1:25520/user/counter#9",
        ),
        (
            "kairo",
            "system",
            Some("127.0.0.1".to_string()),
            Some(25521),
            "kairo://system@127.0.0.1:25520/user/counter#9",
        ),
        (
            "kairo",
            "system",
            Some("127.0.0.1".to_string()),
            Some(25520),
            "kairo://system/user/counter#9",
        ),
    ] {
        let error = ActorRefWireData::from_parts(protocol, system, host, port, path)
            .expect_err("mismatched path metadata should fail");
        assert_eq!(
            error,
            SerializationError::InvalidActorRefPath(path.to_string())
        );
    }
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
fn serialized_message_wire_round_trip_preserves_metadata_tuple() {
    let message = SerializedMessage::new(
        0x0102_0304,
        Manifest::new("kairo.test.CounterCommand"),
        0x1122,
        Bytes::from_static(&[0xaa, 0xbb]),
    );

    let bytes = message.encode_wire().unwrap();
    assert_eq!(
        bytes.as_ref(),
        &[
            1, 2, 3, 4, 0, 0, 0, 25, b'k', b'a', b'i', b'r', b'o', b'.', b't', b'e', b's', b't',
            b'.', b'C', b'o', b'u', b'n', b't', b'e', b'r', b'C', b'o', b'm', b'm', b'a', b'n',
            b'd', 0x11, 0x22, 0, 0, 0, 2, 0xaa, 0xbb,
        ]
    );

    let decoded = SerializedMessage::decode_wire(&bytes).unwrap();

    assert_eq!(decoded, message);
}

#[test]
fn serialized_message_wire_decode_rejects_invalid_manifest_and_trailing_bytes() {
    let mut writer = WireWriter::new();
    writer.write_u32(41);
    writer.write_string(" ").unwrap();
    writer.write_u16(1);
    writer.write_bytes(&Bytes::new()).unwrap();
    let bytes = writer.finish();

    assert_eq!(
        SerializedMessage::decode_wire(&bytes).unwrap_err(),
        SerializationError::InvalidManifest(" ".to_string())
    );

    let mut valid = SerializedMessage::new(
        41,
        Manifest::new("kairo.test.CounterCommand"),
        1,
        Bytes::new(),
    )
    .encode_wire()
    .unwrap()
    .to_vec();
    valid.push(0xff);
    let valid = Bytes::from(valid);

    assert_eq!(
        SerializedMessage::decode_wire(&valid).unwrap_err(),
        SerializationError::Message("wire payload has 1 trailing byte(s)".to_string())
    );
}

#[test]
fn remote_envelope_wire_round_trip_preserves_refs_and_message() {
    let message = SerializedMessage::new(
        7,
        Manifest::new("kairo.test.CounterCommand"),
        3,
        Bytes::from_static(&[1, 2, 3]),
    );
    let envelope = RemoteEnvelope::from_paths(
        "kairo://system@127.0.0.1:25520/user/counter#9",
        Some("kairo://system/user/sender#10".to_string()),
        message,
    )
    .unwrap();

    let bytes = envelope.encode_wire().unwrap();
    let decoded = RemoteEnvelope::decode_wire(&bytes).unwrap();

    assert_eq!(decoded, envelope);
    assert_eq!(decoded.recipient.protocol(), "kairo");
    assert_eq!(decoded.recipient.system(), "system");
    assert_eq!(decoded.recipient.host(), Some("127.0.0.1"));
    assert_eq!(decoded.recipient.port(), Some(25520));
    assert_eq!(
        decoded.sender.as_ref().map(ActorRefWireData::path),
        Some("kairo://system/user/sender#10")
    );
    assert_eq!(
        decoded.message.manifest.as_str(),
        "kairo.test.CounterCommand"
    );
}

#[test]
fn remote_envelope_wire_decode_rejects_invalid_actor_ref_path() {
    let message = SerializedMessage::new(
        7,
        Manifest::new("kairo.test.CounterCommand"),
        3,
        Bytes::from_static(&[1, 2, 3]),
    );
    let mut writer = WireWriter::new();
    writer.write_string("/user/counter").unwrap();
    writer.write_optional_string(None).unwrap();
    message.write_wire(&mut writer).unwrap();
    let bytes = writer.finish();

    assert_eq!(
        RemoteEnvelope::decode_wire(&bytes).unwrap_err(),
        SerializationError::InvalidActorRefPath("/user/counter".to_string())
    );
}

#[test]
fn remote_envelope_wire_decode_rejects_invalid_sender_actor_ref_path() {
    let message = SerializedMessage::new(
        7,
        Manifest::new("kairo.test.CounterCommand"),
        3,
        Bytes::from_static(&[1, 2, 3]),
    );
    let mut writer = WireWriter::new();
    writer
        .write_string("kairo://system@127.0.0.1:25520/user/counter#9")
        .unwrap();
    writer
        .write_optional_string(Some("/user/not-a-canonical-sender"))
        .unwrap();
    message.write_wire(&mut writer).unwrap();
    let bytes = writer.finish();

    assert_eq!(
        RemoteEnvelope::decode_wire(&bytes).unwrap_err(),
        SerializationError::InvalidActorRefPath("/user/not-a-canonical-sender".to_string())
    );
}

#[test]
fn actor_ref_resolution_goes_through_provider_trait() {
    struct Resolver;

    impl ActorRefResolver for Resolver {
        type Ref = String;

        fn actor_ref_to_wire_data(&self, actor_ref: &Self::Ref) -> crate::Result<ActorRefWireData> {
            ActorRefWireData::new(actor_ref)
        }

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
    let formatted = Resolver
        .actor_ref_to_wire_data(&"kairo://system/user/worker#10".to_string())
        .unwrap();

    assert_eq!(
        Resolver.resolve_actor_ref(&wire).unwrap(),
        "kairo:system:kairo://system/user/counter#9"
    );
    assert_eq!(formatted.path(), "kairo://system/user/worker#10");
}

#[test]
fn wire_helpers_use_length_prefixed_strings_and_big_endian_numbers() {
    let mut writer = WireWriter::new();
    writer.write_string("abc").unwrap();
    writer
        .write_bytes(&Bytes::from_static(&[0xaa, 0xbb, 0xcc]))
        .unwrap();
    writer.write_optional_string(Some("host")).unwrap();
    writer.write_optional_string(None).unwrap();
    writer.write_bool(true);
    writer.write_bool(false);
    writer.write_u16(0x1122);
    writer.write_u64(0x0102_0304_0506_0708);
    writer.write_u128(0x0102_0304_0506_0708_1112_1314_1516_1718);
    writer.write_optional_u64(Some(9));
    writer.write_optional_u64(None);
    let bytes = writer.finish();

    assert_eq!(
        bytes.as_ref(),
        &[
            0, 0, 0, 3, b'a', b'b', b'c', 0, 0, 0, 3, 0xaa, 0xbb, 0xcc, 1, 0, 0, 0, 4, b'h', b'o',
            b's', b't', 0, 1, 0, 0x11, 0x22, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 17,
            18, 19, 20, 21, 22, 23, 24, 1, 0, 0, 0, 0, 0, 0, 0, 9, 0,
        ]
    );

    let mut reader = WireReader::new(&bytes);
    assert_eq!(reader.read_string().unwrap(), "abc");
    assert_eq!(
        reader.read_bytes().unwrap(),
        Bytes::from_static(&[0xaa, 0xbb, 0xcc])
    );
    assert_eq!(
        reader.read_optional_string().unwrap(),
        Some("host".to_string())
    );
    assert_eq!(reader.read_optional_string().unwrap(), None);
    assert!(reader.read_bool().unwrap());
    assert!(!reader.read_bool().unwrap());
    assert_eq!(reader.read_u16().unwrap(), 0x1122);
    assert_eq!(reader.read_u64().unwrap(), 0x0102_0304_0506_0708);
    assert_eq!(
        reader.read_u128().unwrap(),
        0x0102_0304_0506_0708_1112_1314_1516_1718
    );
    assert_eq!(reader.read_optional_u64().unwrap(), Some(9));
    assert_eq!(reader.read_optional_u64().unwrap(), None);
    assert_eq!(reader.remaining_len(), 0);
    assert!(reader.is_finished());
    reader.ensure_finished().unwrap();
}

#[test]
fn wire_reader_reports_unread_trailing_bytes() {
    let bytes = Bytes::from_static(&[0x11, 0x22, 0x33]);
    let mut reader = WireReader::new(&bytes);

    assert_eq!(reader.remaining_len(), 3);
    assert!(!reader.is_finished());
    assert_eq!(reader.read_u16().unwrap(), 0x1122);
    assert_eq!(reader.remaining_len(), 1);
    assert!(!reader.is_finished());

    let error = reader
        .ensure_finished()
        .expect_err("trailing payload byte should be rejected");

    assert_eq!(
        error,
        SerializationError::Message("wire payload has 1 trailing byte(s)".to_string())
    );
}

#[test]
fn wire_reader_rejects_invalid_presence_and_bool_markers() {
    let bytes = Bytes::from_static(&[2, 3, 4]);
    let mut reader = WireReader::new(&bytes);

    assert_eq!(
        reader.read_bool().unwrap_err(),
        SerializationError::Message("invalid bool marker 2".to_string())
    );
    assert_eq!(
        reader.read_optional_string().unwrap_err(),
        SerializationError::Message("invalid optional string marker 3".to_string())
    );
    assert_eq!(
        reader.read_optional_u64().unwrap_err(),
        SerializationError::Message("invalid optional u64 marker 4".to_string())
    );
    assert!(reader.is_finished());
}

#[test]
fn wire_reader_rejects_early_eof_and_invalid_utf8() {
    let short_string = Bytes::from_static(&[0, 0, 0, 3, b'a']);
    let mut short_string_reader = WireReader::new(&short_string);
    assert_eq!(
        short_string_reader.read_string().unwrap_err(),
        SerializationError::Message("wire payload ended early".to_string())
    );

    let invalid_utf8 = Bytes::from_static(&[0, 0, 0, 1, 0xff]);
    let mut invalid_utf8_reader = WireReader::new(&invalid_utf8);
    let error = invalid_utf8_reader
        .read_string()
        .expect_err("invalid utf-8 string should fail");
    assert!(
        matches!(error, SerializationError::Message(message) if message.starts_with("wire string is not utf-8:"))
    );

    let short_u64 = Bytes::from_static(&[0, 0, 0, 0]);
    let mut short_u64_reader = WireReader::new(&short_u64);
    assert_eq!(
        short_u64_reader.read_u64().unwrap_err(),
        SerializationError::Message("wire payload ended early".to_string())
    );
}
