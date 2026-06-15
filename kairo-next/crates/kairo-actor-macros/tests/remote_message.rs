use kairo_actor_macros::{KairoRemoteMessage, kairo_message};
use kairo_serialization::RemoteMessage;
use std::path::Path;

#[derive(KairoRemoteMessage)]
#[kairo(manifest = "kairo.test.MacroMessage", version = 12)]
struct MacroMessage;

#[derive(KairoRemoteMessage)]
#[kairo(manifest = "kairo.test.SplitAttributeMessage")]
#[kairo(version = 3)]
struct SplitAttributeMessage;

#[derive(KairoRemoteMessage)]
#[kairo(manifest = "kairo.test.EnumMessage", version = 4)]
enum EnumMessage {
    Started,
    Stopped,
}

#[derive(KairoRemoteMessage)]
#[kairo(manifest = "wire.contract.not.rust.name", version = 1)]
struct RenamedRustType;

#[kairo_message]
#[derive(Debug, PartialEq, Eq)]
struct LocalOnlyMessage {
    value: u8,
}

#[test]
fn derive_remote_message_emits_stable_metadata() {
    assert_eq!(MacroMessage::MANIFEST, "kairo.test.MacroMessage");
    assert_eq!(MacroMessage::VERSION, 12);
}

#[test]
fn derive_remote_message_accepts_split_metadata_attributes() {
    assert_eq!(
        SplitAttributeMessage::MANIFEST,
        "kairo.test.SplitAttributeMessage"
    );
    assert_eq!(SplitAttributeMessage::VERSION, 3);
}

#[test]
fn derive_remote_message_emits_metadata_for_enums_only() {
    assert_eq!(EnumMessage::MANIFEST, "kairo.test.EnumMessage");
    assert_eq!(EnumMessage::VERSION, 4);

    let started = EnumMessage::Started;
    let stopped = EnumMessage::Stopped;
    assert!(matches!(started, EnumMessage::Started));
    assert!(matches!(stopped, EnumMessage::Stopped));
}

#[test]
fn derive_remote_message_does_not_infer_manifest_from_rust_type_name() {
    assert_eq!(RenamedRustType::MANIFEST, "wire.contract.not.rust.name");
    assert_eq!(RenamedRustType::VERSION, 1);
    assert!(
        !RenamedRustType::MANIFEST.contains(std::any::type_name::<RenamedRustType>()),
        "wire manifest must be explicit metadata, not the Rust type name"
    );
    assert!(
        !RenamedRustType::MANIFEST.contains("RenamedRustType"),
        "wire manifest must stay stable if the Rust type is renamed"
    );
}

#[test]
fn kairo_message_marker_leaves_local_message_item_unchanged() {
    assert_eq!(LocalOnlyMessage { value: 7 }, LocalOnlyMessage { value: 7 });
}

#[test]
fn derive_remote_message_does_not_choose_codec_or_format() {
    let macro_source =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"))
            .expect("macro source should be readable");
    let code_only = uncommented_source(&macro_source);

    let forbidden_terms = [
        "MessageCodec",
        "DynCodec",
        "SerializationRegistry",
        "SerializedMessage",
        "serializer_id",
        "register_",
        "serde",
        "bincode",
        "prost",
        "postcard",
        "ciborium",
        "rmp_serde",
        "type_name",
        "stringify!",
        "ident.to_string",
    ];

    for term in forbidden_terms {
        assert!(
            !code_only.contains(term),
            "KairoRemoteMessage derive code must not choose codec or format term `{term}`"
        );
    }
}

fn uncommented_source(source: &str) -> String {
    source
        .lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            !trimmed.starts_with("//")
        })
        .collect::<Vec<_>>()
        .join("\n")
}
