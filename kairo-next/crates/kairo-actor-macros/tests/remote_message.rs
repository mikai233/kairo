use kairo_actor_macros::{KairoRemoteMessage, kairo_message};
use kairo_serialization::RemoteMessage;

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
fn kairo_message_marker_leaves_local_message_item_unchanged() {
    assert_eq!(LocalOnlyMessage { value: 7 }, LocalOnlyMessage { value: 7 });
}
