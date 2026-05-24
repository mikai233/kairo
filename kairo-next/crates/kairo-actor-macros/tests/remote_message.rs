use kairo_actor_macros::KairoRemoteMessage;
use kairo_serialization::RemoteMessage;

#[derive(KairoRemoteMessage)]
#[kairo(manifest = "kairo.test.MacroMessage", version = 12)]
struct MacroMessage;

#[test]
fn derive_remote_message_emits_stable_metadata() {
    assert_eq!(MacroMessage::MANIFEST, "kairo.test.MacroMessage");
    assert_eq!(MacroMessage::VERSION, 12);
}
