use kairo_actor::ActorSystem;
use kairo_serialization::ActorRefWireData;

use crate::RemoteSettings;

#[derive(Clone)]
pub(crate) struct CanonicalLocalAddress {
    protocol: String,
    system: String,
    host: String,
    port: u16,
}

impl CanonicalLocalAddress {
    pub(crate) fn from_system_settings(system: &ActorSystem, settings: RemoteSettings) -> Self {
        Self {
            protocol: system.address().protocol().to_string(),
            system: system.name().to_string(),
            host: settings.canonical_hostname,
            port: settings.canonical_port,
        }
    }

    pub(crate) fn local_recipient_path(&self, recipient: &ActorRefWireData) -> Option<String> {
        self.matches(recipient)
            .then(|| local_path_for_canonical_recipient(recipient))
    }

    pub(crate) fn canonical_recipient_path(&self, local_path: &str) -> Option<String> {
        let local_prefix = format!("{}://{}", self.protocol, self.system);
        let canonical_prefix = format!(
            "{}://{}@{}:{}",
            self.protocol, self.system, self.host, self.port
        );
        let suffix = local_path.strip_prefix(&local_prefix)?;
        if !suffix.starts_with('/') {
            return None;
        }
        Some(format!("{canonical_prefix}{suffix}"))
    }

    fn matches(&self, recipient: &ActorRefWireData) -> bool {
        recipient.protocol() == self.protocol
            && recipient.system() == self.system
            && recipient.host() == Some(self.host.as_str())
            && recipient.port() == Some(self.port)
    }
}

fn local_path_for_canonical_recipient(recipient: &ActorRefWireData) -> String {
    let remote_prefix = format!(
        "{}://{}@{}:{}",
        recipient.protocol(),
        recipient.system(),
        recipient.host().expect("canonical recipient has host"),
        recipient.port().expect("canonical recipient has port")
    );
    let local_prefix = format!("{}://{}", recipient.protocol(), recipient.system());
    recipient.path().replacen(&remote_prefix, &local_prefix, 1)
}
