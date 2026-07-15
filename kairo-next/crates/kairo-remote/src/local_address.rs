#![deny(missing_docs)]

use kairo_actor::ActorSystem;
use kairo_serialization::ActorRefWireData;

use crate::RemoteSettings;

/// Converts actor-ref paths between local-only and owned canonical remote
/// forms for one actor system.
///
/// This keeps Pekko-style `system@host:port` actor-ref wire metadata at remote
/// boundaries while preserving local registry resolution for refs owned by the
/// same actor system.
#[derive(Clone)]
pub struct CanonicalLocalAddress {
    protocol: String,
    system: String,
    host: String,
    port: u16,
}

impl CanonicalLocalAddress {
    /// Builds the canonical address from an actor system name/protocol and the
    /// remote host/port settings advertised for that system.
    pub fn from_system_settings(system: &ActorSystem, settings: RemoteSettings) -> Self {
        Self {
            protocol: system.address().protocol().to_string(),
            system: system.name().to_string(),
            host: settings.canonical_hostname,
            port: settings.canonical_port,
        }
    }

    /// Converts an owned canonical actor-ref recipient back to its local path.
    ///
    /// Foreign canonical addresses return `None`.
    pub fn local_recipient_path(&self, recipient: &ActorRefWireData) -> Option<String> {
        self.matches(recipient)
            .then(|| local_path_for_canonical_recipient(recipient))
    }

    /// Converts an owned local actor-ref path into canonical remote wire form.
    ///
    /// Paths from another actor system, including matching prefixes without a
    /// path segment boundary, return `None`.
    pub fn canonical_recipient_path(&self, local_path: &str) -> Option<String> {
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

#[cfg(test)]
mod tests {
    use kairo_actor::ActorSystem;

    use super::*;

    #[test]
    fn formats_owned_local_path_with_canonical_address() {
        let system = ActorSystem::builder("local-address-canonical-format")
            .build()
            .unwrap();
        let address = CanonicalLocalAddress::from_system_settings(
            &system,
            RemoteSettings::new("127.0.0.1", 25520),
        );

        assert_eq!(
            address
                .canonical_recipient_path("kairo://local-address-canonical-format/user/worker#12"),
            Some(
                "kairo://local-address-canonical-format@127.0.0.1:25520/user/worker#12".to_string()
            )
        );
    }

    #[test]
    fn rejects_local_path_prefix_without_segment_boundary() {
        let system = ActorSystem::builder("local-address-boundary")
            .build()
            .unwrap();
        let address = CanonicalLocalAddress::from_system_settings(
            &system,
            RemoteSettings::new("127.0.0.1", 25520),
        );

        assert_eq!(
            address.canonical_recipient_path("kairo://local-address-boundary-extra/user/worker"),
            None
        );
    }

    #[test]
    fn maps_owned_canonical_recipient_back_to_local_path() {
        let system = ActorSystem::builder("local-address-recipient")
            .build()
            .unwrap();
        let address = CanonicalLocalAddress::from_system_settings(
            &system,
            RemoteSettings::new("127.0.0.1", 25520),
        );
        let recipient =
            ActorRefWireData::new("kairo://local-address-recipient@127.0.0.1:25520/user/worker#12")
                .unwrap();

        assert_eq!(
            address.local_recipient_path(&recipient),
            Some("kairo://local-address-recipient/user/worker#12".to_string())
        );
    }

    #[test]
    fn leaves_foreign_canonical_recipient_unmapped() {
        let system = ActorSystem::builder("local-address-foreign")
            .build()
            .unwrap();
        let address = CanonicalLocalAddress::from_system_settings(
            &system,
            RemoteSettings::new("127.0.0.1", 25520),
        );
        let recipient =
            ActorRefWireData::new("kairo://local-address-foreign@127.0.0.2:25520/user/worker")
                .unwrap();

        assert_eq!(address.local_recipient_path(&recipient), None);
    }
}
