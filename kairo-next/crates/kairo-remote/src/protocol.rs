#![deny(missing_docs)]

use kairo_serialization::{ActorRefWireData, RemoteMessage};

/// Requests remote death watch for one actor on behalf of a watcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchRemote {
    /// The actor whose termination should be observed.
    pub watchee: ActorRefWireData,
    /// The actor that should receive the termination notification.
    pub watcher: ActorRefWireData,
}

impl RemoteMessage for WatchRemote {
    const MANIFEST: &'static str = "kairo.remote.watch-remote";
    const VERSION: u16 = 1;
}

/// Cancels a previously requested remote death watch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnwatchRemote {
    /// The actor that was being observed.
    pub watchee: ActorRefWireData,
    /// The actor that no longer wants termination notifications.
    pub watcher: ActorRefWireData,
}

impl RemoteMessage for UnwatchRemote {
    const MANIFEST: &'static str = "kairo.remote.unwatch-remote";
    const VERSION: u16 = 1;
}

/// Reports that a remotely watched actor has terminated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTerminated {
    /// The actor reported as terminated.
    pub watchee: ActorRefWireData,
    /// Whether the remote system confirmed that the actor previously existed.
    pub existence_confirmed: bool,
}

impl RemoteMessage for RemoteTerminated {
    const MANIFEST: &'static str = "kairo.remote.terminated";
    const VERSION: u16 = 1;
}

/// Probes liveness of a remote actor system for death-watch purposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteHeartbeat {
    /// Unique incarnation identifier of the sending actor system.
    pub from_uid: u64,
}

impl RemoteMessage for RemoteHeartbeat {
    const MANIFEST: &'static str = "kairo.remote.heartbeat";
    const VERSION: u16 = 1;
}

/// Acknowledges a remote death-watch heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteHeartbeatAck {
    /// Unique incarnation identifier of the acknowledging actor system.
    pub uid: u64,
}

impl RemoteMessage for RemoteHeartbeatAck {
    const MANIFEST: &'static str = "kairo.remote.heartbeat-ack";
    const VERSION: u16 = 1;
}

/// Reports that one remote actor-system incarnation is unavailable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressTerminated {
    /// Canonical remote actor-system address.
    pub address: String,
    /// Incarnation identifier, when the terminated incarnation is known.
    pub uid: Option<u64>,
}

impl RemoteMessage for AddressTerminated {
    const MANIFEST: &'static str = "kairo.remote.address-terminated";
    const VERSION: u16 = 1;
}

#[cfg(test)]
mod tests {
    use kairo_serialization::RemoteMessage;

    use super::*;

    #[test]
    fn remote_system_manifests_are_stable() {
        assert_eq!(WatchRemote::MANIFEST, "kairo.remote.watch-remote");
        assert_eq!(UnwatchRemote::MANIFEST, "kairo.remote.unwatch-remote");
        assert_eq!(RemoteTerminated::MANIFEST, "kairo.remote.terminated");
        assert_eq!(RemoteHeartbeat::MANIFEST, "kairo.remote.heartbeat");
        assert_eq!(RemoteHeartbeatAck::MANIFEST, "kairo.remote.heartbeat-ack");
        assert_eq!(
            AddressTerminated::MANIFEST,
            "kairo.remote.address-terminated"
        );
        assert!(!WatchRemote::MANIFEST.contains(std::any::type_name::<WatchRemote>()));
        assert!(!RemoteTerminated::MANIFEST.contains(std::any::type_name::<RemoteTerminated>()));
    }
}
