use kairo_serialization::{ActorRefWireData, RemoteMessage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchRemote {
    pub watchee: ActorRefWireData,
    pub watcher: ActorRefWireData,
}

impl RemoteMessage for WatchRemote {
    const MANIFEST: &'static str = "kairo.remote.watch-remote";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnwatchRemote {
    pub watchee: ActorRefWireData,
    pub watcher: ActorRefWireData,
}

impl RemoteMessage for UnwatchRemote {
    const MANIFEST: &'static str = "kairo.remote.unwatch-remote";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteHeartbeat {
    pub from_uid: u64,
}

impl RemoteMessage for RemoteHeartbeat {
    const MANIFEST: &'static str = "kairo.remote.heartbeat";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteHeartbeatAck {
    pub uid: u64,
}

impl RemoteMessage for RemoteHeartbeatAck {
    const MANIFEST: &'static str = "kairo.remote.heartbeat-ack";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressTerminated {
    pub address: String,
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
        assert_eq!(RemoteHeartbeat::MANIFEST, "kairo.remote.heartbeat");
        assert_eq!(RemoteHeartbeatAck::MANIFEST, "kairo.remote.heartbeat-ack");
        assert_eq!(
            AddressTerminated::MANIFEST,
            "kairo.remote.address-terminated"
        );
        assert!(!WatchRemote::MANIFEST.contains(std::any::type_name::<WatchRemote>()));
    }
}
