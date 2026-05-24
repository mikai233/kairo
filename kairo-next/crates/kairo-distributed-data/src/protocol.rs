use kairo_serialization::{ActorRefWireData, RemoteMessage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGet {
    pub key: String,
    pub request_id: u64,
}

impl RemoteMessage for ReplicatorGet {
    const MANIFEST: &'static str = "kairo.ddata.replicator-get";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorUpdate {
    pub key: String,
    pub request_id: u64,
}

impl RemoteMessage for ReplicatorUpdate {
    const MANIFEST: &'static str = "kairo.ddata.replicator-update";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorSubscribe {
    pub key: String,
    pub subscriber: ActorRefWireData,
}

impl RemoteMessage for ReplicatorSubscribe {
    const MANIFEST: &'static str = "kairo.ddata.replicator-subscribe";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorChanged {
    pub key: String,
}

impl RemoteMessage for ReplicatorChanged {
    const MANIFEST: &'static str = "kairo.ddata.replicator-changed";
    const VERSION: u16 = 1;
}

#[cfg(test)]
mod tests {
    use kairo_serialization::RemoteMessage;

    use super::*;

    #[test]
    fn distributed_data_system_manifests_are_stable() {
        assert_eq!(ReplicatorGet::MANIFEST, "kairo.ddata.replicator-get");
        assert_eq!(
            ReplicatorSubscribe::MANIFEST,
            "kairo.ddata.replicator-subscribe"
        );
        assert_eq!(ReplicatorChanged::VERSION, 1);
        assert!(!ReplicatorUpdate::MANIFEST.contains(std::any::type_name::<ReplicatorUpdate>()));
    }
}
