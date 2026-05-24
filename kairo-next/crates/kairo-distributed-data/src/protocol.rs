use bytes::Bytes;
use kairo_serialization::{ActorRefWireData, RemoteMessage};

use crate::{ReplicaId, SerializedCrdt};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorDeltaPropagation {
    pub from: ReplicaId,
    pub reply: bool,
    pub deltas: Vec<ReplicatorDelta>,
}

impl RemoteMessage for ReplicatorDeltaPropagation {
    const MANIFEST: &'static str = "kairo.ddata.delta-propagation";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorDelta {
    pub key: String,
    pub crdt_manifest: String,
    pub crdt_version: u16,
    pub payload: Bytes,
    pub from_version: u64,
    pub to_version: u64,
}

impl ReplicatorDelta {
    pub fn new(
        key: impl Into<String>,
        data: SerializedCrdt,
        from_version: u64,
        to_version: u64,
    ) -> Self {
        Self {
            key: key.into(),
            crdt_manifest: data.manifest().to_string(),
            crdt_version: data.version(),
            payload: data.into_payload(),
            from_version,
            to_version,
        }
    }

    pub fn serialized_crdt(&self) -> Option<SerializedCrdt> {
        Some(SerializedCrdt::new(
            stable_manifest(self.crdt_manifest.as_str())?,
            self.crdt_version,
            self.payload.clone(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicatorDeltaAck;

impl RemoteMessage for ReplicatorDeltaAck {
    const MANIFEST: &'static str = "kairo.ddata.delta-ack";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicatorDeltaNack;

impl RemoteMessage for ReplicatorDeltaNack {
    const MANIFEST: &'static str = "kairo.ddata.delta-nack";
    const VERSION: u16 = 1;
}

fn stable_manifest(manifest: &str) -> Option<&'static str> {
    match manifest {
        crate::GSET_STRING_MANIFEST => Some(crate::GSET_STRING_MANIFEST),
        crate::GCOUNTER_MANIFEST => Some(crate::GCOUNTER_MANIFEST),
        crate::PNCOUNTER_MANIFEST => Some(crate::PNCOUNTER_MANIFEST),
        _ => None,
    }
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
        assert_eq!(
            ReplicatorDeltaPropagation::MANIFEST,
            "kairo.ddata.delta-propagation"
        );
        assert_eq!(ReplicatorDeltaAck::MANIFEST, "kairo.ddata.delta-ack");
        assert_eq!(ReplicatorDeltaNack::MANIFEST, "kairo.ddata.delta-nack");
        assert!(!ReplicatorUpdate::MANIFEST.contains(std::any::type_name::<ReplicatorUpdate>()));
        assert!(
            !ReplicatorDeltaPropagation::MANIFEST
                .contains(std::any::type_name::<ReplicatorDeltaPropagation>())
        );
    }
}
