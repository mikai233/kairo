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
pub struct ReplicatorWrite {
    pub key: String,
    pub from: Option<ReplicaId>,
    pub envelope: ReplicatorDataEnvelope,
}

impl RemoteMessage for ReplicatorWrite {
    const MANIFEST: &'static str = "kairo.ddata.write";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicatorWriteAck;

impl RemoteMessage for ReplicatorWriteAck {
    const MANIFEST: &'static str = "kairo.ddata.write-ack";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicatorWriteNack;

impl RemoteMessage for ReplicatorWriteNack {
    const MANIFEST: &'static str = "kairo.ddata.write-nack";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRead {
    pub key: String,
    pub from: Option<ReplicaId>,
}

impl RemoteMessage for ReplicatorRead {
    const MANIFEST: &'static str = "kairo.ddata.read";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorReadResult {
    pub envelope: Option<ReplicatorDataEnvelope>,
}

impl RemoteMessage for ReplicatorReadResult {
    const MANIFEST: &'static str = "kairo.ddata.read-result";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorDataEnvelope {
    pub crdt_manifest: String,
    pub crdt_version: u16,
    pub payload: Bytes,
}

impl ReplicatorDataEnvelope {
    pub fn new(data: SerializedCrdt) -> Self {
        Self {
            crdt_manifest: data.manifest().to_string(),
            crdt_version: data.version(),
            payload: data.into_payload(),
        }
    }
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
        assert_eq!(ReplicatorWrite::MANIFEST, "kairo.ddata.write");
        assert_eq!(ReplicatorWriteAck::MANIFEST, "kairo.ddata.write-ack");
        assert_eq!(ReplicatorWriteNack::MANIFEST, "kairo.ddata.write-nack");
        assert_eq!(ReplicatorRead::MANIFEST, "kairo.ddata.read");
        assert_eq!(ReplicatorReadResult::MANIFEST, "kairo.ddata.read-result");
        assert_eq!(ReplicatorDeltaAck::MANIFEST, "kairo.ddata.delta-ack");
        assert_eq!(ReplicatorDeltaNack::MANIFEST, "kairo.ddata.delta-nack");
        assert!(!ReplicatorUpdate::MANIFEST.contains(std::any::type_name::<ReplicatorUpdate>()));
        assert!(!ReplicatorWrite::MANIFEST.contains(std::any::type_name::<ReplicatorWrite>()));
        assert!(
            !ReplicatorDeltaPropagation::MANIFEST
                .contains(std::any::type_name::<ReplicatorDeltaPropagation>())
        );
    }
}
