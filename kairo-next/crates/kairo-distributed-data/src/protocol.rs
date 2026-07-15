#![deny(missing_docs)]

//! Stable distributed-data wire records and remote-message metadata.
//!
//! These records are the codec boundary used by remoting and system actors;
//! they are not the primary typed replicator API. Their manifests, versions,
//! codec-defined field order, and codec-owned bytes form the wire contract and
//! never depend on Rust type names, enum discriminants, or memory layout.

use bytes::Bytes;
use kairo_serialization::{ActorRefWireData, RemoteMessage};

use crate::{ReplicaId, SerializedCrdt};

/// Low-level correlated request to read a replicated key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGet {
    /// Stable typed-namespace key string.
    pub key: String,
    /// Caller-owned correlation identifier echoed by the integration layer.
    pub request_id: u64,
}

impl RemoteMessage for ReplicatorGet {
    const MANIFEST: &'static str = "kairo.ddata.replicator-get";
    const VERSION: u16 = 1;
}

/// Low-level correlated request to update a replicated key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorUpdate {
    /// Stable typed-namespace key string.
    pub key: String,
    /// Caller-owned correlation identifier echoed by the integration layer.
    pub request_id: u64,
}

impl RemoteMessage for ReplicatorUpdate {
    const MANIFEST: &'static str = "kairo.ddata.replicator-update";
    const VERSION: u16 = 1;
}

/// Low-level request to subscribe an actor reference to a replicated key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorSubscribe {
    /// Stable typed-namespace key string.
    pub key: String,
    /// Serialized subscriber actor reference.
    pub subscriber: ActorRefWireData,
}

impl RemoteMessage for ReplicatorSubscribe {
    const MANIFEST: &'static str = "kairo.ddata.replicator-subscribe";
    const VERSION: u16 = 1;
}

/// Low-level notification that a replicated key has changed.
///
/// The owning integration resolves the typed current value; this wire record
/// carries stable key metadata only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorChanged {
    /// Stable typed-namespace key string.
    pub key: String,
}

impl RemoteMessage for ReplicatorChanged {
    const MANIFEST: &'static str = "kairo.ddata.replicator-changed";
    const VERSION: u16 = 1;
}

/// Direct full-state write sent to a peer replicator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorWrite {
    /// Stable typed-namespace key string.
    pub key: String,
    /// Optional source replica used for request/reply routing and diagnostics.
    pub from: Option<ReplicaId>,
    /// Serialized CRDT value and removed-replica pruning metadata.
    pub envelope: ReplicatorDataEnvelope,
}

impl RemoteMessage for ReplicatorWrite {
    const MANIFEST: &'static str = "kairo.ddata.write";
    const VERSION: u16 = 2;
}

/// Successful acknowledgement of a direct full-state write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicatorWriteAck;

impl RemoteMessage for ReplicatorWriteAck {
    const MANIFEST: &'static str = "kairo.ddata.write-ack";
    const VERSION: u16 = 1;
}

/// Rejection of a direct full-state write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicatorWriteNack;

impl RemoteMessage for ReplicatorWriteNack {
    const MANIFEST: &'static str = "kairo.ddata.write-nack";
    const VERSION: u16 = 1;
}

/// Direct request for a peer's current full-state envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRead {
    /// Stable typed-namespace key string.
    pub key: String,
    /// Optional source replica used for request/reply routing and diagnostics.
    pub from: Option<ReplicaId>,
}

impl RemoteMessage for ReplicatorRead {
    const MANIFEST: &'static str = "kairo.ddata.read";
    const VERSION: u16 = 1;
}

/// Direct read reply containing a current envelope or successful absence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorReadResult {
    /// Current full-state envelope, or `None` when the key is absent.
    pub envelope: Option<ReplicatorDataEnvelope>,
}

impl RemoteMessage for ReplicatorReadResult {
    const MANIFEST: &'static str = "kairo.ddata.read-result";
    const VERSION: u16 = 2;
}

/// Digest summary for one key in a gossip status chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGossipDigest {
    /// Stable typed-namespace key string.
    pub key: String,
    /// Stable hash of the serialized data envelope.
    pub digest: u64,
    /// Last-use wall-clock timestamp carried for expiry coordination.
    pub used_timestamp_millis: u64,
}

/// Chunked digest status used to negotiate full-state gossip differences.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGossipStatus {
    /// Digest entries assigned to this chunk.
    pub entries: Vec<ReplicatorGossipDigest>,
    /// Zero-based chunk index.
    pub chunk: u32,
    /// Total number of chunks in the status cycle.
    pub total_chunks: u32,
    /// Optional destination ActorSystem incarnation UID.
    pub to_system_uid: Option<u64>,
    /// Optional source ActorSystem incarnation UID.
    pub from_system_uid: Option<u64>,
}

impl RemoteMessage for ReplicatorGossipStatus {
    const MANIFEST: &'static str = "kairo.ddata.gossip-status";
    const VERSION: u16 = 1;
}

/// One full-state key entry carried by gossip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGossipEntry {
    /// Stable typed-namespace key string.
    pub key: String,
    /// Serialized CRDT value and removed-replica pruning metadata.
    pub envelope: ReplicatorDataEnvelope,
    /// Last-use wall-clock timestamp carried for expiry coordination.
    pub used_timestamp_millis: u64,
}

/// Full-state gossip payload exchanged after status comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGossip {
    /// Full-state entries selected for the peer.
    pub entries: Vec<ReplicatorGossipEntry>,
    /// Whether the receiver should return its corresponding differing entries.
    pub send_back: bool,
    /// Optional destination ActorSystem incarnation UID.
    pub to_system_uid: Option<u64>,
    /// Optional source ActorSystem incarnation UID.
    pub from_system_uid: Option<u64>,
}

impl RemoteMessage for ReplicatorGossip {
    const MANIFEST: &'static str = "kairo.ddata.gossip";
    const VERSION: u16 = 1;
}

/// Serialized CRDT value and its removed-replica pruning state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorDataEnvelope {
    /// Stable CRDT codec manifest.
    pub crdt_manifest: String,
    /// CRDT codec schema version.
    pub crdt_version: u16,
    /// Codec-owned CRDT payload bytes.
    pub payload: Bytes,
    /// Pruning entries associated with the value.
    pub pruning: Vec<ReplicatorPruningEntry>,
}

impl ReplicatorDataEnvelope {
    /// Creates an envelope from serialized CRDT data with no pruning entries.
    pub fn new(data: SerializedCrdt) -> Self {
        Self {
            crdt_manifest: data.manifest().to_string(),
            crdt_version: data.version(),
            payload: data.into_payload(),
            pruning: Vec::new(),
        }
    }

    /// Replaces the envelope's pruning entries.
    pub fn with_pruning(mut self, pruning: Vec<ReplicatorPruningEntry>) -> Self {
        self.pruning = pruning;
        self
    }
}

/// Wire pruning state for one removed replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorPruningEntry {
    /// Replica whose CRDT contribution is being or has been pruned.
    pub removed: ReplicaId,
    /// Current pruning lifecycle state.
    pub state: ReplicatorPruningState,
}

/// Stable tagged representation of removed-replica pruning state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicatorPruningState {
    /// Pruning is owned and waiting for dissemination acknowledgements.
    Initialized {
        /// Replica responsible for performing the prune.
        owner: ReplicaId,
        /// Replicas that have observed initialization.
        ///
        /// Canonical encoders emit deterministic replica order.
        seen: Vec<ReplicaId>,
    },
    /// Pruning completed and suppresses late removed-replica data until expiry.
    Performed {
        /// Inclusive obsolete deadline in Unix-epoch milliseconds.
        obsolete_at_millis: u64,
    },
}

/// Versioned delta ranges sent from one replica to a peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorDeltaPropagation {
    /// Source replica whose per-key versions the ranges belong to.
    pub from: ReplicaId,
    /// Whether the receiver must reply with delta acknowledgement or rejection.
    pub reply: bool,
    /// Per-key delta ranges in deterministic key order when canonically encoded.
    pub deltas: Vec<ReplicatorDelta>,
}

impl RemoteMessage for ReplicatorDeltaPropagation {
    const MANIFEST: &'static str = "kairo.ddata.delta-propagation";
    const VERSION: u16 = 1;
}

/// Serialized CRDT delta and its inclusive per-source version range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorDelta {
    /// Stable typed-namespace key string.
    pub key: String,
    /// Stable CRDT delta codec manifest.
    pub crdt_manifest: String,
    /// CRDT delta codec schema version.
    pub crdt_version: u16,
    /// Codec-owned delta payload bytes.
    pub payload: Bytes,
    /// First source version represented by the payload.
    pub from_version: u64,
    /// Last source version represented by the payload.
    pub to_version: u64,
}

impl ReplicatorDelta {
    /// Creates a serialized delta range from codec output.
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

/// Successful acknowledgement of a causally acceptable delta propagation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicatorDeltaAck;

impl RemoteMessage for ReplicatorDeltaAck {
    const MANIFEST: &'static str = "kairo.ddata.delta-ack";
    const VERSION: u16 = 1;
}

/// Rejection requesting convergence through a later full-state path.
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
        assert_eq!(
            (ReplicatorGet::MANIFEST, ReplicatorGet::VERSION),
            ("kairo.ddata.replicator-get", 1)
        );
        assert_eq!(
            (ReplicatorUpdate::MANIFEST, ReplicatorUpdate::VERSION),
            ("kairo.ddata.replicator-update", 1)
        );
        assert_eq!(
            (ReplicatorSubscribe::MANIFEST, ReplicatorSubscribe::VERSION),
            ("kairo.ddata.replicator-subscribe", 1)
        );
        assert_eq!(
            (ReplicatorChanged::MANIFEST, ReplicatorChanged::VERSION),
            ("kairo.ddata.replicator-changed", 1)
        );
        assert_eq!(
            (ReplicatorWrite::MANIFEST, ReplicatorWrite::VERSION),
            ("kairo.ddata.write", 2)
        );
        assert_eq!(
            (ReplicatorWriteAck::MANIFEST, ReplicatorWriteAck::VERSION),
            ("kairo.ddata.write-ack", 1)
        );
        assert_eq!(
            (ReplicatorWriteNack::MANIFEST, ReplicatorWriteNack::VERSION),
            ("kairo.ddata.write-nack", 1)
        );
        assert_eq!(
            (ReplicatorRead::MANIFEST, ReplicatorRead::VERSION),
            ("kairo.ddata.read", 1)
        );
        assert_eq!(
            (
                ReplicatorReadResult::MANIFEST,
                ReplicatorReadResult::VERSION
            ),
            ("kairo.ddata.read-result", 2)
        );
        assert_eq!(
            (
                ReplicatorGossipStatus::MANIFEST,
                ReplicatorGossipStatus::VERSION
            ),
            ("kairo.ddata.gossip-status", 1)
        );
        assert_eq!(
            (ReplicatorGossip::MANIFEST, ReplicatorGossip::VERSION),
            ("kairo.ddata.gossip", 1)
        );
        assert_eq!(
            (
                ReplicatorDeltaPropagation::MANIFEST,
                ReplicatorDeltaPropagation::VERSION
            ),
            ("kairo.ddata.delta-propagation", 1)
        );
        assert_eq!(
            (ReplicatorDeltaAck::MANIFEST, ReplicatorDeltaAck::VERSION),
            ("kairo.ddata.delta-ack", 1)
        );
        assert_eq!(
            (ReplicatorDeltaNack::MANIFEST, ReplicatorDeltaNack::VERSION),
            ("kairo.ddata.delta-nack", 1)
        );
        assert!(!ReplicatorUpdate::MANIFEST.contains(std::any::type_name::<ReplicatorUpdate>()));
        assert!(!ReplicatorWrite::MANIFEST.contains(std::any::type_name::<ReplicatorWrite>()));
        assert!(
            !ReplicatorDeltaPropagation::MANIFEST
                .contains(std::any::type_name::<ReplicatorDeltaPropagation>())
        );
    }
}
