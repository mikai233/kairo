#![deny(missing_docs)]

use std::collections::BTreeMap;

use kairo_cluster::UniqueAddress;
use kairo_serialization::{RemoteMessage, SerializedMessage};

use crate::{PubSubRegistryDelta, TopicName};

/// Version summary exchanged at the start of a distributed-pubsub gossip round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubStatus {
    /// Exact member incarnation that produced this status.
    pub from: UniqueAddress,
    /// Highest known registry-bucket version keyed by owner ordering key.
    pub versions: BTreeMap<String, u64>,
    /// Whether this status is the single reply leg of an existing gossip round.
    pub reply: bool,
}

impl RemoteMessage for PubSubStatus {
    const MANIFEST: &'static str = "kairo.cluster-tools.pubsub.status";
    const VERSION: u16 = 1;
}

/// Bounded registry delta returned during a distributed-pubsub gossip round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubDelta {
    /// Exact member incarnation that produced this delta.
    pub from: UniqueAddress,
    /// Versioned registry buckets and entries carried by the delta.
    pub delta: PubSubRegistryDelta,
}

impl RemoteMessage for PubSubDelta {
    const MANIFEST: &'static str = "kairo.cluster-tools.pubsub.delta";
    const VERSION: u16 = 1;
}

/// Stable remote envelope for one topic publication.
///
/// A missing `group` requests local broadcast at the receiving mediator;
/// otherwise the receiver delivers once within the named local group. The
/// nested message retains its own serializer id, manifest, version, and bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubPublishEnvelope {
    /// Topic to publish to at the receiving mediator.
    pub topic: TopicName,
    /// Optional subscriber group selected by the originating mediator.
    pub group: Option<String>,
    /// Independently serialized business message.
    pub message: SerializedMessage,
}

impl RemoteMessage for PubSubPublishEnvelope {
    const MANIFEST: &'static str = "kairo.cluster-tools.pubsub.publish";
    const VERSION: u16 = 1;
}

/// Stable remote envelope for one logical actor-path delivery.
///
/// The receiving mediator re-enters this as local-only `Send` or `SendToAll`
/// processing, preventing a remote hop from starting another distributed fan-out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubPathEnvelope {
    /// Logical actor path registered at the receiving mediator.
    pub path: String,
    /// `false` selects one local routee; `true` selects every local routee.
    pub all: bool,
    /// Independently serialized business message.
    pub message: SerializedMessage,
}

impl RemoteMessage for PubSubPathEnvelope {
    const MANIFEST: &'static str = "kairo.cluster-tools.pubsub.path";
    const VERSION: u16 = 1;
}

/// Stable singleton handover request sent by a prospective new owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonHandOverToMe {
    /// Exact member incarnation requesting ownership.
    pub from: UniqueAddress,
}

impl RemoteMessage for SingletonHandOverToMe {
    const MANIFEST: &'static str = "kairo.cluster-tools.singleton.hand-over-to-me";
    const VERSION: u16 = 1;
}

/// Stable singleton response indicating that the previous owner is stopping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonHandOverInProgress {
    /// Exact member incarnation performing the handover.
    pub from: UniqueAddress,
}

impl RemoteMessage for SingletonHandOverInProgress {
    const MANIFEST: &'static str = "kairo.cluster-tools.singleton.hand-over-in-progress";
    const VERSION: u16 = 1;
}

/// Stable singleton response indicating that the previous owner has stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonHandOverDone {
    /// Exact member incarnation that completed the handover.
    pub from: UniqueAddress,
}

impl RemoteMessage for SingletonHandOverDone {
    const MANIFEST: &'static str = "kairo.cluster-tools.singleton.hand-over-done";
    const VERSION: u16 = 1;
}

/// Stable singleton takeover request sent to the current owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonTakeOverFromMe {
    /// Exact member incarnation asking the receiver to take ownership.
    pub from: UniqueAddress,
}

/// Stable envelope for a singleton's registered business protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonMessageEnvelope {
    /// Independently serialized singleton business message.
    pub message: SerializedMessage,
}

impl RemoteMessage for SingletonMessageEnvelope {
    const MANIFEST: &'static str = "kairo.cluster-tools.singleton.message";
    const VERSION: u16 = 1;
}

impl RemoteMessage for SingletonTakeOverFromMe {
    const MANIFEST: &'static str = "kairo.cluster-tools.singleton.take-over-from-me";
    const VERSION: u16 = 1;
}

#[cfg(test)]
mod tests {
    use kairo_serialization::RemoteMessage;

    use super::*;

    #[test]
    fn cluster_tools_system_manifests_are_stable() {
        assert_eq!(PubSubStatus::MANIFEST, "kairo.cluster-tools.pubsub.status");
        assert_eq!(PubSubDelta::MANIFEST, "kairo.cluster-tools.pubsub.delta");
        assert_eq!(
            PubSubPublishEnvelope::MANIFEST,
            "kairo.cluster-tools.pubsub.publish"
        );
        assert_eq!(
            PubSubPathEnvelope::MANIFEST,
            "kairo.cluster-tools.pubsub.path"
        );
        assert_eq!(
            SingletonHandOverToMe::MANIFEST,
            "kairo.cluster-tools.singleton.hand-over-to-me"
        );
        assert_eq!(
            SingletonHandOverInProgress::MANIFEST,
            "kairo.cluster-tools.singleton.hand-over-in-progress"
        );
        assert_eq!(
            SingletonHandOverDone::MANIFEST,
            "kairo.cluster-tools.singleton.hand-over-done"
        );
        assert_eq!(
            SingletonTakeOverFromMe::MANIFEST,
            "kairo.cluster-tools.singleton.take-over-from-me"
        );
        assert_eq!(PubSubStatus::VERSION, 1);
        assert_eq!(PubSubPublishEnvelope::VERSION, 1);
        assert_eq!(PubSubPathEnvelope::VERSION, 1);
        assert_eq!(SingletonHandOverToMe::VERSION, 1);
        assert_eq!(
            SingletonMessageEnvelope::MANIFEST,
            "kairo.cluster-tools.singleton.message"
        );
        assert!(!PubSubStatus::MANIFEST.contains(std::any::type_name::<PubSubStatus>()));
        assert!(!PubSubDelta::MANIFEST.contains(std::any::type_name::<PubSubDelta>()));
        assert!(
            !PubSubPublishEnvelope::MANIFEST
                .contains(std::any::type_name::<PubSubPublishEnvelope>())
        );
        assert!(
            !PubSubPathEnvelope::MANIFEST.contains(std::any::type_name::<PubSubPathEnvelope>())
        );
        assert!(
            !SingletonHandOverToMe::MANIFEST
                .contains(std::any::type_name::<SingletonHandOverToMe>())
        );
    }
}
