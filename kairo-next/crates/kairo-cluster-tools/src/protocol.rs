use std::collections::BTreeMap;

use kairo_cluster::UniqueAddress;
use kairo_serialization::{RemoteMessage, SerializedMessage};

use crate::{PubSubRegistryDelta, TopicName};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubStatus {
    pub from: UniqueAddress,
    pub versions: BTreeMap<String, u64>,
    pub reply: bool,
}

impl RemoteMessage for PubSubStatus {
    const MANIFEST: &'static str = "kairo.cluster-tools.pubsub.status";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubDelta {
    pub from: UniqueAddress,
    pub delta: PubSubRegistryDelta,
}

impl RemoteMessage for PubSubDelta {
    const MANIFEST: &'static str = "kairo.cluster-tools.pubsub.delta";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubPublishEnvelope {
    pub topic: TopicName,
    pub group: Option<String>,
    pub message: SerializedMessage,
}

impl RemoteMessage for PubSubPublishEnvelope {
    const MANIFEST: &'static str = "kairo.cluster-tools.pubsub.publish";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonHandOverToMe {
    pub from: UniqueAddress,
}

impl RemoteMessage for SingletonHandOverToMe {
    const MANIFEST: &'static str = "kairo.cluster-tools.singleton.hand-over-to-me";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonHandOverInProgress {
    pub from: UniqueAddress,
}

impl RemoteMessage for SingletonHandOverInProgress {
    const MANIFEST: &'static str = "kairo.cluster-tools.singleton.hand-over-in-progress";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonHandOverDone {
    pub from: UniqueAddress,
}

impl RemoteMessage for SingletonHandOverDone {
    const MANIFEST: &'static str = "kairo.cluster-tools.singleton.hand-over-done";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonTakeOverFromMe {
    pub from: UniqueAddress,
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
        assert_eq!(SingletonHandOverToMe::VERSION, 1);
        assert!(!PubSubStatus::MANIFEST.contains(std::any::type_name::<PubSubStatus>()));
        assert!(!PubSubDelta::MANIFEST.contains(std::any::type_name::<PubSubDelta>()));
        assert!(
            !PubSubPublishEnvelope::MANIFEST
                .contains(std::any::type_name::<PubSubPublishEnvelope>())
        );
        assert!(
            !SingletonHandOverToMe::MANIFEST
                .contains(std::any::type_name::<SingletonHandOverToMe>())
        );
    }
}
