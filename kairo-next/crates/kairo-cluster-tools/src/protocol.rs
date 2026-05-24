use std::collections::BTreeMap;

use kairo_cluster::UniqueAddress;
use kairo_serialization::RemoteMessage;

use crate::PubSubRegistryDelta;

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

#[cfg(test)]
mod tests {
    use kairo_serialization::RemoteMessage;

    use super::*;

    #[test]
    fn cluster_tools_system_manifests_are_stable() {
        assert_eq!(PubSubStatus::MANIFEST, "kairo.cluster-tools.pubsub.status");
        assert_eq!(PubSubDelta::MANIFEST, "kairo.cluster-tools.pubsub.delta");
        assert_eq!(PubSubStatus::VERSION, 1);
        assert!(!PubSubStatus::MANIFEST.contains(std::any::type_name::<PubSubStatus>()));
        assert!(!PubSubDelta::MANIFEST.contains(std::any::type_name::<PubSubDelta>()));
    }
}
