use kairo_serialization::RemoteMessage;

use crate::UniqueAddress;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Join {
    pub node: UniqueAddress,
}

impl RemoteMessage for Join {
    const MANIFEST: &'static str = "kairo.cluster.join";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Welcome {
    pub from: UniqueAddress,
}

impl RemoteMessage for Welcome {
    const MANIFEST: &'static str = "kairo.cluster.welcome";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipEnvelope {
    pub from: UniqueAddress,
    pub to: UniqueAddress,
    pub sequence_nr: u64,
}

impl RemoteMessage for GossipEnvelope {
    const MANIFEST: &'static str = "kairo.cluster.gossip-envelope";
    const VERSION: u16 = 1;
}

#[cfg(test)]
mod tests {
    use kairo_serialization::RemoteMessage;

    use super::*;

    #[test]
    fn cluster_system_manifests_are_stable() {
        assert_eq!(Join::MANIFEST, "kairo.cluster.join");
        assert_eq!(Welcome::MANIFEST, "kairo.cluster.welcome");
        assert_eq!(GossipEnvelope::MANIFEST, "kairo.cluster.gossip-envelope");
        assert_eq!(Join::VERSION, 1);
        assert!(!GossipEnvelope::MANIFEST.contains(std::any::type_name::<GossipEnvelope>()));
    }
}
