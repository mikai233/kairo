use bytes::Bytes;
use kairo_actor::Address;
use kairo_serialization::RemoteMessage;

use crate::{Gossip, UniqueAddress, VectorClock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterConfigCheck {
    Unchecked,
    Compatible,
    Incompatible,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitJoin {
    pub joining_config_digest: Bytes,
}

impl RemoteMessage for InitJoin {
    const MANIFEST: &'static str = "kairo.cluster.init-join";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitJoinAck {
    pub address: Address,
    pub config_check: ClusterConfigCheck,
}

impl RemoteMessage for InitJoinAck {
    const MANIFEST: &'static str = "kairo.cluster.init-join-ack";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitJoinNack {
    pub address: Address,
}

impl RemoteMessage for InitJoinNack {
    const MANIFEST: &'static str = "kairo.cluster.init-join-nack";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    pub from: UniqueAddress,
    pub sequence_nr: u64,
    pub creation_time_nanos: u64,
}

impl RemoteMessage for Heartbeat {
    const MANIFEST: &'static str = "kairo.cluster.heartbeat";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatRsp {
    pub from: UniqueAddress,
    pub sequence_nr: u64,
    pub creation_time_nanos: u64,
}

impl RemoteMessage for HeartbeatRsp {
    const MANIFEST: &'static str = "kairo.cluster.heartbeat-rsp";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Join {
    pub node: UniqueAddress,
    pub roles: Vec<String>,
}

impl RemoteMessage for Join {
    const MANIFEST: &'static str = "kairo.cluster.join";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Welcome {
    pub from: UniqueAddress,
    pub gossip: Gossip,
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
    pub gossip: Gossip,
}

impl RemoteMessage for GossipEnvelope {
    const MANIFEST: &'static str = "kairo.cluster.gossip-envelope";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipStatus {
    pub from: UniqueAddress,
    pub version: VectorClock,
    pub seen_digest: Bytes,
}

impl RemoteMessage for GossipStatus {
    const MANIFEST: &'static str = "kairo.cluster.gossip-status";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leave {
    pub address: Address,
}

impl RemoteMessage for Leave {
    const MANIFEST: &'static str = "kairo.cluster.leave";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Down {
    pub address: Address,
}

impl RemoteMessage for Down {
    const MANIFEST: &'static str = "kairo.cluster.down";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitingConfirmed {
    pub node: UniqueAddress,
}

impl RemoteMessage for ExitingConfirmed {
    const MANIFEST: &'static str = "kairo.cluster.exiting-confirmed";
    const VERSION: u16 = 1;
}

#[cfg(test)]
mod tests {
    use kairo_serialization::RemoteMessage;

    use super::*;

    #[test]
    fn cluster_system_manifests_are_stable() {
        assert_eq!(InitJoin::MANIFEST, "kairo.cluster.init-join");
        assert_eq!(InitJoinAck::MANIFEST, "kairo.cluster.init-join-ack");
        assert_eq!(InitJoinNack::MANIFEST, "kairo.cluster.init-join-nack");
        assert_eq!(Heartbeat::MANIFEST, "kairo.cluster.heartbeat");
        assert_eq!(HeartbeatRsp::MANIFEST, "kairo.cluster.heartbeat-rsp");
        assert_eq!(Join::MANIFEST, "kairo.cluster.join");
        assert_eq!(Welcome::MANIFEST, "kairo.cluster.welcome");
        assert_eq!(GossipEnvelope::MANIFEST, "kairo.cluster.gossip-envelope");
        assert_eq!(GossipStatus::MANIFEST, "kairo.cluster.gossip-status");
        assert_eq!(Leave::MANIFEST, "kairo.cluster.leave");
        assert_eq!(Down::MANIFEST, "kairo.cluster.down");
        assert_eq!(
            ExitingConfirmed::MANIFEST,
            "kairo.cluster.exiting-confirmed"
        );
        assert_eq!(Heartbeat::VERSION, 1);
        assert_eq!(HeartbeatRsp::VERSION, 1);
        assert_eq!(Join::VERSION, 1);
        assert!(!Heartbeat::MANIFEST.contains(std::any::type_name::<Heartbeat>()));
        assert!(!GossipEnvelope::MANIFEST.contains(std::any::type_name::<GossipEnvelope>()));
        assert!(!InitJoin::MANIFEST.contains(std::any::type_name::<InitJoin>()));
    }
}
