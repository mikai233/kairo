use kairo_serialization::{ActorRefWireData, RemoteMessage, SerializedMessage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Register {
    pub region: ActorRefWireData,
}

impl RemoteMessage for Register {
    const MANIFEST: &'static str = "kairo.sharding.register";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterAck {
    pub coordinator: ActorRefWireData,
}

impl RemoteMessage for RegisterAck {
    const MANIFEST: &'static str = "kairo.sharding.register-ack";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetShardHome {
    pub shard_id: String,
}

impl RemoteMessage for GetShardHome {
    const MANIFEST: &'static str = "kairo.sharding.get-shard-home";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardHome {
    pub shard_id: String,
    pub region: ActorRefWireData,
}

impl RemoteMessage for ShardHome {
    const MANIFEST: &'static str = "kairo.sharding.shard-home";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostShard {
    pub shard_id: String,
}

impl RemoteMessage for HostShard {
    const MANIFEST: &'static str = "kairo.sharding.host-shard";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardStarted {
    pub shard_id: String,
}

impl RemoteMessage for ShardStarted {
    const MANIFEST: &'static str = "kairo.sharding.shard-started";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginHandOff {
    pub shard_id: String,
}

impl RemoteMessage for BeginHandOff {
    const MANIFEST: &'static str = "kairo.sharding.begin-handoff";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginHandOffAck {
    pub shard_id: String,
}

impl RemoteMessage for BeginHandOffAck {
    const MANIFEST: &'static str = "kairo.sharding.begin-handoff-ack";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandOff {
    pub shard_id: String,
}

impl RemoteMessage for HandOff {
    const MANIFEST: &'static str = "kairo.sharding.handoff";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardStopped {
    pub shard_id: String,
}

impl RemoteMessage for ShardStopped {
    const MANIFEST: &'static str = "kairo.sharding.shard-stopped";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GracefulShutdownReq {
    pub region: ActorRefWireData,
}

impl RemoteMessage for GracefulShutdownReq {
    const MANIFEST: &'static str = "kairo.sharding.graceful-shutdown-req";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionStopped {
    pub region: ActorRefWireData,
}

impl RemoteMessage for RegionStopped {
    const MANIFEST: &'static str = "kairo.sharding.region-stopped";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedShardEnvelope {
    pub shard_id: String,
    pub entity_id: String,
    pub message: SerializedMessage,
}

impl RemoteMessage for RoutedShardEnvelope {
    const MANIFEST: &'static str = "kairo.sharding.routed-envelope";
    const VERSION: u16 = 1;
}

#[cfg(test)]
mod tests {
    use kairo_serialization::RemoteMessage;

    use super::*;

    #[test]
    fn sharding_system_manifests_are_stable() {
        assert_eq!(Register::MANIFEST, "kairo.sharding.register");
        assert_eq!(RegisterAck::MANIFEST, "kairo.sharding.register-ack");
        assert_eq!(GetShardHome::MANIFEST, "kairo.sharding.get-shard-home");
        assert_eq!(ShardHome::MANIFEST, "kairo.sharding.shard-home");
        assert_eq!(HostShard::MANIFEST, "kairo.sharding.host-shard");
        assert_eq!(ShardStarted::MANIFEST, "kairo.sharding.shard-started");
        assert_eq!(BeginHandOff::MANIFEST, "kairo.sharding.begin-handoff");
        assert_eq!(
            BeginHandOffAck::MANIFEST,
            "kairo.sharding.begin-handoff-ack"
        );
        assert_eq!(HandOff::MANIFEST, "kairo.sharding.handoff");
        assert_eq!(ShardStopped::VERSION, 1);
        assert_eq!(
            GracefulShutdownReq::MANIFEST,
            "kairo.sharding.graceful-shutdown-req"
        );
        assert_eq!(RegionStopped::MANIFEST, "kairo.sharding.region-stopped");
        assert_eq!(
            RoutedShardEnvelope::MANIFEST,
            "kairo.sharding.routed-envelope"
        );
        assert!(!ShardHome::MANIFEST.contains(std::any::type_name::<ShardHome>()));
    }
}
