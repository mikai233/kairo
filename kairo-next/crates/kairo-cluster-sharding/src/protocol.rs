#![deny(missing_docs)]

use kairo_serialization::{ActorRefWireData, RemoteMessage, SerializedMessage};

/// Repeated region registration request sent to the shard coordinator.
///
/// A region retries this request until it receives [`RegisterAck`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Register {
    /// Stable wire representation of the registering region actor.
    pub region: ActorRefWireData,
}

impl RemoteMessage for Register {
    const MANIFEST: &'static str = "kairo.sharding.register";
    const VERSION: u16 = 1;
}

/// Coordinator acknowledgement that a region registration succeeded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterAck {
    /// Stable wire representation of the coordinator that accepted the region.
    pub coordinator: ActorRefWireData,
}

impl RemoteMessage for RegisterAck {
    const MANIFEST: &'static str = "kairo.sharding.register-ack";
    const VERSION: u16 = 1;
}

/// Region request for the current owner of a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetShardHome {
    /// Stable shard identifier whose owner is requested.
    pub shard_id: String,
}

impl RemoteMessage for GetShardHome {
    const MANIFEST: &'static str = "kairo.sharding.get-shard-home";
    const VERSION: u16 = 1;
}

/// Coordinator reply identifying the region that currently owns a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardHome {
    /// Stable shard identifier whose owner was resolved.
    pub shard_id: String,
    /// Stable wire representation of the owning region actor.
    pub region: ActorRefWireData,
}

impl RemoteMessage for ShardHome {
    const MANIFEST: &'static str = "kairo.sharding.shard-home";
    const VERSION: u16 = 1;
}

/// Coordinator command asking a region to start hosting a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostShard {
    /// Stable shard identifier to host.
    pub shard_id: String,
}

impl RemoteMessage for HostShard {
    const MANIFEST: &'static str = "kairo.sharding.host-shard";
    const VERSION: u16 = 1;
}

/// Region acknowledgement that a requested shard has started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardStarted {
    /// Stable identifier of the shard that started.
    pub shard_id: String,
}

impl RemoteMessage for ShardStarted {
    const MANIFEST: &'static str = "kairo.sharding.shard-started";
    const VERSION: u16 = 1;
}

/// First handoff phase, asking every region to invalidate a shard's cached home.
///
/// Regions buffer subsequent traffic for this shard and reply with
/// [`BeginHandOffAck`] before the coordinator asks the owner to stop it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginHandOff {
    /// Stable identifier of the shard entering handoff.
    pub shard_id: String,
}

impl RemoteMessage for BeginHandOff {
    const MANIFEST: &'static str = "kairo.sharding.begin-handoff";
    const VERSION: u16 = 1;
}

/// Region acknowledgement that it has entered the first handoff phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginHandOffAck {
    /// Stable identifier of the shard whose cached home was invalidated.
    pub shard_id: String,
}

impl RemoteMessage for BeginHandOffAck {
    const MANIFEST: &'static str = "kairo.sharding.begin-handoff-ack";
    const VERSION: u16 = 1;
}

/// Second handoff phase, asking the owning region to stop a shard and its entities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandOff {
    /// Stable identifier of the shard to stop.
    pub shard_id: String,
}

impl RemoteMessage for HandOff {
    const MANIFEST: &'static str = "kairo.sharding.handoff";
    const VERSION: u16 = 1;
}

/// Owning-region acknowledgement that a handed-off shard has stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardStopped {
    /// Stable identifier of the shard that stopped.
    pub shard_id: String,
}

impl RemoteMessage for ShardStopped {
    const MANIFEST: &'static str = "kairo.sharding.shard-stopped";
    const VERSION: u16 = 1;
}

/// Region request for coordinator-managed handoff before graceful shutdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GracefulShutdownReq {
    /// Stable wire representation of the region that is shutting down.
    pub region: ActorRefWireData,
}

impl RemoteMessage for GracefulShutdownReq {
    const MANIFEST: &'static str = "kairo.sharding.graceful-shutdown-req";
    const VERSION: u16 = 1;
}

/// Notification that an entire shard region has stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionStopped {
    /// Stable wire representation of the stopped region.
    pub region: ActorRefWireData,
}

impl RemoteMessage for RegionStopped {
    const MANIFEST: &'static str = "kairo.sharding.region-stopped";
    const VERSION: u16 = 1;
}

/// Serialized entity traffic routed between shard regions.
///
/// The envelope keeps routing metadata outside the serialized business
/// message so entity protocols do not need to embed entity or shard ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedShardEnvelope {
    /// Stable shard identifier selected by the sender.
    pub shard_id: String,
    /// Logical entity identifier within the shard.
    pub entity_id: String,
    /// Business message encoded by its registered application codec.
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
        let contracts = [
            (Register::MANIFEST, Register::VERSION),
            (RegisterAck::MANIFEST, RegisterAck::VERSION),
            (GetShardHome::MANIFEST, GetShardHome::VERSION),
            (ShardHome::MANIFEST, ShardHome::VERSION),
            (HostShard::MANIFEST, HostShard::VERSION),
            (ShardStarted::MANIFEST, ShardStarted::VERSION),
            (BeginHandOff::MANIFEST, BeginHandOff::VERSION),
            (BeginHandOffAck::MANIFEST, BeginHandOffAck::VERSION),
            (HandOff::MANIFEST, HandOff::VERSION),
            (ShardStopped::MANIFEST, ShardStopped::VERSION),
            (GracefulShutdownReq::MANIFEST, GracefulShutdownReq::VERSION),
            (RegionStopped::MANIFEST, RegionStopped::VERSION),
            (RoutedShardEnvelope::MANIFEST, RoutedShardEnvelope::VERSION),
        ];

        assert_eq!(
            contracts,
            [
                ("kairo.sharding.register", 1),
                ("kairo.sharding.register-ack", 1),
                ("kairo.sharding.get-shard-home", 1),
                ("kairo.sharding.shard-home", 1),
                ("kairo.sharding.host-shard", 1),
                ("kairo.sharding.shard-started", 1),
                ("kairo.sharding.begin-handoff", 1),
                ("kairo.sharding.begin-handoff-ack", 1),
                ("kairo.sharding.handoff", 1),
                ("kairo.sharding.shard-stopped", 1),
                ("kairo.sharding.graceful-shutdown-req", 1),
                ("kairo.sharding.region-stopped", 1),
                ("kairo.sharding.routed-envelope", 1),
            ]
        );
        assert!(!ShardHome::MANIFEST.contains(std::any::type_name::<ShardHome>()));
    }
}
