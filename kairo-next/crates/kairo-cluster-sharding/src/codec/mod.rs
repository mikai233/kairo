#![deny(missing_docs)]

mod coordinator;
mod routing;
mod shard;
mod shutdown;
mod wire;

use kairo_serialization::{Registry, SerializationRegistry};

pub use coordinator::{
    GET_SHARD_HOME_SERIALIZER_ID, GetShardHomeCodec, REGISTER_ACK_SERIALIZER_ID,
    REGISTER_SERIALIZER_ID, RegisterAckCodec, RegisterCodec, SHARD_HOME_SERIALIZER_ID,
    ShardHomeCodec,
};
pub use routing::{ROUTED_SHARD_ENVELOPE_SERIALIZER_ID, RoutedShardEnvelopeCodec};
pub use shard::{
    BEGIN_HANDOFF_ACK_SERIALIZER_ID, BEGIN_HANDOFF_SERIALIZER_ID, BeginHandOffAckCodec,
    BeginHandOffCodec, HANDOFF_SERIALIZER_ID, HOST_SHARD_SERIALIZER_ID, HandOffCodec,
    HostShardCodec, SHARD_STARTED_SERIALIZER_ID, SHARD_STOPPED_SERIALIZER_ID, ShardStartedCodec,
    ShardStoppedCodec,
};
pub use shutdown::{
    GRACEFUL_SHUTDOWN_REQ_SERIALIZER_ID, GracefulShutdownReqCodec, REGION_STOPPED_SERIALIZER_ID,
    RegionStoppedCodec,
};

use crate::{
    BeginHandOff, BeginHandOffAck, GetShardHome, GracefulShutdownReq, HandOff, HostShard,
    RegionStopped, Register, RegisterAck, RoutedShardEnvelope, ShardHome, ShardStarted,
    ShardStopped,
};

/// Registers every stable cluster-sharding system-message codec.
///
/// Call this once while composing the shared serialization registry used by
/// the remoting runtime. Existing manifests and serializer ids are wire
/// contracts and must not be reassigned to different message shapes.
pub fn register_sharding_protocol_codecs(
    registry: &mut Registry,
) -> kairo_serialization::Result<()> {
    registry.register::<Register, _>(RegisterCodec)?;
    registry.register::<RegisterAck, _>(RegisterAckCodec)?;
    registry.register::<GetShardHome, _>(GetShardHomeCodec)?;
    registry.register::<ShardHome, _>(ShardHomeCodec)?;
    registry.register::<HostShard, _>(HostShardCodec)?;
    registry.register::<ShardStarted, _>(ShardStartedCodec)?;
    registry.register::<BeginHandOff, _>(BeginHandOffCodec)?;
    registry.register::<BeginHandOffAck, _>(BeginHandOffAckCodec)?;
    registry.register::<HandOff, _>(HandOffCodec)?;
    registry.register::<ShardStopped, _>(ShardStoppedCodec)?;
    registry.register::<RoutedShardEnvelope, _>(RoutedShardEnvelopeCodec)?;
    registry.register::<GracefulShutdownReq, _>(GracefulShutdownReqCodec)?;
    registry.register::<RegionStopped, _>(RegionStoppedCodec)?;
    Ok(())
}

#[cfg(test)]
mod tests;
