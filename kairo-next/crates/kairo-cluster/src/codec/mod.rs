mod control;
mod daemon;
mod gossip;
mod wire;

use kairo_serialization::{Registry, SerializationRegistry};

pub use control::{
    HEARTBEAT_RSP_SERIALIZER_ID, HEARTBEAT_SERIALIZER_ID, HeartbeatCodec, HeartbeatRspCodec,
    JOIN_SERIALIZER_ID, JoinCodec,
};
pub use daemon::{
    DOWN_SERIALIZER_ID, DownCodec, EXITING_CONFIRMED_SERIALIZER_ID, ExitingConfirmedCodec,
    GOSSIP_STATUS_SERIALIZER_ID, GossipStatusCodec, INIT_JOIN_ACK_SERIALIZER_ID,
    INIT_JOIN_NACK_SERIALIZER_ID, INIT_JOIN_SERIALIZER_ID, InitJoinAckCodec, InitJoinCodec,
    InitJoinNackCodec, LEAVE_SERIALIZER_ID, LeaveCodec,
};
pub use gossip::{
    GOSSIP_ENVELOPE_SERIALIZER_ID, GossipEnvelopeCodec, WELCOME_SERIALIZER_ID, WelcomeCodec,
};

use crate::{
    Down, ExitingConfirmed, GossipEnvelope, GossipStatus, Heartbeat, HeartbeatRsp, InitJoin,
    InitJoinAck, InitJoinNack, Join, Leave, Welcome,
};

pub fn register_cluster_control_codecs(registry: &mut Registry) -> kairo_serialization::Result<()> {
    registry.register::<Heartbeat, _>(HeartbeatCodec)?;
    registry.register::<HeartbeatRsp, _>(HeartbeatRspCodec)?;
    registry.register::<Join, _>(JoinCodec)?;
    registry.register::<InitJoin, _>(InitJoinCodec)?;
    registry.register::<InitJoinAck, _>(InitJoinAckCodec)?;
    registry.register::<InitJoinNack, _>(InitJoinNackCodec)?;
    registry.register::<GossipStatus, _>(GossipStatusCodec)?;
    registry.register::<Leave, _>(LeaveCodec)?;
    registry.register::<Down, _>(DownCodec)?;
    registry.register::<ExitingConfirmed, _>(ExitingConfirmedCodec)?;
    Ok(())
}

pub fn register_cluster_protocol_codecs(
    registry: &mut Registry,
) -> kairo_serialization::Result<()> {
    register_cluster_control_codecs(registry)?;
    registry.register::<Welcome, _>(WelcomeCodec)?;
    registry.register::<GossipEnvelope, _>(GossipEnvelopeCodec)?;
    Ok(())
}

#[cfg(test)]
mod tests;
