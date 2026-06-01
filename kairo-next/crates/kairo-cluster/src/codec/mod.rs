mod control;
mod gossip;
mod wire;

use kairo_serialization::{Registry, SerializationRegistry};

pub use control::{
    HEARTBEAT_RSP_SERIALIZER_ID, HEARTBEAT_SERIALIZER_ID, HeartbeatCodec, HeartbeatRspCodec,
    JOIN_SERIALIZER_ID, JoinCodec,
};
pub use gossip::{
    GOSSIP_ENVELOPE_SERIALIZER_ID, GossipEnvelopeCodec, WELCOME_SERIALIZER_ID, WelcomeCodec,
};

use crate::{GossipEnvelope, Heartbeat, HeartbeatRsp, Join, Welcome};

pub fn register_cluster_control_codecs(registry: &mut Registry) -> kairo_serialization::Result<()> {
    registry.register::<Heartbeat, _>(HeartbeatCodec)?;
    registry.register::<HeartbeatRsp, _>(HeartbeatRspCodec)?;
    registry.register::<Join, _>(JoinCodec)?;
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
