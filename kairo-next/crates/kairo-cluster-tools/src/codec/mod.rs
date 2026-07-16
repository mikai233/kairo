mod pubsub;
mod singleton;
mod wire;

use kairo_serialization::{Registry, SerializationRegistry};

pub use pubsub::{
    PUBSUB_DELTA_SERIALIZER_ID, PUBSUB_PATH_SERIALIZER_ID, PUBSUB_PUBLISH_SERIALIZER_ID,
    PUBSUB_STATUS_SERIALIZER_ID, PubSubDeltaCodec, PubSubPathEnvelopeCodec,
    PubSubPublishEnvelopeCodec, PubSubStatusCodec,
};
pub use singleton::{
    SINGLETON_HAND_OVER_DONE_SERIALIZER_ID, SINGLETON_HAND_OVER_IN_PROGRESS_SERIALIZER_ID,
    SINGLETON_HAND_OVER_TO_ME_SERIALIZER_ID, SINGLETON_MESSAGE_SERIALIZER_ID,
    SINGLETON_TAKE_OVER_FROM_ME_SERIALIZER_ID,
};
use singleton::{
    SingletonHandOverDoneCodec, SingletonHandOverInProgressCodec, SingletonHandOverToMeCodec,
    SingletonMessageEnvelopeCodec, SingletonTakeOverFromMeCodec,
};

use crate::{
    PubSubDelta, PubSubPathEnvelope, PubSubPublishEnvelope, PubSubStatus, SingletonHandOverDone,
    SingletonHandOverInProgress, SingletonHandOverToMe, SingletonMessageEnvelope,
    SingletonTakeOverFromMe,
};

/// Registers every stable cluster-tools system protocol codec.
///
/// Call this before the shared remote runtime binds. Registration fails on a
/// duplicate serializer id or message manifest instead of silently replacing
/// an existing wire contract.
pub fn register_cluster_tools_protocol_codecs(
    registry: &mut Registry,
) -> kairo_serialization::Result<()> {
    registry.register::<PubSubStatus, _>(PubSubStatusCodec)?;
    registry.register::<PubSubDelta, _>(PubSubDeltaCodec)?;
    registry.register::<PubSubPublishEnvelope, _>(PubSubPublishEnvelopeCodec)?;
    registry.register::<PubSubPathEnvelope, _>(PubSubPathEnvelopeCodec)?;
    registry.register::<SingletonHandOverToMe, _>(SingletonHandOverToMeCodec)?;
    registry.register::<SingletonHandOverInProgress, _>(SingletonHandOverInProgressCodec)?;
    registry.register::<SingletonHandOverDone, _>(SingletonHandOverDoneCodec)?;
    registry.register::<SingletonTakeOverFromMe, _>(SingletonTakeOverFromMeCodec)?;
    registry.register::<SingletonMessageEnvelope, _>(SingletonMessageEnvelopeCodec)?;
    Ok(())
}

#[cfg(test)]
mod tests;
