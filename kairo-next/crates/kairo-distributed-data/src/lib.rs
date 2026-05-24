//! CRDT-based replicated data for Kairo clusters.

mod codec;
mod protocol;

pub use codec::{
    REPLICATOR_CHANGED_SERIALIZER_ID, REPLICATOR_GET_SERIALIZER_ID,
    REPLICATOR_SUBSCRIBE_SERIALIZER_ID, REPLICATOR_UPDATE_SERIALIZER_ID, ReplicatorChangedCodec,
    ReplicatorGetCodec, ReplicatorSubscribeCodec, ReplicatorUpdateCodec,
    register_ddata_protocol_codecs,
};
pub use protocol::{ReplicatorChanged, ReplicatorGet, ReplicatorSubscribe, ReplicatorUpdate};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplicatorKey(String);

impl ReplicatorKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
