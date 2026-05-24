//! CRDT-based replicated data for Kairo clusters.

mod codec;
mod data;
mod errors;
mod gcounter;
mod gset;
mod key;
mod pncounter;
mod protocol;
mod replica;

#[cfg(test)]
mod tests;

pub use codec::{
    REPLICATOR_CHANGED_SERIALIZER_ID, REPLICATOR_GET_SERIALIZER_ID,
    REPLICATOR_SUBSCRIBE_SERIALIZER_ID, REPLICATOR_UPDATE_SERIALIZER_ID, ReplicatorChangedCodec,
    ReplicatorGetCodec, ReplicatorSubscribeCodec, ReplicatorUpdateCodec,
    register_ddata_protocol_codecs,
};
pub use data::{DeltaReplicatedData, ReplicatedData, ReplicatedDelta};
pub use errors::CrdtError;
pub use gcounter::GCounter;
pub use gset::GSet;
pub use key::ReplicatorKey;
pub use pncounter::PNCounter;
pub use protocol::{ReplicatorChanged, ReplicatorGet, ReplicatorSubscribe, ReplicatorUpdate};
pub use replica::ReplicaId;
