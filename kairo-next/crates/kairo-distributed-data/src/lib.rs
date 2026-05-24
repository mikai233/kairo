//! CRDT-based replicated data for Kairo clusters.

mod aggregation;
mod aggregation_transport;
mod aggregation_wire;
mod codec;
mod consistency;
mod crdt_codec;
mod data;
mod delta;
mod delta_receive;
mod delta_transport;
mod delta_wire;
mod envelope;
mod errors;
mod gcounter;
mod gset;
mod key;
mod orset;
mod pncounter;
mod protocol;
mod read_write_receive;
mod replica;
mod replicator_actor;
mod response;
mod state;

#[cfg(test)]
mod tests;

pub use aggregation::{
    AggregationError, ReadAggregationOutcome, ReadAggregationPlan, ReadAggregatorState,
    ReplicaSelection, WriteAggregationOutcome, WriteAggregationPlan, WriteAggregatorState,
    calculate_majority,
};
pub use aggregation_transport::{
    AggregationTarget, AggregationTransport, AggregationTransportFailure,
    AggregationTransportOperation, AggregationTransportReport,
};
pub use aggregation_wire::{
    decode_data_envelope, decode_read_result, encode_data_envelope, encode_read,
    encode_read_result, encode_write,
};
pub use codec::{
    REPLICATOR_CHANGED_SERIALIZER_ID, REPLICATOR_DELTA_ACK_SERIALIZER_ID,
    REPLICATOR_DELTA_NACK_SERIALIZER_ID, REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID,
    REPLICATOR_GET_SERIALIZER_ID, REPLICATOR_READ_RESULT_SERIALIZER_ID,
    REPLICATOR_READ_SERIALIZER_ID, REPLICATOR_SUBSCRIBE_SERIALIZER_ID,
    REPLICATOR_UPDATE_SERIALIZER_ID, REPLICATOR_WRITE_ACK_SERIALIZER_ID,
    REPLICATOR_WRITE_NACK_SERIALIZER_ID, REPLICATOR_WRITE_SERIALIZER_ID, ReplicatorChangedCodec,
    ReplicatorDeltaAckCodec, ReplicatorDeltaNackCodec, ReplicatorDeltaPropagationCodec,
    ReplicatorGetCodec, ReplicatorReadCodec, ReplicatorReadResultCodec, ReplicatorSubscribeCodec,
    ReplicatorUpdateCodec, ReplicatorWriteAckCodec, ReplicatorWriteCodec, ReplicatorWriteNackCodec,
    register_ddata_protocol_codecs,
};
pub use consistency::{ReadConsistency, WriteConsistency};
pub use crdt_codec::{
    CRDT_CODEC_VERSION, CrdtDataCodec, GCOUNTER_MANIFEST, GCounterCodec, GSET_STRING_MANIFEST,
    GSetStringCodec, PNCOUNTER_MANIFEST, PNCounterCodec, SerializedCrdt,
};
pub use data::{DeltaReplicatedData, ReplicatedData, ReplicatedDelta};
pub use delta::{DeltaPropagation, DeltaPropagationEntry, DeltaPropagationLog};
pub use delta_receive::{
    DeltaPropagationReceiveReport, DeltaReceiveFailure, DeltaReceiveReply, DeltaReceiveStatus,
    DeltaReceiveTracker,
};
pub use delta_transport::{
    DeltaPropagationTarget, DeltaPropagationTransport, DeltaTransportFailure, DeltaTransportReport,
};
pub use delta_wire::{
    DecodedReplicatorDelta, decode_delta, decode_delta_propagation, encode_delta_propagation,
};
pub use envelope::DataEnvelope;
pub use errors::{ConsistencyError, CrdtError};
pub use gcounter::GCounter;
pub use gset::GSet;
pub use key::ReplicatorKey;
pub use orset::{ORSet, ORSetDelta, ORSetRemoveDelta};
pub use pncounter::PNCounter;
pub use protocol::{
    ReplicatorChanged, ReplicatorDataEnvelope, ReplicatorDelta, ReplicatorDeltaAck,
    ReplicatorDeltaNack, ReplicatorDeltaPropagation, ReplicatorGet, ReplicatorRead,
    ReplicatorReadResult, ReplicatorSubscribe, ReplicatorUpdate, ReplicatorWrite,
    ReplicatorWriteAck, ReplicatorWriteNack,
};
pub use read_write_receive::{DirectReadResult, DirectWriteResult, apply_write, serve_read};
pub use replica::ReplicaId;
pub use replicator_actor::{ReplicatorActor, ReplicatorActorMsg};
pub use response::{GetResponse, ReplicatorChange, UpdateOutcome, UpdateResponse};
pub use state::ReplicatorState;
