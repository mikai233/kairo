//! CRDT-based replicated data for Kairo clusters.

mod aggregation;
mod aggregation_actor;
mod aggregation_operation;
mod aggregation_session;
mod aggregation_transport;
mod aggregation_wire;
mod cluster_connector_timing;
mod cluster_routes;
mod cluster_subscription;
mod codec;
mod consistency;
mod crdt_codec;
mod data;
mod delta;
mod delta_loop;
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
mod pruning;
mod read_write_receive;
mod remote_association;
mod remote_association_inbound;
mod remote_envelope;
mod remote_reply;
mod remote_request;
mod remote_targets;
mod replica;
mod replicator_actor;
mod replicator_aggregation;
mod reply_wire;
mod response;
mod state;
mod wire;

#[cfg(test)]
mod tests;

pub use aggregation::{
    AggregationError, ReadAggregationOutcome, ReadAggregationPlan, ReadAggregatorState,
    ReplicaSelection, WriteAggregationOutcome, WriteAggregationPlan, WriteAggregatorState,
    calculate_majority,
};
pub use aggregation_actor::{
    ReadAggregationActor, ReadAggregationActorEvent, ReadAggregationActorMsg,
    WriteAggregationActor, WriteAggregationActorEvent, WriteAggregationActorMsg,
};
pub use aggregation_operation::{
    ReadAggregationOperation, ReadAggregationOperationEvent, ReadAggregationOperationMsg,
    WriteAggregationOperation, WriteAggregationOperationEvent, WriteAggregationOperationMsg,
};
pub use aggregation_session::{
    ReadAggregationSession, ReadAggregationSessionEvent, ReadAggregationSessionMsg,
    ReadAggregationSessionOutcome, WriteAggregationSession, WriteAggregationSessionEvent,
    WriteAggregationSessionMsg,
};
pub use aggregation_transport::{
    AggregationTarget, AggregationTargetRegistry, AggregationTransport,
    AggregationTransportFailure, AggregationTransportOperation, AggregationTransportReport,
    SenderAwareRecipient,
};
pub use aggregation_wire::{
    decode_data_envelope, decode_read_result, encode_data_envelope, encode_read,
    encode_read_result, encode_write,
};
pub use cluster_connector_timing::{
    ReplicatorClusterConnectorClock, ReplicatorClusterConnectorTimingSettings,
    SharedReplicatorClusterConnectorClock, SystemReplicatorClusterConnectorClock,
};
pub use cluster_routes::{
    ReplicatorClusterRouteReport, ReplicatorClusterRouteUpdate, ReplicatorClusterRoutes,
};
pub use cluster_subscription::{
    ReplicatorClusterConnector, ReplicatorClusterConnectorMsg, ReplicatorClusterConnectorSnapshot,
    ReplicatorClusterPruningSettings,
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
pub use data::{DeltaReplicatedData, RemovedNodePruning, ReplicatedData, ReplicatedDelta};
pub use delta::{DeltaPropagation, DeltaPropagationEntry, DeltaPropagationLog};
pub use delta_loop::{DeltaPropagationLoop, DeltaPropagationSink, DeltaPropagationTickReport};
pub use delta_receive::{
    DeltaPropagationReceiveReport, DeltaReceiveFailure, DeltaReceiveReply, DeltaReceiveStatus,
    DeltaReceiveTracker,
};
pub use delta_transport::{
    DeltaPropagationTarget, DeltaPropagationTargetRegistry, DeltaPropagationTransport,
    DeltaTransportFailure, DeltaTransportReport,
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
    ReplicatorDeltaNack, ReplicatorDeltaPropagation, ReplicatorGet, ReplicatorPruningEntry,
    ReplicatorPruningState, ReplicatorRead, ReplicatorReadResult, ReplicatorSubscribe,
    ReplicatorUpdate, ReplicatorWrite, ReplicatorWriteAck, ReplicatorWriteNack,
};
pub use pruning::{
    PruningInitialized, PruningPerformed, PruningState, PruningTable, RemovedNodePruningFailure,
    RemovedNodePruningTick, RemovedNodePruningTickReport, RemovedNodePruningTracker,
};
pub use read_write_receive::{DirectReadResult, DirectWriteResult, apply_write, serve_read};
pub use remote_association::{
    ReplicatorRemoteAssociationError, ReplicatorRemoteAssociationOutbound,
    ReplicatorRemoteAssociationRoutes,
};
pub use remote_association_inbound::{
    ReplicatorRemoteAssociationInbound, ReplicatorRemoteAssociationInboundError,
    ReplicatorRemoteReplyReceiver, ReplicatorRemoteRequestReceiver, is_replicator_reply_manifest,
    is_replicator_request_manifest,
};
pub use remote_envelope::{
    ReplicatorRemoteEnvelope, ReplicatorRemoteEnvelopeError, ReplicatorRemoteEnvelopeInbound,
    ReplicatorRemoteEnvelopeOutbound, ReplicatorRemoteInboundMessage, ReplicatorRemoteTarget,
};
pub use remote_reply::{
    ReplicatorRemoteReplyError, ReplicatorRemoteReplyInbound, ReplicatorRemoteReplyOutbound,
};
pub use remote_request::{ReplicatorRemoteRequestError, ReplicatorRemoteRequestInbound};
pub use remote_targets::{
    DEFAULT_REPLICATOR_REMOTE_PATH, ReplicatorRemoteRouteRegistrationReport,
    ReplicatorRemoteRouteTargets, ReplicatorRemoteTargetError,
    ReplicatorRemoteTargetRegistrationReport,
};
pub use replica::ReplicaId;
pub use replicator_actor::{ReplicatorActor, ReplicatorActorMsg};
pub use replicator_aggregation::ReplicatorAggregation;
pub use reply_wire::{
    ReplicatorReplyWireError, ReplicatorReplyWireInbound, ReplicatorReplyWireOutbound,
    ReplicatorSerializedReply, ReplicatorWireReply,
};
pub use response::{GetResponse, ReplicatorChange, UpdateOutcome, UpdateResponse};
pub use state::ReplicatorState;
pub use wire::{
    ReplicatorSerializedMessage, ReplicatorWireCodecs, ReplicatorWireError, ReplicatorWireInbound,
    ReplicatorWireOutbound, ReplicatorWireReplies,
};
