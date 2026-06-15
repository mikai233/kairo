//! CRDT-based replicated data for Kairo clusters.
//!
//! `kairo-distributed-data` is built around Pekko-style replicated data
//! semantics, but keeps the Rust implementation split into explicit state and
//! transport boundaries. CRDT types such as [`GSet`], [`GCounter`],
//! [`PNCounter`], and [`ORSet`] own merge behavior. [`ReplicatorState`] owns
//! local get/update/write transitions and separates changed-key flushing from
//! the update turn. Actor, aggregation, delta, gossip, pruning, cluster-route,
//! and TCP association modules compose those pieces without making
//! distributed data a cluster membership authority.
//!
//! Local CRDT values do not need remote serialization. Messages and CRDT
//! payloads only cross remote boundaries through stable
//! [`RemoteMessage`](kairo_serialization::RemoteMessage) manifests, serializer
//! ids, versions, and registered codecs. The built-in protocol codecs use
//! explicit fields for replica ids, keys, pruning metadata, deltas, full-state
//! gossip, and direct read/write replies; they must not depend on Rust type
//! names, enum discriminants, or memory layout.
//!
//! ```
//! use kairo_distributed_data::{
//!     GCounter, GetResponse, ReplicaId, ReplicatorKey, ReplicatorState,
//! };
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let replica = ReplicaId::new("node-a");
//! let key = ReplicatorKey::new("counters.requests");
//! let mut state = ReplicatorState::<GCounter>::new();
//!
//! let outcome = state.update_local(key.clone(), GCounter::new(), |counter| {
//!     counter.increment(replica.clone(), 3)
//! })?;
//! assert!(outcome.changed());
//! assert!(outcome.delta().is_some());
//!
//! match state.get_local(&key) {
//!     GetResponse::Success { data, .. } => assert_eq!(data.value()?, 3),
//!     other => panic!("expected successful local read, got {other:?}"),
//! }
//!
//! let changes = state.flush_changes();
//! assert_eq!(changes.len(), 1);
//! assert_eq!(changes[0].key(), &key);
//! assert_eq!(changes[0].data().value()?, 3);
//! assert!(state.flush_changes().is_empty());
//! # Ok(())
//! # }
//! ```
//!
//! The public surface intentionally exposes focused building blocks instead of
//! one erased protocol. [`ReplicatorActor`] wires CRDT state into synchronous
//! actor turns, aggregation session actors handle quorum-style read/write
//! operations, delta and gossip transports encode already-addressed
//! replicator traffic, cluster connectors derive replica routes from cluster
//! events, and TCP peer runtimes route those envelopes through `kairo-remote`
//! association caches.

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
mod gossip;
mod gossip_transport;
mod gset;
mod key;
mod lww_register;
mod ormap;
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
mod remote_tcp;
mod replica;
mod replicator_actor;
mod replicator_aggregation;
mod reply_wire;
mod response;
mod state;
mod tcp_peer_bootstrap;
mod tcp_peer_connector;
mod tcp_peer_reconnect;
mod tcp_peer_routes;
mod tcp_peer_runtime;
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
    REPLICATOR_GET_SERIALIZER_ID, REPLICATOR_GOSSIP_SERIALIZER_ID,
    REPLICATOR_GOSSIP_STATUS_SERIALIZER_ID, REPLICATOR_READ_RESULT_SERIALIZER_ID,
    REPLICATOR_READ_SERIALIZER_ID, REPLICATOR_SUBSCRIBE_SERIALIZER_ID,
    REPLICATOR_UPDATE_SERIALIZER_ID, REPLICATOR_WRITE_ACK_SERIALIZER_ID,
    REPLICATOR_WRITE_NACK_SERIALIZER_ID, REPLICATOR_WRITE_SERIALIZER_ID, ReplicatorChangedCodec,
    ReplicatorDeltaAckCodec, ReplicatorDeltaNackCodec, ReplicatorDeltaPropagationCodec,
    ReplicatorGetCodec, ReplicatorGossipCodec, ReplicatorGossipStatusCodec, ReplicatorReadCodec,
    ReplicatorReadResultCodec, ReplicatorSubscribeCodec, ReplicatorUpdateCodec,
    ReplicatorWriteAckCodec, ReplicatorWriteCodec, ReplicatorWriteNackCodec,
    register_ddata_protocol_codecs,
};
pub use consistency::{ReadConsistency, WriteConsistency};
pub use crdt_codec::{
    CRDT_CODEC_VERSION, CrdtDataCodec, GCOUNTER_MANIFEST, GCounterCodec,
    GSET_STRING_DELTA_MANIFEST, GSET_STRING_MANIFEST, GSetStringCodec, GSetStringDeltaCodec,
    LWW_REGISTER_STRING_MANIFEST, LWWRegisterStringCodec, ORMAP_STRING_GSET_DELTA_MANIFEST,
    ORMAP_STRING_GSET_MANIFEST, ORMapStringGSetCodec, ORMapStringGSetDeltaCodec,
    ORSET_STRING_DELTA_MANIFEST, ORSET_STRING_MANIFEST, ORSetStringCodec, ORSetStringDeltaCodec,
    PNCOUNTER_MANIFEST, PNCounterCodec, SerializedCrdt,
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
pub use gossip::{
    REPLICATOR_GOSSIP_NOT_FOUND_DIGEST, ReplicatorGossipApplyReport, ReplicatorGossipError,
    ReplicatorGossipStatusPlan, apply_gossip, build_gossip_status, create_gossip, digest_envelope,
    respond_to_gossip_status,
};
pub use gossip_transport::{
    ReplicatorGossipReceiveReport, ReplicatorGossipStatusReceiveReport, ReplicatorGossipTarget,
    ReplicatorGossipTargetRegistry, ReplicatorGossipTickReport, ReplicatorGossipTickSkipReason,
    ReplicatorGossipTransport, ReplicatorGossipTransportFailure, ReplicatorGossipTransportReport,
};
pub use gset::GSet;
pub use key::ReplicatorKey;
pub use lww_register::{LWWRegister, default_lww_clock, reverse_lww_clock};
pub use ormap::{ORMap, ORMapDelta};
pub use orset::{ORSet, ORSetDelta, ORSetRemoveDelta};
pub use pncounter::PNCounter;
pub use protocol::{
    ReplicatorChanged, ReplicatorDataEnvelope, ReplicatorDelta, ReplicatorDeltaAck,
    ReplicatorDeltaNack, ReplicatorDeltaPropagation, ReplicatorGet, ReplicatorGossip,
    ReplicatorGossipDigest, ReplicatorGossipEntry, ReplicatorGossipStatus, ReplicatorPruningEntry,
    ReplicatorPruningState, ReplicatorRead, ReplicatorReadResult, ReplicatorSubscribe,
    ReplicatorUpdate, ReplicatorWrite, ReplicatorWriteAck, ReplicatorWriteNack,
};
pub use pruning::{
    PruningInitialized, PruningPerformed, PruningState, PruningTable, RemovedNodePruningFailure,
    RemovedNodePruningTick, RemovedNodePruningTickReport, RemovedNodePruningTracker,
};
pub use read_write_receive::{DirectReadResult, DirectWriteResult, apply_write, serve_read};
pub use remote_association::{
    ReplicatorRemoteAssociationCacheOutbound, ReplicatorRemoteAssociationError,
    ReplicatorRemoteAssociationOutbound, ReplicatorRemoteAssociationRoutes,
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
pub use remote_tcp::{
    ReplicatorTcpAssociationRuntime, replicator_actor_ref_for, tcp_association_identity_for,
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
pub use tcp_peer_bootstrap::{
    ReplicatorTcpPeerBootstrap, ReplicatorTcpPeerBootstrapError,
    ReplicatorTcpPeerBootstrapIdentity, ReplicatorTcpPeerBootstrapResult,
    ReplicatorTcpPeerBootstrapSettings,
};
pub use tcp_peer_connector::{
    ReplicatorTcpPeerConnector, ReplicatorTcpPeerConnectorMsg, ReplicatorTcpPeerConnectorSettings,
    ReplicatorTcpPeerConnectorSettingsError, ReplicatorTcpPeerConnectorSnapshot,
};
pub use tcp_peer_reconnect::{
    ReplicatorTcpPeerReconnectError, ReplicatorTcpPeerReconnectPending,
    ReplicatorTcpPeerReconnectReport, ReplicatorTcpPeerReconnectResult,
    ReplicatorTcpPeerReconnectSettings, ReplicatorTcpPeerReconnectState,
};
pub use tcp_peer_routes::{
    ReplicatorTcpPeerRouteError, ReplicatorTcpPeerRouteReport, ReplicatorTcpPeerRouteResult,
    ReplicatorTcpPeerRoutes,
};
pub use tcp_peer_runtime::{
    ReplicatorTcpPeerRuntime, ReplicatorTcpPeerRuntimeError, ReplicatorTcpPeerRuntimeResult,
    ReplicatorTcpPeerRuntimeSettings, ReplicatorTcpPeerRuntimeShutdownReport,
};
pub use wire::{
    ReplicatorSerializedMessage, ReplicatorWireCodecs, ReplicatorWireError, ReplicatorWireInbound,
    ReplicatorWireOutbound, ReplicatorWireReplies,
};
