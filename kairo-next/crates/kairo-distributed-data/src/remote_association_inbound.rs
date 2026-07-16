#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use kairo_remote::{
    RemoteAssociationAddress, RemoteEnvelopeHandler, RemoteError, RemoteFrameHandler,
    RemoteStreamId, Result as RemoteResult, decode_remote_envelope_frame,
};
use kairo_serialization::{RemoteEnvelope, RemoteMessage};

use crate::{
    DeltaReplicatedData, ReplicaId, ReplicatorDeltaAck, ReplicatorDeltaNack,
    ReplicatorDeltaPropagation, ReplicatorGossip, ReplicatorGossipStatus, ReplicatorRead,
    ReplicatorReadResult, ReplicatorRemoteReplyError, ReplicatorRemoteReplyInbound,
    ReplicatorRemoteRequestError, ReplicatorRemoteRequestInbound, ReplicatorWrite,
    ReplicatorWriteAck, ReplicatorWriteNack,
};

/// Type-erased receiver for source-attributed replicator request envelopes.
pub trait ReplicatorRemoteRequestReceiver: Send + Sync + 'static {
    /// Receives one request attributed to a cluster-approved logical replica.
    fn receive_request_from(
        &self,
        from: ReplicaId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteRequestError>;
}

impl<D> ReplicatorRemoteRequestReceiver for ReplicatorRemoteRequestInbound<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    fn receive_request_from(
        &self,
        from: ReplicaId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteRequestError> {
        self.receive_from(from, envelope)
    }
}

/// Type-erased receiver for source-attributed replicator reply envelopes.
pub trait ReplicatorRemoteReplyReceiver: Send + Sync + 'static {
    /// Receives one reply attributed to a cluster-approved logical replica.
    fn receive_reply_from(
        &self,
        from: ReplicaId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteReplyError>;
}

/// Shared map from current cluster-member transport addresses to logical replica identities.
///
/// Cluster snapshots and events own this map. Association handshakes and envelope metadata cannot
/// add membership or source identities to it.
pub type ReplicatorRemoteSourceMap = Arc<Mutex<BTreeMap<RemoteAssociationAddress, ReplicaId>>>;

impl ReplicatorRemoteReplyReceiver for ReplicatorRemoteReplyInbound {
    fn receive_reply_from(
        &self,
        from: ReplicaId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        self.receive_from(from, envelope)
    }
}

#[derive(Debug)]
/// Failure while identifying, classifying, or dispatching an inbound replicator envelope.
pub enum ReplicatorRemoteAssociationInboundError {
    /// Shared-runtime traffic omitted the sender actor reference used for source attribution.
    MissingSender,
    /// The sender actor reference could not form a canonical remote address.
    InvalidSender {
        /// Address parsing or validation diagnostic.
        reason: String,
    },
    /// The sender address is not mapped to a current cluster replica.
    UnknownSource {
        /// Canonical sender address absent from the cluster-maintained map.
        address: RemoteAssociationAddress,
    },
    /// Ordinary distributed-data traffic arrived on the control lane.
    UnexpectedControlLane {
        /// Stable manifest received on the wrong lane.
        manifest: String,
    },
    /// The stable manifest belongs to neither the request nor reply protocol set.
    UnsupportedManifest {
        /// Unrecognized stable manifest.
        manifest: String,
    },
    /// Request validation, decoding, or local dispatch failed.
    Request(ReplicatorRemoteRequestError),
    /// Reply decoding or local aggregation delivery failed.
    Reply(ReplicatorRemoteReplyError),
}

impl Display for ReplicatorRemoteAssociationInboundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSender => write!(f, "distributed-data remote envelope has no sender"),
            Self::InvalidSender { reason } => {
                write!(f, "distributed-data remote sender is invalid: {reason}")
            }
            Self::UnknownSource { address } => write!(
                f,
                "distributed-data remote sender `{address}` is not a cluster replica"
            ),
            Self::UnexpectedControlLane { manifest } => {
                write!(
                    f,
                    "distributed-data remote manifest `{manifest}` arrived on the control lane"
                )
            }
            Self::UnsupportedManifest { manifest } => {
                write!(
                    f,
                    "unsupported remote replicator association manifest `{manifest}`"
                )
            }
            Self::Request(error) => write!(f, "{error}"),
            Self::Reply(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ReplicatorRemoteAssociationInboundError {}

#[derive(Clone)]
/// Per-association router for ordinary-lane distributed-data request and reply manifests.
///
/// A configured standalone association may use a fixed or fallback replica identity. The composed
/// ActorSystem path instead uses [`ReplicatorRemoteSystemInbound`] for strict cluster-derived
/// source attribution.
pub struct ReplicatorRemoteAssociationInbound {
    source: ReplicatorRemoteAssociationSource,
    requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
    replies: Arc<dyn ReplicatorRemoteReplyReceiver>,
}

#[derive(Clone)]
enum ReplicatorRemoteAssociationSource {
    Fixed(ReplicaId),
    Address {
        address: RemoteAssociationAddress,
        replicas: ReplicatorRemoteSourceMap,
        fallback: ReplicaId,
    },
}

impl ReplicatorRemoteAssociationSource {
    fn replica(&self) -> ReplicaId {
        match self {
            Self::Fixed(replica) => replica.clone(),
            Self::Address {
                address,
                replicas,
                fallback,
            } => replicas
                .lock()
                .expect("replicator remote source map lock poisoned")
                .get(address)
                .cloned()
                .unwrap_or_else(|| fallback.clone()),
        }
    }
}

impl ReplicatorRemoteAssociationInbound {
    /// Creates an association router with one fixed source replica identity.
    pub fn new(
        from: ReplicaId,
        requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
        replies: Arc<dyn ReplicatorRemoteReplyReceiver>,
    ) -> Self {
        Self {
            source: ReplicatorRemoteAssociationSource::Fixed(from),
            requests,
            replies,
        }
    }

    /// Returns the fixed identity or configured fallback identity.
    ///
    /// For address-derived routing this is not necessarily the identity selected for a particular
    /// envelope; current source-map entries take precedence during dispatch.
    pub fn from(&self) -> &ReplicaId {
        match &self.source {
            ReplicatorRemoteAssociationSource::Fixed(from) => from,
            ReplicatorRemoteAssociationSource::Address { fallback, .. } => fallback,
        }
    }

    /// Creates an association router that resolves a source address through a shared replica map.
    ///
    /// `fallback` is retained for the configured standalone runtime when the address is not mapped.
    pub fn from_address(
        address: RemoteAssociationAddress,
        replicas: ReplicatorRemoteSourceMap,
        fallback: ReplicaId,
        requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
        replies: Arc<dyn ReplicatorRemoteReplyReceiver>,
    ) -> Self {
        Self {
            source: ReplicatorRemoteAssociationSource::Address {
                address,
                replicas,
                fallback,
            },
            requests,
            replies,
        }
    }

    /// Classifies and dispatches one decoded association envelope.
    ///
    /// Distributed-data manifests are rejected on the control lane.
    pub fn receive_envelope(
        &self,
        stream_id: RemoteStreamId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteAssociationInboundError> {
        dispatch_replicator_envelope(
            self.source.replica(),
            stream_id,
            envelope,
            self.requests.as_ref(),
            self.replies.as_ref(),
        )
    }
}

/// Shared-runtime inbound router for the stable `/system/ddata` manifests.
///
/// Unlike [`ReplicatorRemoteAssociationInbound`], this handler is shared by
/// every ordinary-lane association. It therefore resolves the source replica
/// from the envelope sender through a cluster-maintained address map.
#[derive(Clone)]
pub struct ReplicatorRemoteSystemInbound {
    replicas: ReplicatorRemoteSourceMap,
    requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
    replies: Arc<dyn ReplicatorRemoteReplyReceiver>,
}

impl ReplicatorRemoteSystemInbound {
    /// Creates shared ingress using only cluster-maintained source identities.
    pub fn new(
        replicas: ReplicatorRemoteSourceMap,
        requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
        replies: Arc<dyn ReplicatorRemoteReplyReceiver>,
    ) -> Self {
        Self {
            replicas,
            requests,
            replies,
        }
    }

    /// Resolves the sender through the current source map and dispatches an ordinary-lane envelope.
    pub fn receive_envelope(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteAssociationInboundError> {
        let from = self.source_replica(&envelope)?;
        dispatch_replicator_envelope(
            from,
            RemoteStreamId::Ordinary,
            envelope,
            self.requests.as_ref(),
            self.replies.as_ref(),
        )
    }

    fn source_replica(
        &self,
        envelope: &RemoteEnvelope,
    ) -> Result<ReplicaId, ReplicatorRemoteAssociationInboundError> {
        let sender = envelope
            .sender
            .as_ref()
            .ok_or(ReplicatorRemoteAssociationInboundError::MissingSender)?;
        let host = sender.host().ok_or_else(|| {
            ReplicatorRemoteAssociationInboundError::InvalidSender {
                reason: "sender has no remote host".to_string(),
            }
        })?;
        let address =
            RemoteAssociationAddress::new(sender.protocol(), sender.system(), host, sender.port())
                .map_err(
                    |error| ReplicatorRemoteAssociationInboundError::InvalidSender {
                        reason: error.to_string(),
                    },
                )?;
        self.replicas
            .lock()
            .expect("replicator remote source map lock poisoned")
            .get(&address)
            .cloned()
            .ok_or(ReplicatorRemoteAssociationInboundError::UnknownSource { address })
    }
}

fn dispatch_replicator_envelope(
    from: ReplicaId,
    stream_id: RemoteStreamId,
    envelope: RemoteEnvelope,
    requests: &dyn ReplicatorRemoteRequestReceiver,
    replies: &dyn ReplicatorRemoteReplyReceiver,
) -> Result<(), ReplicatorRemoteAssociationInboundError> {
    let manifest = envelope.message.manifest.as_str();
    if stream_id == RemoteStreamId::Control {
        return Err(
            ReplicatorRemoteAssociationInboundError::UnexpectedControlLane {
                manifest: manifest.to_string(),
            },
        );
    }
    if is_replicator_request_manifest(manifest) {
        requests
            .receive_request_from(from, envelope)
            .map_err(ReplicatorRemoteAssociationInboundError::Request)
    } else if is_replicator_reply_manifest(manifest) {
        replies
            .receive_reply_from(from, envelope)
            .map_err(ReplicatorRemoteAssociationInboundError::Reply)
    } else {
        Err(
            ReplicatorRemoteAssociationInboundError::UnsupportedManifest {
                manifest: manifest.to_string(),
            },
        )
    }
}

impl RemoteFrameHandler for ReplicatorRemoteAssociationInbound {
    fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> RemoteResult<()> {
        let envelope = decode_remote_envelope_frame(frame)?;
        self.receive_envelope(stream_id, envelope)
            .map_err(|error| RemoteError::Inbound(error.to_string()))
    }
}

impl RemoteEnvelopeHandler for ReplicatorRemoteSystemInbound {
    fn receive(&self, envelope: RemoteEnvelope) -> RemoteResult<()> {
        self.receive_envelope(envelope)
            .map_err(|error| RemoteError::Inbound(error.to_string()))
    }
}

/// Returns whether `manifest` is one of the five stable replicator request manifests.
pub fn is_replicator_request_manifest(manifest: &str) -> bool {
    matches!(
        manifest,
        ReplicatorDeltaPropagation::MANIFEST
            | ReplicatorWrite::MANIFEST
            | ReplicatorRead::MANIFEST
            | ReplicatorGossipStatus::MANIFEST
            | ReplicatorGossip::MANIFEST
    )
}

/// Returns whether `manifest` is one of the five stable replicator reply manifests.
pub fn is_replicator_reply_manifest(manifest: &str) -> bool {
    matches!(
        manifest,
        ReplicatorDeltaAck::MANIFEST
            | ReplicatorDeltaNack::MANIFEST
            | ReplicatorWriteAck::MANIFEST
            | ReplicatorWriteNack::MANIFEST
            | ReplicatorReadResult::MANIFEST
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use kairo_remote::{
        AssociationRemoteInbound, RemoteByteSink, RemoteStreamWriter, encode_remote_envelope_frame,
    };
    use kairo_serialization::{ActorRefWireData, Manifest, SerializedMessage};

    use super::*;
    use crate::{
        REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID, REPLICATOR_READ_RESULT_SERIALIZER_ID,
        REPLICATOR_READ_SERIALIZER_ID,
    };

    #[derive(Default)]
    struct RecordingRequests {
        received: Mutex<Vec<(ReplicaId, RemoteEnvelope)>>,
    }

    impl RecordingRequests {
        fn received(&self) -> Vec<(ReplicaId, RemoteEnvelope)> {
            self.received
                .lock()
                .expect("recording requests poisoned")
                .clone()
        }
    }

    impl ReplicatorRemoteRequestReceiver for RecordingRequests {
        fn receive_request_from(
            &self,
            from: ReplicaId,
            envelope: RemoteEnvelope,
        ) -> Result<(), ReplicatorRemoteRequestError> {
            self.received
                .lock()
                .expect("recording requests poisoned")
                .push((from, envelope));
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingReplies {
        received: Mutex<Vec<(ReplicaId, RemoteEnvelope)>>,
    }

    impl RecordingReplies {
        fn received(&self) -> Vec<(ReplicaId, RemoteEnvelope)> {
            self.received
                .lock()
                .expect("recording replies poisoned")
                .clone()
        }
    }

    impl ReplicatorRemoteReplyReceiver for RecordingReplies {
        fn receive_reply_from(
            &self,
            from: ReplicaId,
            envelope: RemoteEnvelope,
        ) -> Result<(), ReplicatorRemoteReplyError> {
            self.received
                .lock()
                .expect("recording replies poisoned")
                .push((from, envelope));
            Ok(())
        }
    }

    struct InboundByteSink {
        inbound: Mutex<AssociationRemoteInbound<ReplicatorRead>>,
    }

    impl InboundByteSink {
        fn new(inbound: AssociationRemoteInbound<ReplicatorRead>) -> Self {
            Self {
                inbound: Mutex::new(inbound),
            }
        }
    }

    impl RemoteByteSink for InboundByteSink {
        fn send_bytes(&self, bytes: Bytes) -> RemoteResult<()> {
            self.inbound
                .lock()
                .expect("association inbound poisoned")
                .push_ordinary_bytes(bytes)?;
            Ok(())
        }
    }

    fn wire_ref(path: &str) -> ActorRefWireData {
        ActorRefWireData::new(path).unwrap()
    }

    fn envelope(manifest: &'static str, serializer_id: u32) -> RemoteEnvelope {
        RemoteEnvelope::new(
            wire_ref("kairo://local@127.0.0.1:25521/system/ddata"),
            Some(wire_ref("kairo://remote@127.0.0.1:25520/system/ddata")),
            SerializedMessage::new(serializer_id, Manifest::new(manifest), 1, Bytes::new()),
        )
    }

    fn inbound(
        from: ReplicaId,
        requests: Arc<RecordingRequests>,
        replies: Arc<RecordingReplies>,
    ) -> ReplicatorRemoteAssociationInbound {
        ReplicatorRemoteAssociationInbound::new(
            from,
            requests as Arc<dyn ReplicatorRemoteRequestReceiver>,
            replies as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )
    }

    #[test]
    fn inbound_dispatches_request_manifests_to_request_receiver() {
        let requests = Arc::new(RecordingRequests::default());
        let replies = Arc::new(RecordingReplies::default());
        let inbound = inbound(ReplicaId::new("remote"), requests.clone(), replies.clone());

        inbound
            .receive_envelope(
                RemoteStreamId::Ordinary,
                envelope(
                    ReplicatorDeltaPropagation::MANIFEST,
                    REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID,
                ),
            )
            .unwrap();

        let received = requests.received();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0, ReplicaId::new("remote"));
        assert_eq!(
            received[0].1.message.manifest.as_str(),
            ReplicatorDeltaPropagation::MANIFEST
        );
        assert!(replies.received().is_empty());
    }

    #[test]
    fn sender_derived_inbound_accepts_only_cluster_mapped_replica_addresses() {
        let requests = Arc::new(RecordingRequests::default());
        let replies = Arc::new(RecordingReplies::default());
        let address =
            RemoteAssociationAddress::new("kairo", "remote", "127.0.0.1", Some(25520)).unwrap();
        let replicas = ReplicatorRemoteSourceMap::default();
        replicas
            .lock()
            .expect("replicator remote source map poisoned")
            .insert(address, ReplicaId::new("remote-uid"));
        let inbound =
            ReplicatorRemoteSystemInbound::new(replicas, requests.clone(), replies.clone());

        inbound
            .receive_envelope(envelope(
                ReplicatorDeltaPropagation::MANIFEST,
                REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID,
            ))
            .unwrap();

        assert_eq!(requests.received()[0].0, ReplicaId::new("remote-uid"));
        assert!(replies.received().is_empty());
    }

    #[test]
    fn sender_derived_inbound_rejects_non_member_replica_addresses() {
        let inbound = ReplicatorRemoteSystemInbound::new(
            ReplicatorRemoteSourceMap::default(),
            Arc::new(RecordingRequests::default()),
            Arc::new(RecordingReplies::default()),
        );

        let error = inbound
            .receive_envelope(envelope(
                ReplicatorRead::MANIFEST,
                REPLICATOR_READ_SERIALIZER_ID,
            ))
            .expect_err("non-member sender should be rejected");

        assert!(matches!(
            error,
            ReplicatorRemoteAssociationInboundError::UnknownSource { .. }
        ));
    }

    #[test]
    fn inbound_dispatches_reply_manifests_to_reply_receiver() {
        let requests = Arc::new(RecordingRequests::default());
        let replies = Arc::new(RecordingReplies::default());
        let inbound = inbound(ReplicaId::new("remote"), requests.clone(), replies.clone());

        inbound
            .receive_envelope(
                RemoteStreamId::Large,
                envelope(
                    ReplicatorReadResult::MANIFEST,
                    REPLICATOR_READ_RESULT_SERIALIZER_ID,
                ),
            )
            .unwrap();

        let received = replies.received();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0, ReplicaId::new("remote"));
        assert_eq!(
            received[0].1.message.manifest.as_str(),
            ReplicatorReadResult::MANIFEST
        );
        assert!(requests.received().is_empty());
    }

    #[test]
    fn inbound_rejects_unknown_manifest() {
        let inbound = inbound(
            ReplicaId::new("remote"),
            Arc::new(RecordingRequests::default()),
            Arc::new(RecordingReplies::default()),
        );

        let error = inbound
            .receive_envelope(
                RemoteStreamId::Ordinary,
                envelope("kairo.ddata.unknown", 9999),
            )
            .expect_err("unknown manifest should be rejected");

        assert!(matches!(
            error,
            ReplicatorRemoteAssociationInboundError::UnsupportedManifest { .. }
        ));
    }

    #[test]
    fn inbound_rejects_replicator_messages_on_control_lane() {
        let inbound = inbound(
            ReplicaId::new("remote"),
            Arc::new(RecordingRequests::default()),
            Arc::new(RecordingReplies::default()),
        );

        let error = inbound
            .receive_envelope(
                RemoteStreamId::Control,
                envelope(ReplicatorRead::MANIFEST, REPLICATOR_READ_SERIALIZER_ID),
            )
            .expect_err("replicator traffic should not arrive on the control lane");

        assert!(matches!(
            error,
            ReplicatorRemoteAssociationInboundError::UnexpectedControlLane { .. }
        ));
    }

    #[test]
    fn inbound_handles_frames_from_association_streams() {
        let requests = Arc::new(RecordingRequests::default());
        let replies = Arc::new(RecordingReplies::default());
        let handler = inbound(ReplicaId::new("remote"), requests.clone(), replies.clone());
        let association_inbound =
            AssociationRemoteInbound::<ReplicatorRead>::from_handler(Arc::new(handler));
        let sink = InboundByteSink::new(association_inbound);
        let writer = RemoteStreamWriter::new(RemoteStreamId::Ordinary, Arc::new(sink));
        let frame = encode_remote_envelope_frame(&envelope(
            ReplicatorReadResult::MANIFEST,
            REPLICATOR_READ_RESULT_SERIALIZER_ID,
        ))
        .unwrap();

        writer.send_frame_payload(frame).unwrap();

        let received = replies.received();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0, ReplicaId::new("remote"));
        assert_eq!(requests.received().len(), 0);
    }
}
