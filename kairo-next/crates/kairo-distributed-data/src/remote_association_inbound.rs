use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use bytes::Bytes;
use kairo_remote::{
    RemoteError, RemoteFrameHandler, RemoteStreamId, Result as RemoteResult,
    decode_remote_envelope_frame,
};
use kairo_serialization::{RemoteEnvelope, RemoteMessage};

use crate::{
    DeltaReplicatedData, ReplicaId, ReplicatorDeltaAck, ReplicatorDeltaNack,
    ReplicatorDeltaPropagation, ReplicatorRead, ReplicatorReadResult, ReplicatorRemoteReplyError,
    ReplicatorRemoteReplyInbound, ReplicatorRemoteRequestError, ReplicatorRemoteRequestInbound,
    ReplicatorWrite, ReplicatorWriteAck, ReplicatorWriteNack,
};

pub trait ReplicatorRemoteRequestReceiver: Send + Sync + 'static {
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

pub trait ReplicatorRemoteReplyReceiver: Send + Sync + 'static {
    fn receive_reply_from(
        &self,
        from: ReplicaId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteReplyError>;
}

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
pub enum ReplicatorRemoteAssociationInboundError {
    UnexpectedControlLane { manifest: String },
    UnsupportedManifest { manifest: String },
    Request(ReplicatorRemoteRequestError),
    Reply(ReplicatorRemoteReplyError),
}

impl Display for ReplicatorRemoteAssociationInboundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
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
pub struct ReplicatorRemoteAssociationInbound {
    from: ReplicaId,
    requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
    replies: Arc<dyn ReplicatorRemoteReplyReceiver>,
}

impl ReplicatorRemoteAssociationInbound {
    pub fn new(
        from: ReplicaId,
        requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
        replies: Arc<dyn ReplicatorRemoteReplyReceiver>,
    ) -> Self {
        Self {
            from,
            requests,
            replies,
        }
    }

    pub fn from(&self) -> &ReplicaId {
        &self.from
    }

    pub fn receive_envelope(
        &self,
        stream_id: RemoteStreamId,
        envelope: RemoteEnvelope,
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
            self.requests
                .receive_request_from(self.from.clone(), envelope)
                .map_err(ReplicatorRemoteAssociationInboundError::Request)
        } else if is_replicator_reply_manifest(manifest) {
            self.replies
                .receive_reply_from(self.from.clone(), envelope)
                .map_err(ReplicatorRemoteAssociationInboundError::Reply)
        } else {
            Err(
                ReplicatorRemoteAssociationInboundError::UnsupportedManifest {
                    manifest: manifest.to_string(),
                },
            )
        }
    }
}

impl RemoteFrameHandler for ReplicatorRemoteAssociationInbound {
    fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> RemoteResult<()> {
        let envelope = decode_remote_envelope_frame(frame)?;
        self.receive_envelope(stream_id, envelope)
            .map_err(|error| RemoteError::Inbound(error.to_string()))
    }
}

pub fn is_replicator_request_manifest(manifest: &str) -> bool {
    matches!(
        manifest,
        ReplicatorDeltaPropagation::MANIFEST | ReplicatorWrite::MANIFEST | ReplicatorRead::MANIFEST
    )
}

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
