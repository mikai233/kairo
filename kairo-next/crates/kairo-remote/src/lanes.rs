#![deny(missing_docs)]

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use kairo_serialization::{RemoteEnvelope, RemoteMessage};

use crate::{
    AddressTerminated, RemoteError, RemoteHeartbeat, RemoteHeartbeatAck, RemoteOutbound,
    RemoteTerminated, Result, UnwatchRemote, WatchRemote, encode_remote_envelope_frame,
    stream::RemoteStreamId,
};

/// Classifies encoded remote envelopes into control, ordinary, or large
/// transport lanes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteLaneClassifier {
    control_manifests: BTreeSet<String>,
    large_frame_threshold: Option<usize>,
}

impl RemoteLaneClassifier {
    /// Creates a classifier with the built-in remote death-watch manifests on
    /// the control lane and no large-frame threshold.
    pub fn new() -> Self {
        let mut classifier = Self {
            control_manifests: BTreeSet::new(),
            large_frame_threshold: None,
        };
        classifier.add_control_manifest(WatchRemote::MANIFEST);
        classifier.add_control_manifest(UnwatchRemote::MANIFEST);
        classifier.add_control_manifest(RemoteTerminated::MANIFEST);
        classifier.add_control_manifest(RemoteHeartbeat::MANIFEST);
        classifier.add_control_manifest(RemoteHeartbeatAck::MANIFEST);
        classifier.add_control_manifest(AddressTerminated::MANIFEST);
        classifier
    }

    /// Routes non-control frames of at least `threshold` encoded bytes to the
    /// large lane.
    pub fn with_large_frame_threshold(mut self, threshold: usize) -> Self {
        self.large_frame_threshold = Some(threshold);
        self
    }

    /// Adds a stable manifest to the control-lane set.
    pub fn add_control_manifest(&mut self, manifest: impl Into<String>) {
        self.control_manifests.insert(manifest.into());
    }

    /// Selects the lane for an envelope with the supplied encoded length.
    ///
    /// Control manifests take precedence over the large-frame threshold.
    pub fn classify(&self, envelope: &RemoteEnvelope, encoded_len: usize) -> RemoteStreamId {
        if self
            .control_manifests
            .contains(envelope.message.manifest.as_str())
        {
            RemoteStreamId::Control
        } else if self
            .large_frame_threshold
            .is_some_and(|threshold| encoded_len >= threshold)
        {
            RemoteStreamId::Large
        } else {
            RemoteStreamId::Ordinary
        }
    }
}

impl Default for RemoteLaneClassifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Sink for a complete encoded envelope frame assigned to a transport lane.
pub trait RemoteLaneSink: Send + Sync + 'static {
    /// Sends one encoded frame on `lane`.
    fn send_lane_frame(&self, lane: RemoteStreamId, frame: Bytes) -> Result<()>;
}

impl<F> RemoteLaneSink for F
where
    F: Fn(RemoteStreamId, Bytes) -> Result<()> + Send + Sync + 'static,
{
    fn send_lane_frame(&self, lane: RemoteStreamId, frame: Bytes) -> Result<()> {
        self(lane, frame)
    }
}

#[derive(Clone)]
/// Encodes remote envelopes, classifies them, and sends them to a lane sink.
pub struct LaneRemoteOutbound {
    classifier: RemoteLaneClassifier,
    sink: Arc<dyn RemoteLaneSink>,
}

impl LaneRemoteOutbound {
    /// Creates a lane-aware outbound with an explicit classifier and sink.
    pub fn new(classifier: RemoteLaneClassifier, sink: Arc<dyn RemoteLaneSink>) -> Self {
        Self { classifier, sink }
    }

    /// Returns the lane classifier.
    pub fn classifier(&self) -> &RemoteLaneClassifier {
        &self.classifier
    }

    /// Returns the encoded-frame lane sink.
    pub fn sink(&self) -> &Arc<dyn RemoteLaneSink> {
        &self.sink
    }
}

impl RemoteOutbound for LaneRemoteOutbound {
    fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
        let frame = encode_remote_envelope_frame(&envelope)?;
        let lane = self.classifier.classify(&envelope, frame.len());
        self.sink.send_lane_frame(lane, frame)
    }
}

impl From<Arc<dyn RemoteLaneSink>> for LaneRemoteOutbound {
    fn from(sink: Arc<dyn RemoteLaneSink>) -> Self {
        Self::new(RemoteLaneClassifier::default(), sink)
    }
}

/// Creates an outbound error annotated with the failed lane.
pub fn lane_send_failure(lane: RemoteStreamId, reason: impl Into<String>) -> RemoteError {
    RemoteError::Outbound(format!(
        "remote {:?} lane delivery failed: {}",
        lane,
        reason.into()
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use kairo_serialization::{
        ActorRefWireData, Manifest, MessageCodec, Registry, RemoteMessage, SerializationRegistry,
    };

    use super::*;
    use crate::{decode_remote_envelope_frame, register_remote_protocol_codecs};

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Business {
        value: u8,
    }

    impl RemoteMessage for Business {
        const MANIFEST: &'static str = "kairo.remote.test.Business";
        const VERSION: u16 = 1;
    }

    struct BusinessCodec;

    impl MessageCodec<Business> for BusinessCodec {
        fn serializer_id(&self) -> u32 {
            151
        }

        fn encode(&self, message: &Business) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(vec![message.value]))
        }

        fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Business> {
            Ok(Business { value: payload[0] })
        }
    }

    #[derive(Default)]
    struct CollectingLaneSink {
        sent: Mutex<Vec<(RemoteStreamId, Bytes)>>,
        fail_with: Mutex<Option<String>>,
    }

    impl CollectingLaneSink {
        fn sent(&self) -> Vec<(RemoteStreamId, Bytes)> {
            self.sent.lock().expect("lane sink poisoned").clone()
        }

        fn fail(&self, reason: impl Into<String>) {
            *self.fail_with.lock().expect("lane sink poisoned") = Some(reason.into());
        }
    }

    impl RemoteLaneSink for CollectingLaneSink {
        fn send_lane_frame(&self, lane: RemoteStreamId, frame: Bytes) -> Result<()> {
            if let Some(reason) = self.fail_with.lock().expect("lane sink poisoned").clone() {
                return Err(lane_send_failure(lane, reason));
            }
            self.sent
                .lock()
                .expect("lane sink poisoned")
                .push((lane, frame));
            Ok(())
        }
    }

    fn registry() -> Registry {
        let mut registry = Registry::new();
        registry.register::<Business, _>(BusinessCodec).unwrap();
        register_remote_protocol_codecs(&mut registry).unwrap();
        registry
    }

    fn business_envelope(value: u8) -> RemoteEnvelope {
        let registry = registry();
        RemoteEnvelope::new(
            ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/target").unwrap(),
            None,
            registry.serialize(&Business { value }).unwrap(),
        )
    }

    fn watch_envelope() -> RemoteEnvelope {
        let registry = registry();
        let watchee = ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/target").unwrap();
        let watcher = ActorRefWireData::new("kairo://local@127.0.0.1:25521/user/watcher").unwrap();
        RemoteEnvelope::new(
            watchee.clone(),
            Some(watcher.clone()),
            registry
                .serialize(&WatchRemote { watchee, watcher })
                .unwrap(),
        )
    }

    fn remote_terminated_envelope() -> RemoteEnvelope {
        let registry = registry();
        let watchee = ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/target").unwrap();
        RemoteEnvelope::new(
            ActorRefWireData::new("kairo://local@127.0.0.1:25521/system/remote-watch").unwrap(),
            Some(
                ActorRefWireData::new("kairo://remote@127.0.0.1:25520/system/remote-watch")
                    .unwrap(),
            ),
            registry
                .serialize(&RemoteTerminated {
                    watchee,
                    existence_confirmed: true,
                })
                .unwrap(),
        )
    }

    #[test]
    fn classifier_routes_remote_system_manifests_to_control_lane() {
        let envelope = watch_envelope();
        let frame = encode_remote_envelope_frame(&envelope).unwrap();

        assert_eq!(
            RemoteLaneClassifier::default().classify(&envelope, frame.len()),
            RemoteStreamId::Control
        );

        let envelope = remote_terminated_envelope();
        let frame = encode_remote_envelope_frame(&envelope).unwrap();
        assert_eq!(
            RemoteLaneClassifier::default().classify(&envelope, frame.len()),
            RemoteStreamId::Control
        );
    }

    #[test]
    fn classifier_routes_business_messages_to_ordinary_lane() {
        let envelope = business_envelope(7);
        let frame = encode_remote_envelope_frame(&envelope).unwrap();

        assert_eq!(
            RemoteLaneClassifier::default().classify(&envelope, frame.len()),
            RemoteStreamId::Ordinary
        );
    }

    #[test]
    fn classifier_can_route_large_frames_to_large_lane() {
        let envelope = business_envelope(9);
        let frame = encode_remote_envelope_frame(&envelope).unwrap();
        let classifier = RemoteLaneClassifier::default().with_large_frame_threshold(frame.len());

        assert_eq!(
            classifier.classify(&envelope, frame.len()),
            RemoteStreamId::Large
        );
    }

    #[test]
    fn lane_outbound_encodes_and_sends_to_selected_lane() {
        let sink = Arc::new(CollectingLaneSink::default());
        let outbound = LaneRemoteOutbound::new(
            RemoteLaneClassifier::default(),
            sink.clone() as Arc<dyn RemoteLaneSink>,
        );

        outbound.send(watch_envelope()).unwrap();
        outbound.send(business_envelope(13)).unwrap();

        let sent = sink.sent();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].0, RemoteStreamId::Control);
        assert_eq!(sent[1].0, RemoteStreamId::Ordinary);
        let decoded = decode_remote_envelope_frame(sent[1].1.clone()).unwrap();
        assert_eq!(decoded.message.manifest.as_str(), Business::MANIFEST);
        assert_eq!(decoded.message.payload, Bytes::from_static(&[13]));
    }

    #[test]
    fn lane_outbound_propagates_sink_failure() {
        let sink = Arc::new(CollectingLaneSink::default());
        sink.fail("queue full");
        let outbound = LaneRemoteOutbound::new(
            RemoteLaneClassifier::default(),
            sink as Arc<dyn RemoteLaneSink>,
        );

        let error = outbound
            .send(business_envelope(1))
            .expect_err("lane sink failure should propagate");

        assert!(matches!(error, RemoteError::Outbound(_)));
        assert!(error.to_string().contains("Ordinary"));
        assert!(error.to_string().contains("queue full"));
    }

    #[test]
    fn classifier_can_register_additional_control_manifests() {
        let envelope = RemoteEnvelope::new(
            ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/target").unwrap(),
            None,
            kairo_serialization::SerializedMessage::new(
                999,
                Manifest::new("kairo.remote.test.ExtraControl"),
                1,
                Bytes::new(),
            ),
        );
        let mut classifier = RemoteLaneClassifier::default();
        classifier.add_control_manifest("kairo.remote.test.ExtraControl");

        assert_eq!(classifier.classify(&envelope, 0), RemoteStreamId::Control);
    }
}
