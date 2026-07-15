#![deny(missing_docs)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use kairo_serialization::RemoteEnvelope;

use crate::{
    AssociationRemoteOutbound, AssociationState, LaneRemoteOutbound, QueuedRemoteByteSink,
    RemoteAssociation, RemoteByteSink, RemoteError, RemoteLaneClassifier, RemoteLaneSink,
    RemoteOutbound, RemoteOutboundQueueSettings, RemoteStreamId, Result, StreamLaneSink,
};

#[derive(Clone)]
/// Complete outbound association path from lifecycle guard through envelope
/// framing, lane classification, stream framing, and byte sinks.
pub struct AssociationOutboundPipeline {
    association: Arc<Mutex<RemoteAssociation>>,
    stream_sink: Arc<StreamLaneSink>,
    lane_outbound: Arc<LaneRemoteOutbound>,
    outbound: AssociationRemoteOutbound,
}

impl AssociationOutboundPipeline {
    /// Creates a direct-writer pipeline with a new idle association.
    pub fn new(
        remote_address: impl Into<String>,
        classifier: RemoteLaneClassifier,
        control: Arc<dyn RemoteByteSink>,
        ordinary: Arc<dyn RemoteByteSink>,
        large: Arc<dyn RemoteByteSink>,
    ) -> Self {
        Self::from_association(
            RemoteAssociation::new(remote_address),
            classifier,
            control,
            ordinary,
            large,
        )
    }

    /// Creates a direct-writer pipeline from owned association state.
    pub fn from_association(
        association: RemoteAssociation,
        classifier: RemoteLaneClassifier,
        control: Arc<dyn RemoteByteSink>,
        ordinary: Arc<dyn RemoteByteSink>,
        large: Arc<dyn RemoteByteSink>,
    ) -> Self {
        Self::shared(
            Arc::new(Mutex::new(association)),
            classifier,
            control,
            ordinary,
            large,
        )
    }

    /// Creates a direct-writer pipeline using existing shared association
    /// state.
    pub fn shared(
        association: Arc<Mutex<RemoteAssociation>>,
        classifier: RemoteLaneClassifier,
        control: Arc<dyn RemoteByteSink>,
        ordinary: Arc<dyn RemoteByteSink>,
        large: Arc<dyn RemoteByteSink>,
    ) -> Self {
        let stream_sink = Arc::new(StreamLaneSink::new(control, ordinary, large));
        let lane_outbound = Arc::new(LaneRemoteOutbound::new(
            classifier,
            stream_sink.clone() as Arc<dyn RemoteLaneSink>,
        ));
        let outbound = AssociationRemoteOutbound::shared(
            association.clone(),
            lane_outbound.clone() as Arc<dyn RemoteOutbound>,
        );

        Self {
            association,
            stream_sink,
            lane_outbound,
            outbound,
        }
    }

    /// Creates a pipeline with one bounded non-blocking writer queue per lane.
    ///
    /// The first background writer failure closes the association and all three
    /// underlying lane sinks. A control-lane queue overflow quarantines the
    /// current remote incarnation synchronously.
    pub fn shared_queued(
        association: Arc<Mutex<RemoteAssociation>>,
        classifier: RemoteLaneClassifier,
        queue_settings: RemoteOutboundQueueSettings,
        control: Arc<dyn RemoteByteSink>,
        ordinary: Arc<dyn RemoteByteSink>,
        large: Arc<dyn RemoteByteSink>,
    ) -> Result<Self> {
        let failure_recorded = Arc::new(AtomicBool::new(false));
        let failure_association = association.clone();
        let failure_sinks = [control.clone(), ordinary.clone(), large.clone()];
        let failure_handler = Arc::new(move |lane: RemoteStreamId, reason: String| {
            if failure_recorded.swap(true, Ordering::AcqRel) {
                return;
            }
            let close_reason = format!("{lane:?} lane writer failed: {reason}");
            failure_association
                .lock()
                .expect("remote association lock poisoned")
                .close(close_reason);
            for sink in &failure_sinks {
                let _ = sink.close();
            }
        });
        let control = Arc::new(QueuedRemoteByteSink::new_with_failure_handler(
            RemoteStreamId::Control,
            queue_settings.capacity_for(RemoteStreamId::Control),
            control,
            Some(failure_handler.clone()),
        )?) as Arc<dyn RemoteByteSink>;
        let ordinary = Arc::new(QueuedRemoteByteSink::new_with_failure_handler(
            RemoteStreamId::Ordinary,
            queue_settings.capacity_for(RemoteStreamId::Ordinary),
            ordinary,
            Some(failure_handler.clone()),
        )?) as Arc<dyn RemoteByteSink>;
        let large = Arc::new(QueuedRemoteByteSink::new_with_failure_handler(
            RemoteStreamId::Large,
            queue_settings.capacity_for(RemoteStreamId::Large),
            large,
            Some(failure_handler),
        )?) as Arc<dyn RemoteByteSink>;
        Ok(Self::shared(
            association,
            classifier,
            control,
            ordinary,
            large,
        ))
    }

    /// Returns the shared association lifecycle state.
    pub fn association(&self) -> &Arc<Mutex<RemoteAssociation>> {
        &self.association
    }

    /// Returns the three-lane stream sink.
    pub fn stream_sink(&self) -> &Arc<StreamLaneSink> {
        &self.stream_sink
    }

    /// Returns the envelope framing and lane-classification outbound.
    pub fn lane_outbound(&self) -> &Arc<LaneRemoteOutbound> {
        &self.lane_outbound
    }

    /// Returns the association-state-guarded outbound.
    pub fn guarded_outbound(&self) -> &AssociationRemoteOutbound {
        &self.outbound
    }

    /// Closes the association and all three stream writers.
    ///
    /// The association retains its first terminal reason. All writers are still
    /// asked to close, and the first close failure is returned.
    pub fn close(&self, reason: impl Into<String>) -> Result<()> {
        self.association
            .lock()
            .expect("remote association lock poisoned")
            .close(reason);
        self.stream_sink.close()
    }
}

impl RemoteOutbound for AssociationOutboundPipeline {
    fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
        let result = self.outbound.send(envelope);
        if matches!(
            result,
            Err(RemoteError::OutboundLaneQueueFull { ref lane, .. }) if lane == "control"
        ) {
            let mut association = self
                .association
                .lock()
                .expect("remote association lock poisoned");
            let remote_uid = match association.state() {
                AssociationState::Active { remote_uid }
                | AssociationState::Quarantined { remote_uid, .. } => *remote_uid,
                _ => None,
            };
            association.quarantine(remote_uid, "control lane queue overflow");
        }
        result
    }

    fn close(&self, reason: &str) -> Result<()> {
        AssociationOutboundPipeline::close(self, reason)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use kairo_serialization::{
        ActorRefWireData, MessageCodec, Registry, RemoteMessage, SerializationRegistry,
    };
    use kairo_testkit::await_assert;

    use super::*;
    use crate::{
        AssociationRemoteInbound, FramedRemoteInbound, InboundMessage, RemoteError, RemoteInbound,
        RemoteInboundDelivery, RemoteStreamId, stream_send_failure,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Business {
        value: Vec<u8>,
    }

    impl RemoteMessage for Business {
        const MANIFEST: &'static str = "kairo.remote.test.AssociationPipelineBusiness";
        const VERSION: u16 = 1;
    }

    struct BusinessCodec;

    impl MessageCodec<Business> for BusinessCodec {
        fn serializer_id(&self) -> u32 {
            701
        }

        fn encode(&self, message: &Business) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(message.value.clone()))
        }

        fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Business> {
            Ok(Business {
                value: payload.to_vec(),
            })
        }
    }

    #[derive(Default)]
    struct CollectingDelivery {
        messages: Mutex<Vec<InboundMessage<Business>>>,
    }

    impl CollectingDelivery {
        fn messages(&self) -> Vec<InboundMessage<Business>> {
            self.messages
                .lock()
                .expect("collecting delivery mutex poisoned")
                .clone()
        }
    }

    impl RemoteInboundDelivery<Business> for CollectingDelivery {
        fn deliver(&self, message: InboundMessage<Business>) -> Result<()> {
            self.messages
                .lock()
                .expect("collecting delivery mutex poisoned")
                .push(message);
            Ok(())
        }
    }

    struct InboundByteSink<M> {
        inbound: Arc<Mutex<AssociationRemoteInbound<M>>>,
        stream_id: RemoteStreamId,
    }

    impl<M> InboundByteSink<M> {
        fn new(
            inbound: Arc<Mutex<AssociationRemoteInbound<M>>>,
            stream_id: RemoteStreamId,
        ) -> Self {
            Self { inbound, stream_id }
        }
    }

    impl<M> RemoteByteSink for InboundByteSink<M>
    where
        M: RemoteMessage,
    {
        fn send_bytes(&self, bytes: Bytes) -> Result<()> {
            let mut inbound = self.inbound.lock().expect("association inbound poisoned");
            match self.stream_id {
                RemoteStreamId::Control => inbound.push_control_bytes(bytes),
                RemoteStreamId::Ordinary => inbound.push_ordinary_bytes(bytes),
                RemoteStreamId::Large => inbound.push_large_bytes(bytes),
            }?;
            Ok(())
        }
    }

    struct FailingByteSink {
        stream_id: RemoteStreamId,
    }

    impl RemoteByteSink for FailingByteSink {
        fn send_bytes(&self, _bytes: Bytes) -> Result<()> {
            Err(stream_send_failure(self.stream_id, "socket closed"))
        }
    }

    struct CloseTrackingByteSink {
        fail_writes: bool,
        closes: AtomicUsize,
    }

    impl CloseTrackingByteSink {
        fn new(fail_writes: bool) -> Self {
            Self {
                fail_writes,
                closes: AtomicUsize::new(0),
            }
        }

        fn close_count(&self) -> usize {
            self.closes.load(Ordering::Acquire)
        }
    }

    impl RemoteByteSink for CloseTrackingByteSink {
        fn send_bytes(&self, _bytes: Bytes) -> Result<()> {
            if self.fail_writes {
                Err(RemoteError::Outbound("forced writer failure".to_string()))
            } else {
                Ok(())
            }
        }

        fn close(&self) -> Result<()> {
            self.closes.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    #[derive(Default)]
    struct BlockingByteSink {
        entered: Mutex<bool>,
        entered_changed: Condvar,
        released: Mutex<bool>,
        release_changed: Condvar,
    }

    impl BlockingByteSink {
        fn wait_until_entered(&self, timeout: Duration) {
            let deadline = Instant::now() + timeout;
            let mut entered = self.entered.lock().expect("blocking sink poisoned");
            while !*entered {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .expect("queued control writer did not start");
                let (next, wait) = self
                    .entered_changed
                    .wait_timeout(entered, remaining)
                    .expect("blocking sink poisoned");
                entered = next;
                assert!(!wait.timed_out(), "queued control writer did not start");
            }
        }

        fn release(&self) {
            *self.released.lock().expect("blocking sink poisoned") = true;
            self.release_changed.notify_all();
        }
    }

    impl RemoteByteSink for BlockingByteSink {
        fn send_bytes(&self, _bytes: Bytes) -> Result<()> {
            *self.entered.lock().expect("blocking sink poisoned") = true;
            self.entered_changed.notify_all();
            let mut released = self.released.lock().expect("blocking sink poisoned");
            while !*released {
                released = self
                    .release_changed
                    .wait(released)
                    .expect("blocking sink poisoned");
            }
            Ok(())
        }

        fn close(&self) -> Result<()> {
            self.release();
            Ok(())
        }
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        registry.register::<Business, _>(BusinessCodec).unwrap();
        Arc::new(registry)
    }

    fn envelope(registry: &Registry, value: &[u8]) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new("kairo://receiver@127.0.0.1:25520/user/target").unwrap(),
            Some(ActorRefWireData::new("kairo://sender@127.0.0.1:25521/user/source").unwrap()),
            registry
                .serialize(&Business {
                    value: value.to_vec(),
                })
                .unwrap(),
        )
    }

    fn inbound(
        registry: Arc<Registry>,
        delivery: Arc<CollectingDelivery>,
    ) -> Arc<Mutex<AssociationRemoteInbound<Business>>> {
        Arc::new(Mutex::new(AssociationRemoteInbound::new(
            FramedRemoteInbound::new(RemoteInbound::new(
                registry,
                delivery as Arc<dyn RemoteInboundDelivery<Business>>,
            )),
        )))
    }

    fn pipeline_to_inbound(
        classifier: RemoteLaneClassifier,
        inbound: Arc<Mutex<AssociationRemoteInbound<Business>>>,
    ) -> AssociationOutboundPipeline {
        AssociationOutboundPipeline::new(
            "kairo://receiver@127.0.0.1:25520",
            classifier,
            Arc::new(InboundByteSink::new(
                inbound.clone(),
                RemoteStreamId::Control,
            )),
            Arc::new(InboundByteSink::new(
                inbound.clone(),
                RemoteStreamId::Ordinary,
            )),
            Arc::new(InboundByteSink::new(inbound, RemoteStreamId::Large)),
        )
    }

    #[test]
    fn pipeline_sends_ordinary_envelopes_through_streams_to_inbound_delivery() {
        let registry = registry();
        let delivery = Arc::new(CollectingDelivery::default());
        let inbound = inbound(registry.clone(), delivery.clone());
        let pipeline = pipeline_to_inbound(RemoteLaneClassifier::default(), inbound.clone());

        pipeline.send(envelope(&registry, b"hello")).unwrap();

        let messages = delivery.messages();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message.value, b"hello");
        assert_eq!(messages[0].recipient.system(), "receiver");
        assert_eq!(
            messages[0].sender.as_ref().map(ActorRefWireData::system),
            Some("sender")
        );
        assert_eq!(
            inbound
                .lock()
                .expect("association inbound poisoned")
                .ordinary_stream_id(),
            Some(RemoteStreamId::Ordinary)
        );
    }

    #[test]
    fn pipeline_can_route_configured_control_manifest_to_control_stream() {
        let registry = registry();
        let delivery = Arc::new(CollectingDelivery::default());
        let inbound = inbound(registry.clone(), delivery.clone());
        let mut classifier = RemoteLaneClassifier::default();
        classifier.add_control_manifest(Business::MANIFEST);
        let pipeline = pipeline_to_inbound(classifier, inbound.clone());

        pipeline.send(envelope(&registry, b"control")).unwrap();

        assert_eq!(delivery.messages()[0].message.value, b"control");
        assert_eq!(
            inbound
                .lock()
                .expect("association inbound poisoned")
                .control_stream_id(),
            Some(RemoteStreamId::Control)
        );
    }

    #[test]
    fn pipeline_routes_large_frames_to_large_stream() {
        let registry = registry();
        let delivery = Arc::new(CollectingDelivery::default());
        let inbound = inbound(registry.clone(), delivery.clone());
        let pipeline = pipeline_to_inbound(
            RemoteLaneClassifier::default().with_large_frame_threshold(1),
            inbound.clone(),
        );

        pipeline.send(envelope(&registry, b"large")).unwrap();

        assert_eq!(delivery.messages()[0].message.value, b"large");
        assert_eq!(
            inbound
                .lock()
                .expect("association inbound poisoned")
                .large_stream_id(),
            Some(RemoteStreamId::Large)
        );
    }

    #[test]
    fn pipeline_rejects_closed_association_before_writing_stream_bytes() {
        let registry = registry();
        let delivery = Arc::new(CollectingDelivery::default());
        let inbound = inbound(registry.clone(), delivery.clone());
        let pipeline = pipeline_to_inbound(RemoteLaneClassifier::default(), inbound.clone());
        pipeline
            .association()
            .lock()
            .expect("association mutex poisoned")
            .close("connection stopped");

        let error = pipeline
            .send(envelope(&registry, b"blocked"))
            .expect_err("closed association should reject before stream write");

        assert!(matches!(error, RemoteError::AssociationClosed { .. }));
        assert!(delivery.messages().is_empty());
        assert_eq!(
            inbound
                .lock()
                .expect("association inbound poisoned")
                .ordinary_stream_id(),
            None
        );
    }

    #[test]
    fn control_queue_overflow_quarantines_exact_association_incarnation() {
        let registry = registry();
        let blocking = Arc::new(BlockingByteSink::default());
        let control = Arc::new(
            QueuedRemoteByteSink::new(
                RemoteStreamId::Control,
                1,
                blocking.clone() as Arc<dyn RemoteByteSink>,
            )
            .unwrap(),
        ) as Arc<dyn RemoteByteSink>;
        let mut association = RemoteAssociation::new("kairo://receiver@127.0.0.1:25520");
        association.activate(Some(42));
        let association = Arc::new(Mutex::new(association));
        let mut classifier = RemoteLaneClassifier::default();
        classifier.add_control_manifest(Business::MANIFEST);
        let pipeline = AssociationOutboundPipeline::shared(
            association.clone(),
            classifier,
            control,
            Arc::new(|_: Bytes| Ok(())),
            Arc::new(|_: Bytes| Ok(())),
        );

        pipeline.send(envelope(&registry, b"one")).unwrap();
        blocking.wait_until_entered(Duration::from_secs(1));
        pipeline.send(envelope(&registry, b"two")).unwrap();
        let error = pipeline
            .send(envelope(&registry, b"three"))
            .expect_err("control overflow should fail immediately");

        assert!(matches!(
            error,
            RemoteError::OutboundLaneQueueFull { lane, capacity: 1 }
                if lane == "control"
        ));
        assert!(matches!(
            association
                .lock()
                .expect("association poisoned")
                .state(),
            AssociationState::Quarantined {
                remote_uid: Some(42),
                reason,
            } if reason == "control lane queue overflow"
        ));
        assert!(matches!(
            pipeline
                .send(envelope(&registry, b"four"))
                .expect_err("quarantined association should reject later sends"),
            RemoteError::AssociationQuarantined { .. }
        ));

        blocking.release();
        pipeline.close("test complete").unwrap();
    }

    #[test]
    fn pipeline_propagates_stream_write_failure_with_lane_context() {
        let registry = registry();
        let pipeline = AssociationOutboundPipeline::new(
            "kairo://receiver@127.0.0.1:25520",
            RemoteLaneClassifier::default(),
            Arc::new(FailingByteSink {
                stream_id: RemoteStreamId::Control,
            }),
            Arc::new(FailingByteSink {
                stream_id: RemoteStreamId::Ordinary,
            }),
            Arc::new(FailingByteSink {
                stream_id: RemoteStreamId::Large,
            }),
        );

        let error = pipeline
            .send(envelope(&registry, b"fails"))
            .expect_err("byte sink failure should propagate");

        assert!(matches!(error, RemoteError::Outbound(_)));
        assert!(error.to_string().contains("Ordinary"));
        assert!(error.to_string().contains("socket closed"));
    }

    #[test]
    fn queued_writer_failure_closes_association_and_all_sibling_lanes() {
        let registry = registry();
        let control = Arc::new(CloseTrackingByteSink::new(false));
        let ordinary = Arc::new(CloseTrackingByteSink::new(true));
        let large = Arc::new(CloseTrackingByteSink::new(false));
        let mut association = RemoteAssociation::new("kairo://receiver@127.0.0.1:25520");
        association.activate(Some(42));
        let association = Arc::new(Mutex::new(association));
        let pipeline = AssociationOutboundPipeline::shared_queued(
            association.clone(),
            RemoteLaneClassifier::default(),
            RemoteOutboundQueueSettings::default(),
            control.clone() as Arc<dyn RemoteByteSink>,
            ordinary.clone() as Arc<dyn RemoteByteSink>,
            large.clone() as Arc<dyn RemoteByteSink>,
        )
        .unwrap();

        pipeline.send(envelope(&registry, b"fails later")).unwrap();

        await_assert(Duration::from_secs(1), Duration::from_millis(1), || {
            let state = association
                .lock()
                .expect("association poisoned")
                .state()
                .clone();
            if matches!(state, AssociationState::Closed { .. }) {
                Ok(())
            } else {
                Err(format!("expected closed association, found {state:?}"))
            }
        })
        .unwrap();

        assert!(control.close_count() >= 1);
        assert!(ordinary.close_count() >= 1);
        assert!(large.close_count() >= 1);
        assert!(matches!(
            pipeline
                .send(envelope(&registry, b"rejected"))
                .expect_err("closed association must reject sibling-lane sends"),
            RemoteError::AssociationClosed { .. }
        ));
    }
}
