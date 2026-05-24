use std::sync::{Arc, Mutex};

use kairo_serialization::RemoteEnvelope;

use crate::{
    AssociationRemoteOutbound, LaneRemoteOutbound, RemoteAssociation, RemoteByteSink,
    RemoteLaneClassifier, RemoteLaneSink, RemoteOutbound, Result, StreamLaneSink,
};

#[derive(Clone)]
pub struct AssociationOutboundPipeline {
    association: Arc<Mutex<RemoteAssociation>>,
    stream_sink: Arc<StreamLaneSink>,
    lane_outbound: Arc<LaneRemoteOutbound>,
    outbound: AssociationRemoteOutbound,
}

impl AssociationOutboundPipeline {
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

    pub fn association(&self) -> &Arc<Mutex<RemoteAssociation>> {
        &self.association
    }

    pub fn stream_sink(&self) -> &Arc<StreamLaneSink> {
        &self.stream_sink
    }

    pub fn lane_outbound(&self) -> &Arc<LaneRemoteOutbound> {
        &self.lane_outbound
    }

    pub fn guarded_outbound(&self) -> &AssociationRemoteOutbound {
        &self.outbound
    }
}

impl RemoteOutbound for AssociationOutboundPipeline {
    fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
        self.outbound.send(envelope)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use kairo_serialization::{
        ActorRefWireData, MessageCodec, Registry, RemoteMessage, SerializationRegistry,
    };

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
}
