#![deny(missing_docs)]

use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use kairo_serialization::RemoteMessage;

use crate::{FramedRemoteInbound, RemoteFrameHandler, RemoteStreamId, Result, StreamFrameInbound};

struct DeliverRemoteFrame<M> {
    inbound: FramedRemoteInbound<M>,
}

impl<M> RemoteFrameHandler for DeliverRemoteFrame<M>
where
    M: RemoteMessage,
{
    fn handle_frame(&self, _stream_id: RemoteStreamId, frame: Bytes) -> Result<()> {
        self.inbound.receive_frame(frame)
    }
}

/// Maintains independent incremental readers for an association's control,
/// ordinary, and large inbound streams.
pub struct AssociationRemoteInbound<M> {
    control: StreamFrameInbound,
    ordinary: StreamFrameInbound,
    large: StreamFrameInbound,
    _message: PhantomData<fn(M)>,
}

impl<M> AssociationRemoteInbound<M>
where
    M: RemoteMessage,
{
    /// Creates a three-lane association inbound that decodes envelopes into one
    /// typed inbound pipeline.
    pub fn new(inbound: FramedRemoteInbound<M>) -> Self {
        let handler = Arc::new(DeliverRemoteFrame { inbound }) as Arc<dyn RemoteFrameHandler>;
        Self::from_handler(handler)
    }

    /// Creates a three-lane association inbound backed by a shared decoded-frame
    /// handler.
    pub fn from_handler(handler: Arc<dyn RemoteFrameHandler>) -> Self {
        Self {
            control: StreamFrameInbound::new(handler.clone()),
            ordinary: StreamFrameInbound::new(handler.clone()),
            large: StreamFrameInbound::new(handler),
            _message: PhantomData,
        }
    }

    /// Returns the decoded control stream identifier, if its header arrived.
    pub fn control_stream_id(&self) -> Option<RemoteStreamId> {
        self.control.stream_id()
    }

    /// Returns the decoded ordinary stream identifier, if its header arrived.
    pub fn ordinary_stream_id(&self) -> Option<RemoteStreamId> {
        self.ordinary.stream_id()
    }

    /// Returns the decoded large stream identifier, if its header arrived.
    pub fn large_stream_id(&self) -> Option<RemoteStreamId> {
        self.large.stream_id()
    }

    /// Adds control-lane bytes and returns the number of frames delivered.
    pub fn push_control_bytes(&mut self, chunk: Bytes) -> Result<usize> {
        self.control.push_bytes(chunk)
    }

    /// Adds ordinary-lane bytes and returns the number of frames delivered.
    pub fn push_ordinary_bytes(&mut self, chunk: Bytes) -> Result<usize> {
        self.ordinary.push_bytes(chunk)
    }

    /// Adds large-lane bytes and returns the number of frames delivered.
    pub fn push_large_bytes(&mut self, chunk: Bytes) -> Result<usize> {
        self.large.push_bytes(chunk)
    }

    /// Finishes all lane readers, rejecting any truncated stream.
    pub fn finish(self) -> Result<()> {
        self.control.finish()?;
        self.ordinary.finish()?;
        self.large.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use kairo_serialization::{ActorRefWireData, MessageCodec, Registry, SerializationRegistry};

    use super::*;
    use crate::{
        InboundMessage, RemoteByteSink, RemoteError, RemoteInbound, RemoteInboundDelivery,
        RemoteStreamWriter,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Ping {
        value: u8,
    }

    impl RemoteMessage for Ping {
        const MANIFEST: &'static str = "kairo.remote.test.AssociationInboundPing";
        const VERSION: u16 = 1;
    }

    struct PingCodec;

    impl MessageCodec<Ping> for PingCodec {
        fn serializer_id(&self) -> u32 {
            601
        }

        fn encode(&self, message: &Ping) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(vec![message.value]))
        }

        fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Ping> {
            Ok(Ping { value: payload[0] })
        }
    }

    #[derive(Default)]
    struct CollectingDelivery {
        messages: Mutex<Vec<InboundMessage<Ping>>>,
    }

    impl CollectingDelivery {
        fn messages(&self) -> Vec<InboundMessage<Ping>> {
            self.messages
                .lock()
                .expect("collecting delivery mutex poisoned")
                .clone()
        }
    }

    impl RemoteInboundDelivery<Ping> for CollectingDelivery {
        fn deliver(&self, message: InboundMessage<Ping>) -> Result<()> {
            self.messages
                .lock()
                .expect("collecting delivery mutex poisoned")
                .push(message);
            Ok(())
        }
    }

    #[derive(Default)]
    struct CollectingByteSink {
        writes: Mutex<Vec<Bytes>>,
    }

    impl CollectingByteSink {
        fn writes(&self) -> Vec<Bytes> {
            self.writes
                .lock()
                .expect("collecting byte sink mutex poisoned")
                .clone()
        }
    }

    impl RemoteByteSink for CollectingByteSink {
        fn send_bytes(&self, bytes: Bytes) -> Result<()> {
            self.writes
                .lock()
                .expect("collecting byte sink mutex poisoned")
                .push(bytes);
            Ok(())
        }
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        registry.register::<Ping, _>(PingCodec).unwrap();
        Arc::new(registry)
    }

    fn remote_envelope(registry: &Registry, value: u8) -> kairo_serialization::RemoteEnvelope {
        kairo_serialization::RemoteEnvelope::new(
            ActorRefWireData::new("kairo://receiver@127.0.0.1:25520/user/target").unwrap(),
            Some(ActorRefWireData::new("kairo://sender@127.0.0.1:25521/user/source").unwrap()),
            registry.serialize(&Ping { value }).unwrap(),
        )
    }

    #[test]
    fn association_inbound_decodes_lane_stream_bytes_to_typed_delivery() {
        let registry = registry();
        let delivery = Arc::new(CollectingDelivery::default());
        let framed = FramedRemoteInbound::new(RemoteInbound::new(
            registry.clone(),
            delivery.clone() as Arc<dyn RemoteInboundDelivery<Ping>>,
        ));
        let mut inbound = AssociationRemoteInbound::new(framed);
        let ordinary_sink = Arc::new(CollectingByteSink::default());
        let writer = RemoteStreamWriter::new(
            RemoteStreamId::Ordinary,
            ordinary_sink.clone() as Arc<dyn RemoteByteSink>,
        );

        writer
            .send_frame_payload(
                crate::encode_remote_envelope_frame(&remote_envelope(&registry, 7)).unwrap(),
            )
            .unwrap();
        writer
            .send_frame_payload(
                crate::encode_remote_envelope_frame(&remote_envelope(&registry, 9)).unwrap(),
            )
            .unwrap();

        for write in ordinary_sink.writes() {
            inbound.push_ordinary_bytes(write).unwrap();
        }
        assert_eq!(inbound.ordinary_stream_id(), Some(RemoteStreamId::Ordinary));
        inbound.finish().unwrap();

        let messages = delivery.messages();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message, Ping { value: 7 });
        assert_eq!(messages[1].message, Ping { value: 9 });
        assert_eq!(messages[0].recipient.system(), "receiver");
        assert_eq!(
            messages[0].sender.as_ref().map(ActorRefWireData::system),
            Some("sender")
        );
    }

    #[test]
    fn association_inbound_keeps_control_ordinary_and_large_readers_separate() {
        let registry = registry();
        let delivery = Arc::new(CollectingDelivery::default());
        let framed = FramedRemoteInbound::new(RemoteInbound::new(
            registry.clone(),
            delivery.clone() as Arc<dyn RemoteInboundDelivery<Ping>>,
        ));
        let mut inbound = AssociationRemoteInbound::new(framed);
        let control_sink = Arc::new(CollectingByteSink::default());
        let large_sink = Arc::new(CollectingByteSink::default());
        let control = RemoteStreamWriter::new(
            RemoteStreamId::Control,
            control_sink.clone() as Arc<dyn RemoteByteSink>,
        );
        let large = RemoteStreamWriter::new(
            RemoteStreamId::Large,
            large_sink.clone() as Arc<dyn RemoteByteSink>,
        );

        control
            .send_frame_payload(
                crate::encode_remote_envelope_frame(&remote_envelope(&registry, 1)).unwrap(),
            )
            .unwrap();
        large
            .send_frame_payload(
                crate::encode_remote_envelope_frame(&remote_envelope(&registry, 2)).unwrap(),
            )
            .unwrap();

        inbound
            .push_large_bytes(large_sink.writes()[0].clone())
            .unwrap();
        inbound
            .push_control_bytes(control_sink.writes()[0].clone())
            .unwrap();

        assert_eq!(inbound.control_stream_id(), Some(RemoteStreamId::Control));
        assert_eq!(inbound.large_stream_id(), Some(RemoteStreamId::Large));
        assert_eq!(inbound.ordinary_stream_id(), None);
        assert_eq!(
            delivery
                .messages()
                .into_iter()
                .map(|message| message.message.value)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
    }

    #[test]
    fn association_inbound_propagates_invalid_stream_before_delivery() {
        let delivery = Arc::new(CollectingDelivery::default());
        let framed = FramedRemoteInbound::new(RemoteInbound::new(
            registry(),
            delivery.clone() as Arc<dyn RemoteInboundDelivery<Ping>>,
        ));
        let mut inbound = AssociationRemoteInbound::new(framed);

        let error = inbound
            .push_control_bytes(Bytes::from_static(b"invalid stream bytes"))
            .expect_err("invalid stream should fail");

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(delivery.messages().is_empty());
    }

    #[test]
    fn association_inbound_propagates_missing_codec_after_stream_decode() {
        let delivery = Arc::new(CollectingDelivery::default());
        let framed = FramedRemoteInbound::new(RemoteInbound::new(
            Arc::new(Registry::new()),
            delivery.clone() as Arc<dyn RemoteInboundDelivery<Ping>>,
        ));
        let mut inbound = AssociationRemoteInbound::new(framed);
        let sink = Arc::new(CollectingByteSink::default());
        let writer = RemoteStreamWriter::new(
            RemoteStreamId::Ordinary,
            sink.clone() as Arc<dyn RemoteByteSink>,
        );
        let registered = registry();

        writer
            .send_frame_payload(
                crate::encode_remote_envelope_frame(&remote_envelope(&registered, 3)).unwrap(),
            )
            .unwrap();

        let error = inbound
            .push_ordinary_bytes(sink.writes()[0].clone())
            .expect_err("missing codec should fail after stream decode");

        assert!(error.to_string().contains("no codec registered"));
        assert!(delivery.messages().is_empty());
    }

    #[test]
    fn association_inbound_finish_rejects_truncated_lane_stream() {
        let delivery = Arc::new(CollectingDelivery::default());
        let framed = FramedRemoteInbound::new(RemoteInbound::new(
            registry(),
            delivery.clone() as Arc<dyn RemoteInboundDelivery<Ping>>,
        ));
        let mut inbound = AssociationRemoteInbound::new(framed);
        let sink = Arc::new(CollectingByteSink::default());
        let writer = RemoteStreamWriter::new(
            RemoteStreamId::Large,
            sink.clone() as Arc<dyn RemoteByteSink>,
        );

        writer
            .send_frame_payload(Bytes::from_static(b"not complete"))
            .unwrap();
        inbound
            .push_large_bytes(sink.writes()[0].slice(..8))
            .expect("partial stream header should be buffered");

        let error = inbound.finish().expect_err("truncated lane should fail");

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(error.to_string().contains("truncated"));
        assert!(delivery.messages().is_empty());
    }
}
