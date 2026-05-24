use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use kairo_serialization::{RemoteEnvelope, RemoteMessage};

use crate::{
    RemoteInbound, RemoteOutbound, Result, decode_remote_envelope_frame,
    encode_remote_envelope_frame,
};

pub trait RemoteFrameSink: Send + Sync + 'static {
    fn send_frame(&self, frame: Bytes) -> Result<()>;
}

impl<F> RemoteFrameSink for F
where
    F: Fn(Bytes) -> Result<()> + Send + Sync + 'static,
{
    fn send_frame(&self, frame: Bytes) -> Result<()> {
        self(frame)
    }
}

#[derive(Clone)]
pub struct FramedRemoteOutbound {
    sink: Arc<dyn RemoteFrameSink>,
}

impl FramedRemoteOutbound {
    pub fn new(sink: Arc<dyn RemoteFrameSink>) -> Self {
        Self { sink }
    }

    pub fn sink(&self) -> &Arc<dyn RemoteFrameSink> {
        &self.sink
    }
}

impl RemoteOutbound for FramedRemoteOutbound {
    fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
        let frame = encode_remote_envelope_frame(&envelope)?;
        self.sink.send_frame(frame)
    }
}

pub struct FramedRemoteInbound<M> {
    inbound: RemoteInbound<M>,
    _message: PhantomData<fn(M)>,
}

impl<M> FramedRemoteInbound<M>
where
    M: RemoteMessage,
{
    pub fn new(inbound: RemoteInbound<M>) -> Self {
        Self {
            inbound,
            _message: PhantomData,
        }
    }

    pub fn receive_frame(&self, frame: Bytes) -> Result<()> {
        self.inbound.receive(decode_remote_envelope_frame(frame)?)
    }

    pub fn inbound(&self) -> &RemoteInbound<M> {
        &self.inbound
    }
}

impl<M> Clone for FramedRemoteInbound<M> {
    fn clone(&self) -> Self {
        Self {
            inbound: self.inbound.clone(),
            _message: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use kairo_serialization::{
        ActorRefWireData, Manifest, MessageCodec, Registry, SerializationRegistry,
        SerializedMessage,
    };

    use super::*;
    use crate::{InboundMessage, RemoteError, RemoteInboundDelivery};

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Ping {
        value: u8,
    }

    impl RemoteMessage for Ping {
        const MANIFEST: &'static str = "kairo.remote.test.TransportPing";
        const VERSION: u16 = 1;
    }

    struct PingCodec;

    impl MessageCodec<Ping> for PingCodec {
        fn serializer_id(&self) -> u32 {
            101
        }

        fn encode(&self, message: &Ping) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(vec![message.value]))
        }

        fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Ping> {
            Ok(Ping { value: payload[0] })
        }
    }

    #[derive(Default)]
    struct CollectingFrameSink {
        frames: Mutex<Vec<Bytes>>,
    }

    impl CollectingFrameSink {
        fn frames(&self) -> Vec<Bytes> {
            self.frames.lock().expect("frame sink poisoned").clone()
        }
    }

    impl RemoteFrameSink for CollectingFrameSink {
        fn send_frame(&self, frame: Bytes) -> Result<()> {
            self.frames.lock().expect("frame sink poisoned").push(frame);
            Ok(())
        }
    }

    #[derive(Default)]
    struct CollectingDelivery {
        messages: Mutex<Vec<InboundMessage<Ping>>>,
    }

    impl CollectingDelivery {
        fn messages(&self) -> Vec<InboundMessage<Ping>> {
            self.messages.lock().expect("delivery poisoned").clone()
        }
    }

    impl RemoteInboundDelivery<Ping> for CollectingDelivery {
        fn deliver(&self, message: InboundMessage<Ping>) -> Result<()> {
            self.messages
                .lock()
                .expect("delivery poisoned")
                .push(message);
            Ok(())
        }
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        registry.register::<Ping, _>(PingCodec).unwrap();
        Arc::new(registry)
    }

    fn envelope(registry: &Registry, value: u8) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new("kairo://receiver@127.0.0.1:25520/user/target").unwrap(),
            Some(ActorRefWireData::new("kairo://sender@127.0.0.1:25521/user/source").unwrap()),
            registry.serialize(&Ping { value }).unwrap(),
        )
    }

    #[test]
    fn framed_outbound_encodes_envelope_before_transport_send() {
        let registry = registry();
        let sink = Arc::new(CollectingFrameSink::default());
        let outbound = FramedRemoteOutbound::new(sink.clone() as Arc<dyn RemoteFrameSink>);

        outbound.send(envelope(&registry, 3)).unwrap();

        let frames = sink.frames();
        assert_eq!(frames.len(), 1);
        let decoded = decode_remote_envelope_frame(frames[0].clone()).unwrap();
        assert_eq!(decoded.recipient.system(), "receiver");
        assert_eq!(
            decoded.sender.as_ref().map(ActorRefWireData::system),
            Some("sender")
        );
        assert_eq!(decoded.message.serializer_id, 101);
        assert_eq!(decoded.message.manifest.as_str(), Ping::MANIFEST);
        assert_eq!(decoded.message.version, Ping::VERSION);
        assert_eq!(decoded.message.payload, Bytes::from_static(&[3]));
    }

    #[test]
    fn framed_inbound_decodes_frame_and_delivers_typed_message() {
        let registry = registry();
        let sink = Arc::new(CollectingFrameSink::default());
        let delivery = Arc::new(CollectingDelivery::default());
        let outbound = FramedRemoteOutbound::new(sink.clone() as Arc<dyn RemoteFrameSink>);
        let inbound = FramedRemoteInbound::<Ping>::new(RemoteInbound::new(
            Arc::clone(&registry),
            delivery.clone() as Arc<dyn RemoteInboundDelivery<Ping>>,
        ));

        outbound.send(envelope(&registry, 8)).unwrap();
        inbound.receive_frame(sink.frames()[0].clone()).unwrap();

        let messages = delivery.messages();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].recipient.system(), "receiver");
        assert_eq!(
            messages[0].sender.as_ref().map(ActorRefWireData::system),
            Some("sender")
        );
        assert_eq!(messages[0].message, Ping { value: 8 });
    }

    #[test]
    fn framed_outbound_propagates_transport_failure() {
        let registry = registry();
        let outbound = FramedRemoteOutbound::new(Arc::new(|_frame: Bytes| {
            Err(RemoteError::Outbound("write buffer full".to_string()))
        }));

        let error = outbound
            .send(envelope(&registry, 1))
            .expect_err("transport failure should propagate");

        assert!(matches!(error, RemoteError::Outbound(_)));
        assert!(error.to_string().contains("write buffer full"));
    }

    #[test]
    fn framed_inbound_rejects_invalid_frame_before_delivery() {
        let delivery = Arc::new(CollectingDelivery::default());
        let inbound = FramedRemoteInbound::<Ping>::new(RemoteInbound::new(
            registry(),
            delivery.clone() as Arc<dyn RemoteInboundDelivery<Ping>>,
        ));

        let error = inbound
            .receive_frame(Bytes::from_static(b"not a remote envelope"))
            .expect_err("invalid frame should fail");

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(delivery.messages().is_empty());
    }

    #[test]
    fn framed_inbound_reports_missing_wire_codec_after_frame_decode() {
        let delivery = Arc::new(CollectingDelivery::default());
        let inbound = FramedRemoteInbound::<Ping>::new(RemoteInbound::new(
            Arc::new(Registry::new()),
            delivery.clone() as Arc<dyn RemoteInboundDelivery<Ping>>,
        ));
        let envelope = RemoteEnvelope::new(
            ActorRefWireData::new("kairo://receiver@127.0.0.1:25520/user/target").unwrap(),
            None,
            SerializedMessage::new(
                101,
                Manifest::new(Ping::MANIFEST),
                Ping::VERSION,
                Bytes::from_static(&[1]),
            ),
        );

        let error = inbound
            .receive_frame(encode_remote_envelope_frame(&envelope).unwrap())
            .expect_err("missing codec should fail after frame decode");

        assert!(error.to_string().contains("no codec registered"));
        assert!(delivery.messages().is_empty());
    }
}
