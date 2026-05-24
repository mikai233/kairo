use std::fmt::{self, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use kairo_actor::{ActorPath, Recipient, SendError};
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializedMessage,
};

use crate::{RemoteOutbound, Result};

pub struct RemoteActorRef<M> {
    path: ActorPath,
    recipient: ActorRefWireData,
    registry: Arc<Registry>,
    outbound: Arc<dyn RemoteOutbound>,
    stopped: Arc<AtomicBool>,
    _message: PhantomData<fn(M)>,
}

impl<M> RemoteActorRef<M> {
    pub fn new(
        recipient: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        Self {
            path: ActorPath::new(recipient.path()),
            recipient,
            registry,
            outbound,
            stopped: Arc::new(AtomicBool::new(false)),
            _message: PhantomData,
        }
    }

    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    pub fn recipient(&self) -> &ActorRefWireData {
        &self.recipient
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub fn mark_stopped(&self) {
        self.stopped.store(true, Ordering::Release);
    }
}

impl<M> Clone for RemoteActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            recipient: self.recipient.clone(),
            registry: Arc::clone(&self.registry),
            outbound: Arc::clone(&self.outbound),
            stopped: Arc::clone(&self.stopped),
            _message: PhantomData,
        }
    }
}

impl<M> fmt::Debug for RemoteActorRef<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteActorRef")
            .field("path", &self.path)
            .field("stopped", &self.is_stopped())
            .finish_non_exhaustive()
    }
}

impl<M> RemoteActorRef<M>
where
    M: RemoteMessage,
{
    pub fn build_envelope(
        &self,
        message: &M,
        sender: Option<ActorRefWireData>,
    ) -> Result<RemoteEnvelope> {
        let serialized = self.registry.serialize(message)?;
        Ok(self.envelope_from_serialized(serialized, sender))
    }

    pub fn tell(&self, message: M) -> std::result::Result<(), SendError<M>> {
        self.tell_with_sender(message, None)
    }

    pub fn tell_with_sender(
        &self,
        message: M,
        sender: Option<ActorRefWireData>,
    ) -> std::result::Result<(), SendError<M>> {
        if self.is_stopped() {
            return Err(SendError::new(message, "remote actor ref is stopped"));
        }

        let envelope = match self.build_envelope(&message, sender) {
            Ok(envelope) => envelope,
            Err(error) => return Err(SendError::new(message, error.to_string())),
        };

        match self.outbound.send(envelope) {
            Ok(()) => Ok(()),
            Err(error) => Err(SendError::new(message, error.to_string())),
        }
    }

    fn envelope_from_serialized(
        &self,
        message: SerializedMessage,
        sender: Option<ActorRefWireData>,
    ) -> RemoteEnvelope {
        RemoteEnvelope::new(self.recipient.clone(), sender, message)
    }
}

impl<M> Recipient<M> for RemoteActorRef<M>
where
    M: RemoteMessage,
{
    fn tell(&self, message: M) -> std::result::Result<(), SendError<M>> {
        RemoteActorRef::tell(self, message)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bytes::Bytes;
    use kairo_serialization::{MessageCodec, SerializationRegistry};

    use super::*;
    use crate::RemoteError;

    #[derive(Debug, PartialEq, Eq)]
    struct Ping {
        value: u8,
    }

    impl RemoteMessage for Ping {
        const MANIFEST: &'static str = "kairo.remote.test.Ping";
        const VERSION: u16 = 3;
    }

    #[derive(Debug, Clone, Copy)]
    struct PingCodec;

    impl MessageCodec<Ping> for PingCodec {
        fn serializer_id(&self) -> u32 {
            77
        }

        fn encode(&self, message: &Ping) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(vec![message.value]))
        }

        fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Ping> {
            Ok(Ping { value: payload[0] })
        }
    }

    #[derive(Default)]
    struct CollectingOutbound {
        sent: Mutex<Vec<RemoteEnvelope>>,
        fail_with: Mutex<Option<String>>,
    }

    impl CollectingOutbound {
        fn envelopes(&self) -> Vec<RemoteEnvelope> {
            self.sent.lock().expect("outbound poisoned").clone()
        }

        fn fail(&self, reason: impl Into<String>) {
            *self.fail_with.lock().expect("outbound poisoned") = Some(reason.into());
        }
    }

    impl RemoteOutbound for CollectingOutbound {
        fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
            if let Some(reason) = self.fail_with.lock().expect("outbound poisoned").clone() {
                return Err(RemoteError::Outbound(reason));
            }
            self.sent.lock().expect("outbound poisoned").push(envelope);
            Ok(())
        }
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        registry.register::<Ping, _>(PingCodec).unwrap();
        Arc::new(registry)
    }

    #[test]
    fn remote_ref_serializes_to_remote_envelope_before_outbound_send() {
        let outbound = Arc::new(CollectingOutbound::default());
        let recipient =
            ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/pinger#4").unwrap();
        let remote_ref = RemoteActorRef::<Ping>::new(
            recipient,
            registry(),
            outbound.clone() as Arc<dyn RemoteOutbound>,
        );
        let sender = ActorRefWireData::new("kairo://local/user/sender#1").unwrap();

        remote_ref
            .tell_with_sender(Ping { value: 9 }, Some(sender))
            .unwrap();

        let envelopes = outbound.envelopes();
        assert_eq!(envelopes.len(), 1);
        let envelope = &envelopes[0];
        assert_eq!(
            envelope.recipient.path(),
            "kairo://remote@127.0.0.1:25520/user/pinger#4"
        );
        assert_eq!(
            envelope.sender.as_ref().map(ActorRefWireData::system),
            Some("local")
        );
        assert_eq!(envelope.message.serializer_id, 77);
        assert_eq!(envelope.message.manifest.as_str(), "kairo.remote.test.Ping");
        assert_eq!(envelope.message.version, 3);
        assert_eq!(envelope.message.payload, Bytes::from_static(&[9]));
    }

    #[test]
    fn remote_ref_returns_original_message_on_outbound_failure() {
        let outbound = Arc::new(CollectingOutbound::default());
        outbound.fail("association closed");
        let remote_ref = RemoteActorRef::<Ping>::new(
            ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/pinger").unwrap(),
            registry(),
            outbound as Arc<dyn RemoteOutbound>,
        );

        let error = remote_ref
            .tell(Ping { value: 11 })
            .expect_err("send should fail");

        assert_eq!(error.into_message(), Ping { value: 11 });
    }

    #[test]
    fn remote_ref_requires_registered_codec() {
        let outbound = Arc::new(CollectingOutbound::default());
        let remote_ref = RemoteActorRef::<Ping>::new(
            ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/pinger").unwrap(),
            Arc::new(Registry::new()),
            outbound as Arc<dyn RemoteOutbound>,
        );

        let error = remote_ref
            .tell(Ping { value: 1 })
            .expect_err("missing codec should fail");

        assert!(error.reason().contains("no codec registered"));
        assert_eq!(error.into_message(), Ping { value: 1 });
    }
}
