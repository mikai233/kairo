use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_serialization::RemoteEnvelope;

use crate::Result;

pub trait RemoteOutbound: Send + Sync + 'static {
    fn send(&self, envelope: RemoteEnvelope) -> Result<()>;

    fn close(&self, _reason: &str) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct RemoteOutboundRecipient {
    outbound: Arc<dyn RemoteOutbound>,
}

impl RemoteOutboundRecipient {
    pub fn new(outbound: impl RemoteOutbound + 'static) -> Self {
        Self::from_arc(Arc::new(outbound))
    }

    pub fn from_arc(outbound: Arc<dyn RemoteOutbound>) -> Self {
        Self { outbound }
    }
}

impl Recipient<RemoteEnvelope> for RemoteOutboundRecipient {
    fn tell(&self, message: RemoteEnvelope) -> std::result::Result<(), SendError<RemoteEnvelope>> {
        self.outbound
            .send(message.clone())
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use kairo_serialization::{ActorRefWireData, Manifest, SerializedMessage};

    use super::*;
    use crate::{RemoteError, RemoteOutbound};

    #[derive(Default)]
    struct CollectingRemoteOutbound {
        envelopes: Mutex<Vec<RemoteEnvelope>>,
    }

    impl CollectingRemoteOutbound {
        fn envelopes(&self) -> Vec<RemoteEnvelope> {
            self.envelopes
                .lock()
                .expect("collecting remote outbound lock poisoned")
                .clone()
        }
    }

    impl RemoteOutbound for CollectingRemoteOutbound {
        fn send(&self, envelope: RemoteEnvelope) -> crate::Result<()> {
            self.envelopes
                .lock()
                .expect("collecting remote outbound lock poisoned")
                .push(envelope);
            Ok(())
        }
    }

    struct FailingRemoteOutbound;

    impl RemoteOutbound for FailingRemoteOutbound {
        fn send(&self, _envelope: RemoteEnvelope) -> crate::Result<()> {
            Err(RemoteError::Outbound("closed".to_string()))
        }
    }

    fn envelope(value: u8) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new("kairo://remote@127.0.0.1:25521/user/target").unwrap(),
            None,
            SerializedMessage::new(17, Manifest::new("test.Envelope"), 1, vec![value].into()),
        )
    }

    #[test]
    fn remote_outbound_recipient_adapts_successful_sends() {
        let collecting = Arc::new(CollectingRemoteOutbound::default());
        let recipient =
            RemoteOutboundRecipient::from_arc(collecting.clone() as Arc<dyn RemoteOutbound>);
        let message = envelope(7);

        recipient.tell(message.clone()).unwrap();

        assert_eq!(collecting.envelopes(), vec![message]);
    }

    #[test]
    fn remote_outbound_recipient_preserves_failed_envelope() {
        let recipient = RemoteOutboundRecipient::new(FailingRemoteOutbound);
        let message = envelope(9);

        let error = recipient
            .tell(message.clone())
            .expect_err("failed remote send should return SendError");

        assert_eq!(error.reason(), "remote outbound delivery failed: closed");
        assert_eq!(error.into_message(), message);
    }
}
