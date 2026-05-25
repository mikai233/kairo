use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_cluster::UniqueAddress;
use kairo_remote::RemoteOutbound;
use kairo_serialization::{ActorRefWireData, RemoteEnvelope, SerializationError};

use super::wire::PubSubSerializedGossip;

pub const DEFAULT_PUBSUB_REMOTE_PATH: &str = "/system/pubsub";

#[derive(Debug)]
pub enum PubSubRemoteEnvelopeError {
    InvalidRecipientPath(String),
    MissingRemoteHost { node: String },
    Serialization(SerializationError),
    Send { node: String, reason: String },
}

impl Display for PubSubRemoteEnvelopeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRecipientPath(path) => {
                write!(f, "pubsub remote target path `{path}` must start with `/`")
            }
            Self::MissingRemoteHost { node } => {
                write!(f, "pubsub remote target {node} has no remote host")
            }
            Self::Serialization(error) => write!(f, "{error}"),
            Self::Send { node, reason } => {
                write!(f, "pubsub remote envelope send to {node} failed: {reason}")
            }
        }
    }
}

impl std::error::Error for PubSubRemoteEnvelopeError {}

impl From<SerializationError> for PubSubRemoteEnvelopeError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
pub struct PubSubRemoteEnvelopeOutbound {
    recipient_path: String,
    sender: Option<ActorRefWireData>,
    outbound: Arc<dyn RemoteOutbound>,
}

impl PubSubRemoteEnvelopeOutbound {
    pub fn new(outbound: impl RemoteOutbound + 'static) -> Self {
        Self::from_arc(Arc::new(outbound))
    }

    pub fn from_arc(outbound: Arc<dyn RemoteOutbound>) -> Self {
        Self {
            recipient_path: DEFAULT_PUBSUB_REMOTE_PATH.to_string(),
            sender: None,
            outbound,
        }
    }

    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    pub fn with_sender(mut self, sender: Option<ActorRefWireData>) -> Self {
        self.sender = sender;
        self
    }

    pub fn recipient_for_node(
        &self,
        node: &UniqueAddress,
    ) -> Result<ActorRefWireData, PubSubRemoteEnvelopeError> {
        if !self.recipient_path.starts_with('/') {
            return Err(PubSubRemoteEnvelopeError::InvalidRecipientPath(
                self.recipient_path.clone(),
            ));
        }

        if node.address.host().is_none() {
            return Err(PubSubRemoteEnvelopeError::MissingRemoteHost {
                node: node.ordering_key(),
            });
        }

        Ok(ActorRefWireData::new(format!(
            "{}{}",
            node.address, self.recipient_path
        ))?)
    }

    pub fn send_serialized(
        &self,
        gossip: PubSubSerializedGossip,
    ) -> Result<(), PubSubRemoteEnvelopeError> {
        let target = gossip.target.clone();
        let recipient = self.recipient_for_node(&target)?;
        let envelope = RemoteEnvelope::new(recipient, self.sender.clone(), gossip.message);
        self.outbound
            .send(envelope)
            .map_err(|error| PubSubRemoteEnvelopeError::Send {
                node: target.ordering_key(),
                reason: error.to_string(),
            })
    }
}

impl Recipient<PubSubSerializedGossip> for PubSubRemoteEnvelopeOutbound {
    fn tell(
        &self,
        message: PubSubSerializedGossip,
    ) -> Result<(), SendError<PubSubSerializedGossip>> {
        let rejected = message.clone();
        self.send_serialized(message)
            .map_err(|error| SendError::new(rejected, error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use kairo_actor::{Address, Recipient};
    use kairo_cluster::UniqueAddress;
    use kairo_remote::{RemoteAssociationAddress, RemoteAssociationCache, Result};
    use kairo_serialization::{Manifest, RemoteEnvelope, RemoteMessage, SerializedMessage};

    use super::*;
    use crate::{PUBSUB_STATUS_SERIALIZER_ID, PubSubStatus};

    #[derive(Default)]
    struct CollectingRemoteOutbound {
        sent: Mutex<Vec<RemoteEnvelope>>,
    }

    impl CollectingRemoteOutbound {
        fn sent(&self) -> Vec<RemoteEnvelope> {
            self.sent
                .lock()
                .expect("collecting remote outbound poisoned")
                .clone()
        }
    }

    impl RemoteOutbound for CollectingRemoteOutbound {
        fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
            self.sent
                .lock()
                .expect("collecting remote outbound poisoned")
                .push(envelope);
            Ok(())
        }
    }

    fn node(name: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new(
                "kairo",
                "pubsub",
                Some(format!("{name}.example.test")),
                Some(2552),
            ),
            uid,
        )
    }

    fn gossip_for(target: UniqueAddress, value: u8) -> PubSubSerializedGossip {
        PubSubSerializedGossip::new(
            target,
            SerializedMessage::new(
                PUBSUB_STATUS_SERIALIZER_ID,
                Manifest::new(PubSubStatus::MANIFEST),
                PubSubStatus::VERSION,
                Bytes::from(vec![value]),
            ),
        )
    }

    #[test]
    fn remote_envelope_outbound_wraps_serialized_gossip_for_target_mediator() {
        let collecting = Arc::new(CollectingRemoteOutbound::default());
        let outbound =
            PubSubRemoteEnvelopeOutbound::from_arc(collecting.clone() as Arc<dyn RemoteOutbound>);
        let target = node("peer", 7);

        outbound.tell(gossip_for(target, 1)).unwrap();

        let sent = collecting.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].recipient.path(),
            "kairo://pubsub@peer.example.test:2552/system/pubsub"
        );
        assert_eq!(sent[0].message.payload, Bytes::from_static(&[1]));
    }

    #[test]
    fn remote_envelope_outbound_rejects_local_only_target() {
        let outbound = PubSubRemoteEnvelopeOutbound::new(CollectingRemoteOutbound::default());
        let local = UniqueAddress::new(Address::local("pubsub"), 1);
        let message = gossip_for(local, 2);

        let error = outbound
            .tell(message)
            .expect_err("local-only target should be rejected");

        assert!(error.reason().contains("has no remote host"));
        assert_eq!(error.into_message().target.address.host(), None);
    }

    #[test]
    fn remote_envelope_outbound_can_use_association_cache() {
        let cache = RemoteAssociationCache::new();
        let collecting = Arc::new(CollectingRemoteOutbound::default());
        cache.insert_route(
            RemoteAssociationAddress::new("kairo", "pubsub", "peer.example.test", Some(2552))
                .unwrap(),
            collecting.clone() as Arc<dyn RemoteOutbound>,
        );
        let outbound = PubSubRemoteEnvelopeOutbound::new(cache);

        outbound.tell(gossip_for(node("peer", 9), 3)).unwrap();

        let sent = collecting.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].recipient.path(),
            "kairo://pubsub@peer.example.test:2552/system/pubsub"
        );
        assert_eq!(
            sent[0].message.manifest.as_str(),
            "kairo.cluster-tools.pubsub.status"
        );
    }
}
