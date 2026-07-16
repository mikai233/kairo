use std::marker::PhantomData;
use std::sync::Arc;

use kairo_actor::ActorRef;
use kairo_cluster::UniqueAddress;
use kairo_serialization::{Registry, RemoteEnvelope, RemoteMessage, SerializedMessage};

use crate::{
    DistributedPubSubMediatorMsg, LocalPubSubMsg, PubSubPathEnvelope, PubSubPublishEnvelope,
    TopicPublishMode,
};

use super::{DEFAULT_PUBSUB_REMOTE_PATH, PubSubRemoteDeliveryError, validate_recipient};

/// Typed inbound adapter for remote pubsub business-delivery envelopes.
///
/// Stable outer envelopes are decoded through the shared registry, then the
/// nested business message is decoded as `M` and re-enters the local mediator
/// through its typed mailbox. Registration and subscription commands are never
/// accepted on this boundary.
#[derive(Clone)]
pub struct PubSubRemoteDeliveryInbound<M>
where
    M: Send + 'static,
{
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    recipient_path: String,
    mediator: ActorRef<DistributedPubSubMediatorMsg<M>>,
    _message: PhantomData<fn(M)>,
}

impl<M> PubSubRemoteDeliveryInbound<M>
where
    M: Send + 'static,
{
    /// Creates an inbound adapter for this member's canonical pubsub endpoint.
    pub fn new(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        mediator: ActorRef<DistributedPubSubMediatorMsg<M>>,
    ) -> Self {
        Self {
            self_node,
            registry,
            recipient_path: DEFAULT_PUBSUB_REMOTE_PATH.to_string(),
            mediator,
            _message: PhantomData,
        }
    }

    /// Overrides the absolute recipient path validated on inbound envelopes.
    ///
    /// Path validity is checked when an envelope is received.
    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    /// Validates a canonical remote recipient and dispatches its stable payload.
    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), PubSubRemoteDeliveryError>
    where
        M: RemoteMessage,
    {
        validate_recipient(&self.self_node, &self.recipient_path, &envelope.recipient)?;
        self.receive_message(envelope.message)
    }

    /// Decodes and dispatches a stable payload without recipient validation.
    ///
    /// Use this only when a path-indexed remoting router has already validated
    /// the canonical recipient. Direct callers should prefer [`Self::receive`].
    pub fn receive_message(
        &self,
        message: SerializedMessage,
    ) -> Result<(), PubSubRemoteDeliveryError>
    where
        M: RemoteMessage,
    {
        match message.manifest.as_str() {
            PubSubPublishEnvelope::MANIFEST => {
                let envelope = self
                    .registry
                    .deserialize::<PubSubPublishEnvelope>(message)?;
                let business = self.registry.deserialize::<M>(envelope.message)?;
                let delivery = match envelope.group {
                    Some(group) => LocalPubSubMsg::PublishGroup {
                        topic: envelope.topic,
                        group,
                        message: business,
                        reply_to: None,
                    },
                    None => LocalPubSubMsg::Publish {
                        topic: envelope.topic,
                        message: business,
                        mode: TopicPublishMode::Broadcast,
                        reply_to: None,
                    },
                };
                self.tell_mediator(delivery)
            }
            PubSubPathEnvelope::MANIFEST => {
                let envelope = self.registry.deserialize::<PubSubPathEnvelope>(message)?;
                let business = self.registry.deserialize::<M>(envelope.message)?;
                let delivery = if envelope.all {
                    LocalPubSubMsg::SendToAll {
                        path: envelope.path,
                        message: business,
                        reply_to: None,
                    }
                } else {
                    LocalPubSubMsg::Send {
                        path: envelope.path,
                        message: business,
                        reply_to: None,
                    }
                };
                self.tell_mediator(delivery)
            }
            manifest => Err(PubSubRemoteDeliveryError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }

    fn tell_mediator(&self, delivery: LocalPubSubMsg<M>) -> Result<(), PubSubRemoteDeliveryError> {
        self.mediator
            .tell(DistributedPubSubMediatorMsg::LocalDelivery(delivery))
            .map_err(|error| PubSubRemoteDeliveryError::Send {
                target: self.mediator.path().to_string(),
                reason: error.reason().to_string(),
            })
    }
}
