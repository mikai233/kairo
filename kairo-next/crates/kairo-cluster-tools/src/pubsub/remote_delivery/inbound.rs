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

    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), PubSubRemoteDeliveryError>
    where
        M: RemoteMessage,
    {
        validate_recipient(&self.self_node, &self.recipient_path, &envelope.recipient)?;
        self.receive_message(envelope.message)
    }

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
