use std::marker::PhantomData;
use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_cluster::UniqueAddress;
use kairo_remote::RemoteOutbound;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{LocalPubSubMsg, PubSubPublishEnvelope, TopicPublishMode};

use super::{DEFAULT_PUBSUB_REMOTE_PATH, PubSubRemoteDeliveryError, recipient_for_node};

#[derive(Clone)]
pub struct PubSubRemoteDeliveryOutbound<M> {
    target: UniqueAddress,
    registry: Arc<Registry>,
    recipient_path: String,
    sender: Option<ActorRefWireData>,
    outbound: Arc<dyn RemoteOutbound>,
    _message: PhantomData<fn(M)>,
}

impl<M> PubSubRemoteDeliveryOutbound<M> {
    pub fn new(
        target: UniqueAddress,
        registry: Arc<Registry>,
        outbound: impl RemoteOutbound + 'static,
    ) -> Self {
        Self::from_arc(target, registry, Arc::new(outbound))
    }

    pub fn from_arc(
        target: UniqueAddress,
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        Self {
            target,
            registry,
            recipient_path: DEFAULT_PUBSUB_REMOTE_PATH.to_string(),
            sender: None,
            outbound,
            _message: PhantomData,
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

    pub fn target(&self) -> &UniqueAddress {
        &self.target
    }

    pub fn recipient_for_target(&self) -> Result<ActorRefWireData, PubSubRemoteDeliveryError> {
        recipient_for_node(&self.target, &self.recipient_path)
    }

    fn send_envelope(
        &self,
        envelope: PubSubPublishEnvelope,
    ) -> Result<(), PubSubRemoteDeliveryError> {
        let recipient = self.recipient_for_target()?;
        let serialized = self.registry.serialize(&envelope)?;
        let remote = RemoteEnvelope::new(recipient, self.sender.clone(), serialized);
        self.outbound
            .send(remote)
            .map_err(|error| PubSubRemoteDeliveryError::Send {
                target: self.target.ordering_key(),
                reason: error.to_string(),
            })
    }
}

impl<M> PubSubRemoteDeliveryOutbound<M>
where
    M: RemoteMessage,
{
    fn publish_envelope(
        &self,
        topic: crate::TopicName,
        group: Option<String>,
        message: &M,
    ) -> Result<PubSubPublishEnvelope, PubSubRemoteDeliveryError> {
        Ok(PubSubPublishEnvelope {
            topic,
            group,
            message: self.registry.serialize(message)?,
        })
    }
}

impl<M> Recipient<LocalPubSubMsg<M>> for PubSubRemoteDeliveryOutbound<M>
where
    M: RemoteMessage + Send + 'static,
{
    fn tell(&self, message: LocalPubSubMsg<M>) -> Result<(), SendError<LocalPubSubMsg<M>>> {
        match message {
            LocalPubSubMsg::Publish {
                topic,
                message,
                mode: TopicPublishMode::Broadcast,
                reply_to,
            } => match self.publish_envelope(topic.clone(), None, &message) {
                Ok(envelope) => self.send_envelope(envelope).map_err(|error| {
                    SendError::new(
                        LocalPubSubMsg::Publish {
                            topic,
                            message,
                            mode: TopicPublishMode::Broadcast,
                            reply_to,
                        },
                        error.to_string(),
                    )
                }),
                Err(error) => Err(SendError::new(
                    LocalPubSubMsg::Publish {
                        topic,
                        message,
                        mode: TopicPublishMode::Broadcast,
                        reply_to,
                    },
                    error.to_string(),
                )),
            },
            LocalPubSubMsg::Publish {
                topic,
                message,
                mode: TopicPublishMode::OnePerGroup,
                reply_to,
            } => Err(SendError::new(
                LocalPubSubMsg::Publish {
                    topic,
                    message,
                    mode: TopicPublishMode::OnePerGroup,
                    reply_to,
                },
                PubSubRemoteDeliveryError::UnsupportedLocalMessage("publish-one-per-group")
                    .to_string(),
            )),
            LocalPubSubMsg::PublishGroup {
                topic,
                group,
                message,
                reply_to,
            } => match self.publish_envelope(topic.clone(), Some(group.clone()), &message) {
                Ok(envelope) => self.send_envelope(envelope).map_err(|error| {
                    SendError::new(
                        LocalPubSubMsg::PublishGroup {
                            topic,
                            group,
                            message,
                            reply_to,
                        },
                        error.to_string(),
                    )
                }),
                Err(error) => Err(SendError::new(
                    LocalPubSubMsg::PublishGroup {
                        topic,
                        group,
                        message,
                        reply_to,
                    },
                    error.to_string(),
                )),
            },
            LocalPubSubMsg::Subscribe {
                topic,
                subscriber,
                reply_to,
            } => Err(SendError::new(
                LocalPubSubMsg::Subscribe {
                    topic,
                    subscriber,
                    reply_to,
                },
                PubSubRemoteDeliveryError::UnsupportedLocalMessage("subscribe").to_string(),
            )),
            LocalPubSubMsg::SubscribeGroup {
                topic,
                group,
                subscriber,
                reply_to,
            } => Err(SendError::new(
                LocalPubSubMsg::SubscribeGroup {
                    topic,
                    group,
                    subscriber,
                    reply_to,
                },
                PubSubRemoteDeliveryError::UnsupportedLocalMessage("subscribe-group").to_string(),
            )),
            LocalPubSubMsg::Unsubscribe {
                topic,
                subscriber,
                reply_to,
            } => Err(SendError::new(
                LocalPubSubMsg::Unsubscribe {
                    topic,
                    subscriber,
                    reply_to,
                },
                PubSubRemoteDeliveryError::UnsupportedLocalMessage("unsubscribe").to_string(),
            )),
            LocalPubSubMsg::UnsubscribeGroup {
                topic,
                group,
                subscriber,
                reply_to,
            } => Err(SendError::new(
                LocalPubSubMsg::UnsubscribeGroup {
                    topic,
                    group,
                    subscriber,
                    reply_to,
                },
                PubSubRemoteDeliveryError::UnsupportedLocalMessage("unsubscribe-group").to_string(),
            )),
            LocalPubSubMsg::GetTopics { reply_to } => Err(SendError::new(
                LocalPubSubMsg::GetTopics { reply_to },
                PubSubRemoteDeliveryError::UnsupportedLocalMessage("get-topics").to_string(),
            )),
            LocalPubSubMsg::RemoveSubscriber {
                subscriber,
                reply_to,
            } => Err(SendError::new(
                LocalPubSubMsg::RemoveSubscriber {
                    subscriber,
                    reply_to,
                },
                PubSubRemoteDeliveryError::UnsupportedLocalMessage("remove-subscriber").to_string(),
            )),
        }
    }
}
