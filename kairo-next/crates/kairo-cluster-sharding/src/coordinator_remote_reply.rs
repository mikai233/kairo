use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, SerializationError, SerializedMessage,
};

use crate::{RegisterAck, ShardHome, ShardId};

#[derive(Debug)]
pub enum CoordinatorRemoteReplyError {
    Serialization(SerializationError),
    Send { target: String, reason: String },
}

impl Display for CoordinatorRemoteReplyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => {
                write!(
                    f,
                    "shard-coordinator remote reply serialization failed: {error}"
                )
            }
            Self::Send { target, reason } => {
                write!(
                    f,
                    "shard-coordinator remote reply send to `{target}` failed: {reason}"
                )
            }
        }
    }
}

impl std::error::Error for CoordinatorRemoteReplyError {}

impl From<SerializationError> for CoordinatorRemoteReplyError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
pub struct CoordinatorRemoteReplyTarget {
    coordinator: ActorRefWireData,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
}

impl CoordinatorRemoteReplyTarget {
    pub fn new(
        coordinator: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: impl Recipient<RemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::from_arc(coordinator, registry, Arc::new(outbound))
    }

    pub fn from_arc(
        coordinator: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    ) -> Self {
        Self {
            coordinator,
            registry,
            outbound,
        }
    }

    pub fn coordinator(&self) -> &ActorRefWireData {
        &self.coordinator
    }

    pub fn send_register_ack(
        &self,
        region: ActorRefWireData,
    ) -> Result<(), CoordinatorRemoteReplyError> {
        self.send_to(
            region,
            self.registry.serialize(&RegisterAck {
                coordinator: self.coordinator.clone(),
            })?,
        )
    }

    pub fn send_shard_home(
        &self,
        shard: ShardId,
        region: ActorRefWireData,
        home_region: ActorRefWireData,
    ) -> Result<(), CoordinatorRemoteReplyError> {
        self.send_to(
            region,
            self.registry.serialize(&ShardHome {
                shard_id: shard,
                region: home_region,
            })?,
        )
    }

    fn send_to(
        &self,
        recipient: ActorRefWireData,
        message: SerializedMessage,
    ) -> Result<(), CoordinatorRemoteReplyError> {
        let target = recipient.path().to_string();
        self.outbound
            .tell(RemoteEnvelope::new(
                recipient,
                Some(self.coordinator.clone()),
                message,
            ))
            .map_err(
                |error: SendError<RemoteEnvelope>| CoordinatorRemoteReplyError::Send {
                    target,
                    reason: error.reason().to_string(),
                },
            )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{self, Receiver};

    use kairo_actor::{Recipient, SendError};
    use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

    use crate::{
        REGISTER_ACK_SERIALIZER_ID, RegisterAck, SHARD_HOME_SERIALIZER_ID, ShardHome,
        register_sharding_protocol_codecs,
    };

    use super::*;

    struct CollectingRecipient<M> {
        tx: mpsc::Sender<M>,
    }

    impl<M> Recipient<M> for CollectingRecipient<M>
    where
        M: Send + 'static,
    {
        fn tell(&self, message: M) -> Result<(), SendError<M>> {
            self.tx
                .send(message)
                .map_err(|error| SendError::new(error.0, "collector closed"))
        }
    }

    fn collector<M>() -> (CollectingRecipient<M>, Receiver<M>)
    where
        M: Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        (CollectingRecipient { tx }, rx)
    }

    #[test]
    fn remote_reply_target_sends_register_ack_and_shard_home() {
        let registry = registry();
        let (outbound, rx) = collector::<RemoteEnvelope>();
        let reply = CoordinatorRemoteReplyTarget::new(coordinator(), registry.clone(), outbound);

        reply.send_register_ack(region()).unwrap();
        reply
            .send_shard_home("12".to_string(), region(), region())
            .unwrap();

        let ack = rx.recv().unwrap();
        assert_eq!(ack.recipient, region());
        assert_eq!(ack.sender, Some(coordinator()));
        assert_eq!(ack.message.serializer_id, REGISTER_ACK_SERIALIZER_ID);
        assert_eq!(ack.message.manifest.as_str(), RegisterAck::MANIFEST);

        let home = rx.recv().unwrap();
        assert_eq!(home.recipient, region());
        assert_eq!(home.sender, Some(coordinator()));
        assert_eq!(home.message.serializer_id, SHARD_HOME_SERIALIZER_ID);
        assert_eq!(home.message.manifest.as_str(), ShardHome::MANIFEST);
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_sharding_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn coordinator() -> ActorRefWireData {
        ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/coordinator").unwrap()
    }

    fn region() -> ActorRefWireData {
        ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/region").unwrap()
    }
}
