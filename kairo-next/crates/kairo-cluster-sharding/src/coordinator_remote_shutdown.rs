use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, SerializationError};

use crate::{GracefulShutdownReq, RegionStopped, ShardCoordinatorRemoteTarget};

#[derive(Debug)]
pub enum ShardCoordinatorRemoteShutdownError {
    Serialization(SerializationError),
    Send { target: String, reason: String },
}

impl Display for ShardCoordinatorRemoteShutdownError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => {
                write!(
                    f,
                    "shard-coordinator remote shutdown serialization failed: {error}"
                )
            }
            Self::Send { target, reason } => {
                write!(
                    f,
                    "shard-coordinator remote shutdown send to `{target}` failed: {reason}"
                )
            }
        }
    }
}

impl std::error::Error for ShardCoordinatorRemoteShutdownError {}

impl From<SerializationError> for ShardCoordinatorRemoteShutdownError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
pub struct ShardCoordinatorRemoteShutdownOutbound {
    target: ShardCoordinatorRemoteTarget,
    region: ActorRefWireData,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
}

impl ShardCoordinatorRemoteShutdownOutbound {
    pub fn new(
        target: ShardCoordinatorRemoteTarget,
        region: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: impl Recipient<RemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::from_arc(target, region, registry, Arc::new(outbound))
    }

    pub fn from_arc(
        target: ShardCoordinatorRemoteTarget,
        region: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    ) -> Self {
        Self {
            target,
            region,
            registry,
            outbound,
        }
    }

    pub fn graceful_shutdown(&self) -> Result<(), ShardCoordinatorRemoteShutdownError> {
        self.send(self.registry.serialize(&GracefulShutdownReq {
            region: self.region.clone(),
        })?)
    }

    pub fn region_stopped(&self) -> Result<(), ShardCoordinatorRemoteShutdownError> {
        self.send(self.registry.serialize(&RegionStopped {
            region: self.region.clone(),
        })?)
    }

    fn send(
        &self,
        message: kairo_serialization::SerializedMessage,
    ) -> Result<(), ShardCoordinatorRemoteShutdownError> {
        let target = self.target.recipient().path().to_string();
        self.outbound
            .tell(RemoteEnvelope::new(
                self.target.recipient().clone(),
                Some(self.region.clone()),
                message,
            ))
            .map_err(
                |error: SendError<RemoteEnvelope>| ShardCoordinatorRemoteShutdownError::Send {
                    target,
                    reason: error.reason().to_string(),
                },
            )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{self, Receiver};

    use kairo_actor::{Address, Recipient, SendError};
    use kairo_cluster::UniqueAddress;
    use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

    use crate::{
        DEFAULT_SHARD_COORDINATOR_REMOTE_PATH, GRACEFUL_SHUTDOWN_REQ_SERIALIZER_ID,
        GracefulShutdownReq, REGION_STOPPED_SERIALIZER_ID, RegionStopped,
        ShardCoordinatorRemoteTarget, register_sharding_protocol_codecs,
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
    fn remote_shutdown_outbound_sends_stable_shutdown_envelopes() {
        let registry = registry();
        let target = target();
        let (outbound, rx) = collector::<RemoteEnvelope>();
        let shutdown = ShardCoordinatorRemoteShutdownOutbound::new(
            target.clone(),
            region(),
            registry.clone(),
            outbound,
        );

        shutdown.graceful_shutdown().unwrap();
        shutdown.region_stopped().unwrap();

        let graceful = rx.recv().unwrap();
        assert_eq!(graceful.recipient, target.recipient().clone());
        assert_eq!(graceful.sender, Some(region()));
        assert_eq!(
            graceful.message.serializer_id,
            GRACEFUL_SHUTDOWN_REQ_SERIALIZER_ID
        );
        assert_eq!(
            graceful.message.manifest.as_str(),
            GracefulShutdownReq::MANIFEST
        );

        let stopped = rx.recv().unwrap();
        assert_eq!(stopped.recipient, target.recipient().clone());
        assert_eq!(stopped.sender, Some(region()));
        assert_eq!(stopped.message.serializer_id, REGION_STOPPED_SERIALIZER_ID);
        assert_eq!(stopped.message.manifest.as_str(), RegionStopped::MANIFEST);
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_sharding_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn target() -> ShardCoordinatorRemoteTarget {
        ShardCoordinatorRemoteTarget::for_node(
            UniqueAddress::new(
                Address::new("kairo", "remote", Some("127.0.0.1".to_string()), Some(2552)),
                2,
            ),
            DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
        )
        .unwrap()
    }

    fn region() -> ActorRefWireData {
        ActorRefWireData::new("kairo://local@127.0.0.1:2551/system/sharding/region").unwrap()
    }
}
