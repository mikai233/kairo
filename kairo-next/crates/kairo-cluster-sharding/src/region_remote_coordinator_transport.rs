use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::Recipient;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope};

use crate::{
    GetShardHome, ShardCoordinatorRemoteHomeError, ShardCoordinatorRemoteHomeOutbound,
    ShardCoordinatorRemoteRegistrationError, ShardCoordinatorRemoteRegistrationOutbound,
    ShardCoordinatorRemoteTarget,
};

#[derive(Debug)]
pub enum RegionRemoteCoordinatorTransportError {
    Registration(ShardCoordinatorRemoteRegistrationError),
    ShardHome(ShardCoordinatorRemoteHomeError),
}

impl Display for RegionRemoteCoordinatorTransportError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registration(error) => {
                write!(f, "remote coordinator registration send failed: {error}")
            }
            Self::ShardHome(error) => {
                write!(f, "remote coordinator shard-home send failed: {error}")
            }
        }
    }
}

impl std::error::Error for RegionRemoteCoordinatorTransportError {}

#[derive(Clone)]
pub struct RegionRemoteCoordinatorTransport {
    region: ActorRefWireData,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
}

impl RegionRemoteCoordinatorTransport {
    pub fn new(
        region: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: impl Recipient<RemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::from_arc(region, registry, Arc::new(outbound))
    }

    pub fn from_arc(
        region: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    ) -> Self {
        Self {
            region,
            registry,
            outbound,
        }
    }

    pub fn region(&self) -> &ActorRefWireData {
        &self.region
    }

    pub fn register(
        &self,
        target: &ShardCoordinatorRemoteTarget,
    ) -> Result<(), RegionRemoteCoordinatorTransportError> {
        ShardCoordinatorRemoteRegistrationOutbound::from_arc(
            target.clone(),
            self.region.clone(),
            self.registry.clone(),
            self.outbound.clone(),
        )
        .register()
        .map_err(RegionRemoteCoordinatorTransportError::Registration)
    }

    pub fn request_shard_home(
        &self,
        target: &ShardCoordinatorRemoteTarget,
        request: GetShardHome,
    ) -> Result<(), RegionRemoteCoordinatorTransportError> {
        ShardCoordinatorRemoteHomeOutbound::from_arc(
            target.clone(),
            self.region.clone(),
            self.registry.clone(),
            self.outbound.clone(),
        )
        .send_request(request)
        .map_err(RegionRemoteCoordinatorTransportError::ShardHome)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc::{self, Receiver};

    use kairo_actor::{Address, Recipient, SendError};
    use kairo_cluster::UniqueAddress;
    use kairo_serialization::RemoteMessage;

    use crate::{
        DEFAULT_SHARD_COORDINATOR_REMOTE_PATH, GET_SHARD_HOME_SERIALIZER_ID,
        REGISTER_SERIALIZER_ID, Register, ShardCoordinatorRemoteTarget,
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
    fn remote_coordinator_transport_sends_register_and_get_shard_home() {
        let registry = registry();
        let target = target();
        let (outbound, rx) = collector::<RemoteEnvelope>();
        let transport = RegionRemoteCoordinatorTransport::new(region(), registry, outbound);

        transport.register(&target).unwrap();
        transport
            .request_shard_home(
                &target,
                GetShardHome {
                    shard_id: "12".to_string(),
                },
            )
            .unwrap();

        let register = rx.recv().unwrap();
        assert_eq!(register.recipient, target.recipient().clone());
        assert_eq!(register.sender, Some(region()));
        assert_eq!(register.message.serializer_id, REGISTER_SERIALIZER_ID);
        assert_eq!(register.message.manifest.as_str(), Register::MANIFEST);

        let home = rx.recv().unwrap();
        assert_eq!(home.recipient, target.recipient().clone());
        assert_eq!(home.sender, Some(region()));
        assert_eq!(home.message.serializer_id, GET_SHARD_HOME_SERIALIZER_ID);
        assert_eq!(home.message.manifest.as_str(), GetShardHome::MANIFEST);
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
