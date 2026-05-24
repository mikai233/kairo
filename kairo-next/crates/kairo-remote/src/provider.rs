use std::sync::Arc;

use kairo_serialization::{ActorRefWireData, Registry, RemoteMessage};

use crate::{RemoteActorRef, RemoteError, RemoteOutbound, RemoteSettings, Result};

#[derive(Clone)]
pub struct RemoteActorRefProvider {
    system_name: String,
    settings: RemoteSettings,
    registry: Arc<Registry>,
    outbound: Arc<dyn RemoteOutbound>,
}

impl RemoteActorRefProvider {
    pub fn new(
        system_name: impl Into<String>,
        settings: RemoteSettings,
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        Self {
            system_name: system_name.into(),
            settings,
            registry,
            outbound,
        }
    }

    pub fn system_name(&self) -> &str {
        &self.system_name
    }

    pub fn settings(&self) -> &RemoteSettings {
        &self.settings
    }

    pub fn resolve<M>(&self, path: impl Into<String>) -> Result<RemoteActorRef<M>>
    where
        M: RemoteMessage,
    {
        let wire = ActorRefWireData::new(path.into())?;
        self.resolve_wire(wire)
    }

    pub fn resolve_wire<M>(&self, wire: ActorRefWireData) -> Result<RemoteActorRef<M>>
    where
        M: RemoteMessage,
    {
        if wire.host().is_none() {
            return Err(RemoteError::LocalAddress(wire.path().to_string()));
        }
        if wire.system().is_empty() || wire.protocol().is_empty() {
            return Err(RemoteError::InvalidRemoteRef(
                wire.path().to_string(),
                "missing protocol or system".to_string(),
            ));
        }

        Ok(RemoteActorRef::new(
            wire,
            Arc::clone(&self.registry),
            Arc::clone(&self.outbound),
        ))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kairo_serialization::{MessageCodec, RemoteEnvelope, SerializationRegistry};

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct RemoteCmd;

    impl RemoteMessage for RemoteCmd {
        const MANIFEST: &'static str = "kairo.remote.test.RemoteCmd";
        const VERSION: u16 = 1;
    }

    struct UnitCodec;

    impl MessageCodec<RemoteCmd> for UnitCodec {
        fn serializer_id(&self) -> u32 {
            88
        }

        fn encode(&self, _message: &RemoteCmd) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::new())
        }

        fn decode(&self, _payload: Bytes, _version: u16) -> kairo_serialization::Result<RemoteCmd> {
            Ok(RemoteCmd)
        }
    }

    struct DropOutbound;

    impl RemoteOutbound for DropOutbound {
        fn send(&self, _envelope: RemoteEnvelope) -> Result<()> {
            Ok(())
        }
    }

    fn provider() -> RemoteActorRefProvider {
        let mut registry = Registry::new();
        registry.register::<RemoteCmd, _>(UnitCodec).unwrap();
        RemoteActorRefProvider::new(
            "local",
            RemoteSettings::new("127.0.0.1", 25520),
            Arc::new(registry),
            Arc::new(DropOutbound),
        )
    }

    #[test]
    fn provider_resolves_remote_path_to_typed_remote_ref() {
        let remote_ref = provider()
            .resolve::<RemoteCmd>("kairo://remote@127.0.0.1:25521/user/worker")
            .unwrap();

        assert_eq!(
            remote_ref.path().as_str(),
            "kairo://remote@127.0.0.1:25521/user/worker"
        );
        assert_eq!(remote_ref.recipient().host(), Some("127.0.0.1"));
    }

    #[test]
    fn provider_rejects_local_only_paths() {
        let error = provider()
            .resolve::<RemoteCmd>("kairo://local/user/worker")
            .expect_err("local path should not create remote ref");

        assert!(matches!(error, RemoteError::LocalAddress(_)));
    }
}
