use std::sync::Arc;

use kairo_actor::{ActorSystem, LocalActorRefProvider};
use kairo_serialization::{ActorRefWireData, Registry, RemoteMessage};

use crate::local_address::CanonicalLocalAddress;
use crate::{
    RemoteActorRef, RemoteError, RemoteOutbound, RemoteSettings, ResolvedActorRef, Result,
};

#[derive(Clone)]
pub struct RemoteActorRefProvider {
    system_name: String,
    local_provider: Option<LocalActorRefProvider>,
    canonical_address: Option<CanonicalLocalAddress>,
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
            local_provider: None,
            canonical_address: None,
            settings,
            registry,
            outbound,
        }
    }

    pub fn with_actor_system(
        system: ActorSystem,
        settings: RemoteSettings,
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        Self::with_local_provider(system.provider(), settings, registry, outbound)
    }

    pub fn with_local_provider(
        local_provider: LocalActorRefProvider,
        settings: RemoteSettings,
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        let system = local_provider.system();
        Self {
            system_name: system.name().to_string(),
            canonical_address: Some(CanonicalLocalAddress::from_system_settings(
                system,
                settings.clone(),
            )),
            local_provider: Some(local_provider),
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

    pub fn resolve_actor_ref<M>(&self, path: impl Into<String>) -> Result<ResolvedActorRef<M>>
    where
        M: RemoteMessage,
    {
        let wire = ActorRefWireData::new(path.into())?;
        self.resolve_actor_ref_wire(wire)
    }

    pub fn resolve_actor_ref_wire<M>(&self, wire: ActorRefWireData) -> Result<ResolvedActorRef<M>>
    where
        M: RemoteMessage,
    {
        if let Some(local_path) = self.local_path_for_owned_address(&wire) {
            let local_provider = self
                .local_provider
                .as_ref()
                .expect("owned local address requires local provider");
            return Ok(ResolvedActorRef::Local(
                local_provider.system().resolve_local_or_missing(local_path),
            ));
        }

        self.resolve_wire(wire).map(ResolvedActorRef::Remote)
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

    fn local_path_for_owned_address(&self, wire: &ActorRefWireData) -> Option<String> {
        let system = self.local_provider.as_ref()?.system();
        if wire.host().is_none()
            && wire.protocol() == system.address().protocol()
            && wire.system() == system.name()
        {
            return Some(wire.path().to_string());
        }

        self.canonical_address
            .as_ref()
            .and_then(|canonical| canonical.local_recipient_path(wire))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use bytes::Bytes;
    use kairo_actor::{Actor, ActorResult, Context, Props};
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

    struct Probe {
        received: mpsc::Sender<u8>,
    }

    impl Actor for Probe {
        type Msg = LocalCmd;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            self.received
                .send(msg.value)
                .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct LocalCmd {
        value: u8,
    }

    impl RemoteMessage for LocalCmd {
        const MANIFEST: &'static str = "kairo.remote.test.LocalCmd";
        const VERSION: u16 = 1;
    }

    struct LocalCmdCodec;

    impl MessageCodec<LocalCmd> for LocalCmdCodec {
        fn serializer_id(&self) -> u32 {
            89
        }

        fn encode(&self, message: &LocalCmd) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(vec![message.value]))
        }

        fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<LocalCmd> {
            Ok(LocalCmd { value: payload[0] })
        }
    }

    fn provider_with_system(system: ActorSystem) -> RemoteActorRefProvider {
        provider_with_local_provider(system.provider())
    }

    fn provider_with_local_provider(
        local_provider: LocalActorRefProvider,
    ) -> RemoteActorRefProvider {
        let mut registry = Registry::new();
        registry.register::<LocalCmd, _>(LocalCmdCodec).unwrap();
        RemoteActorRefProvider::with_local_provider(
            local_provider,
            RemoteSettings::new("127.0.0.1", 25520),
            Arc::new(registry),
            Arc::new(DropOutbound),
        )
    }

    fn provider_with_empty_registry(system: ActorSystem) -> RemoteActorRefProvider {
        RemoteActorRefProvider::with_actor_system(
            system,
            RemoteSettings::new("127.0.0.1", 25520),
            Arc::new(Registry::new()),
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

    #[test]
    fn provider_resolves_local_only_path_through_actor_system() {
        let system = ActorSystem::builder("local").build().unwrap();
        let provider = RemoteActorRefProvider::with_actor_system(
            system.clone(),
            RemoteSettings::new("127.0.0.1", 25520),
            Arc::new({
                let mut registry = Registry::new();
                registry.register::<LocalCmd, _>(LocalCmdCodec).unwrap();
                registry
            }),
            Arc::new(DropOutbound),
        );
        let (received_tx, received_rx) = mpsc::channel();
        let target = system
            .spawn(
                "target",
                Props::new(move || Probe {
                    received: received_tx,
                }),
            )
            .unwrap();

        let resolved = provider
            .resolve_actor_ref::<LocalCmd>(target.path().to_string())
            .unwrap();

        assert!(resolved.is_local());
        resolved.tell(LocalCmd { value: 7 }).unwrap();
        assert_eq!(received_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 7);
    }

    #[test]
    fn provider_local_resolution_does_not_require_registered_codec() {
        let system = ActorSystem::builder("local").build().unwrap();
        let provider = provider_with_empty_registry(system.clone());
        let (received_tx, received_rx) = mpsc::channel();
        let target = system
            .spawn(
                "target",
                Props::new(move || Probe {
                    received: received_tx,
                }),
            )
            .unwrap();

        let resolved = provider
            .resolve_actor_ref::<LocalCmd>(target.path().to_string())
            .unwrap();

        assert!(resolved.is_local());
        resolved.tell(LocalCmd { value: 13 }).unwrap();
        assert_eq!(
            received_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            13
        );
    }

    #[test]
    fn provider_resolves_local_only_path_through_local_provider_boundary() {
        let system = ActorSystem::builder("local").build().unwrap();
        let provider = provider_with_local_provider(system.provider());
        let (received_tx, received_rx) = mpsc::channel();
        let target = system
            .spawn(
                "target",
                Props::new(move || Probe {
                    received: received_tx,
                }),
            )
            .unwrap();

        let resolved = provider
            .resolve_actor_ref::<LocalCmd>(target.path().to_string())
            .unwrap();

        assert!(resolved.is_local());
        resolved.tell(LocalCmd { value: 11 }).unwrap();
        assert_eq!(
            received_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            11
        );
    }

    #[test]
    fn provider_maps_owned_canonical_remote_path_to_local_actor() {
        let system = ActorSystem::builder("local").build().unwrap();
        let provider = provider_with_system(system.clone());
        let (received_tx, received_rx) = mpsc::channel();
        let target = system
            .spawn(
                "target",
                Props::new(move || Probe {
                    received: received_tx,
                }),
            )
            .unwrap();
        let canonical_path =
            target
                .path()
                .as_str()
                .replacen("kairo://local", "kairo://local@127.0.0.1:25520", 1);

        let resolved = provider
            .resolve_actor_ref::<LocalCmd>(canonical_path)
            .unwrap();

        assert!(resolved.is_local());
        resolved.tell(LocalCmd { value: 8 }).unwrap();
        assert_eq!(received_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 8);
    }

    #[test]
    fn provider_resolves_unknown_owned_path_to_missing_local_ref() {
        let system = ActorSystem::builder("local").build().unwrap();
        let provider = provider_with_system(system.clone());

        let resolved = provider
            .resolve_actor_ref::<LocalCmd>("kairo://local/user/missing#42")
            .unwrap();

        assert!(resolved.is_local());
        assert_eq!(resolved.path().as_str(), "kairo://local/user/missing#42");
        let error = resolved
            .tell(LocalCmd { value: 9 })
            .expect_err("missing local ref should reject");
        assert_eq!(error.reason(), "actor does not exist");
        assert!(
            system
                .dead_letters()
                .wait_for_len(1, Duration::from_secs(1))
        );
        assert_eq!(
            system.dead_letters().records()[0].recipient().as_str(),
            "kairo://local/user/missing#42"
        );
    }

    #[test]
    fn provider_maps_owned_canonical_missing_path_to_local_missing_ref_without_codec() {
        let system = ActorSystem::builder("local").build().unwrap();
        let provider = provider_with_empty_registry(system.clone());

        let resolved = provider
            .resolve_actor_ref::<LocalCmd>("kairo://local@127.0.0.1:25520/user/missing#42")
            .unwrap();

        assert!(resolved.is_local());
        assert_eq!(resolved.path().as_str(), "kairo://local/user/missing#42");
        let error = resolved
            .tell(LocalCmd { value: 9 })
            .expect_err("missing canonical local ref should reject");
        assert_eq!(error.reason(), "actor does not exist");
        assert!(
            system
                .dead_letters()
                .wait_for_len(1, Duration::from_secs(1))
        );
        assert_eq!(
            system.dead_letters().records()[0].recipient().as_str(),
            "kairo://local/user/missing#42"
        );
    }

    #[test]
    fn provider_resolve_actor_ref_keeps_foreign_paths_remote() {
        let system = ActorSystem::builder("local").build().unwrap();
        let provider = provider_with_system(system);

        let resolved = provider
            .resolve_actor_ref::<LocalCmd>("kairo://remote@127.0.0.1:25521/user/worker")
            .unwrap();

        assert!(resolved.is_remote());
        assert_eq!(
            resolved.path().as_str(),
            "kairo://remote@127.0.0.1:25521/user/worker"
        );
    }
}
