use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
    sync::{Arc, RwLock},
};

use kairo_serialization::{ActorRefWireData, RemoteEnvelope};

use crate::{RemoteError, RemoteOutbound, Result};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RemoteAssociationAddress {
    protocol: String,
    system: String,
    host: String,
    port: Option<u16>,
}

impl RemoteAssociationAddress {
    pub fn new(
        protocol: impl Into<String>,
        system: impl Into<String>,
        host: impl Into<String>,
        port: Option<u16>,
    ) -> Result<Self> {
        let address = Self {
            protocol: protocol.into(),
            system: system.into(),
            host: host.into(),
            port,
        };
        if address.protocol.is_empty() || address.system.is_empty() || address.host.is_empty() {
            return Err(RemoteError::InvalidRemoteRef(
                address.to_string(),
                "remote association address requires protocol, system, and host".to_string(),
            ));
        }
        Ok(address)
    }

    pub fn from_actor_ref(wire: &ActorRefWireData) -> Result<Self> {
        let Some(host) = wire.host() else {
            return Err(RemoteError::LocalAddress(wire.path().to_string()));
        };
        Self::new(wire.protocol(), wire.system(), host, wire.port())
    }

    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    pub fn system(&self) -> &str {
        &self.system
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> Option<u16> {
        self.port
    }
}

impl Display for RemoteAssociationAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}@{}", self.protocol, self.system, self.host)?;
        if let Some(port) = self.port {
            write!(f, ":{port}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct RemoteAssociationCache {
    routes: Arc<RwLock<BTreeMap<RemoteAssociationAddress, Arc<dyn RemoteOutbound>>>>,
}

impl RemoteAssociationCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_route(
        &self,
        address: RemoteAssociationAddress,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Option<Arc<dyn RemoteOutbound>> {
        self.routes
            .write()
            .expect("remote association cache lock poisoned")
            .insert(address, outbound)
    }

    pub fn insert_route_for_actor_ref(
        &self,
        wire: &ActorRefWireData,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Result<Option<Arc<dyn RemoteOutbound>>> {
        let address = RemoteAssociationAddress::from_actor_ref(wire)?;
        Ok(self.insert_route(address, outbound))
    }

    pub fn remove_route(
        &self,
        address: &RemoteAssociationAddress,
    ) -> Option<Arc<dyn RemoteOutbound>> {
        self.routes
            .write()
            .expect("remote association cache lock poisoned")
            .remove(address)
    }

    pub fn route_count(&self) -> usize {
        self.routes
            .read()
            .expect("remote association cache lock poisoned")
            .len()
    }

    pub fn address_for_recipient(
        &self,
        recipient: &ActorRefWireData,
    ) -> Result<RemoteAssociationAddress> {
        RemoteAssociationAddress::from_actor_ref(recipient)
    }

    pub fn route_for_recipient(
        &self,
        recipient: &ActorRefWireData,
    ) -> Result<Arc<dyn RemoteOutbound>> {
        let address = self.address_for_recipient(recipient)?;
        self.routes
            .read()
            .expect("remote association cache lock poisoned")
            .get(&address)
            .cloned()
            .ok_or_else(|| RemoteError::AssociationUnavailable {
                remote: address.to_string(),
            })
    }

    pub fn send_to_recipient(&self, envelope: RemoteEnvelope) -> Result<()> {
        let route = self.route_for_recipient(&envelope.recipient)?;
        route.send(envelope)
    }
}

impl RemoteOutbound for RemoteAssociationCache {
    fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
        self.send_to_recipient(envelope)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use kairo_serialization::{ActorRefWireData, Manifest, SerializedMessage};

    use super::*;
    use crate::{AssociationRemoteOutbound, RemoteAssociation};

    #[derive(Default)]
    struct CollectingOutbound {
        sent: Mutex<Vec<RemoteEnvelope>>,
    }

    impl CollectingOutbound {
        fn sent(&self) -> Vec<RemoteEnvelope> {
            self.sent
                .lock()
                .expect("collecting outbound mutex poisoned")
                .clone()
        }
    }

    impl RemoteOutbound for CollectingOutbound {
        fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
            self.sent
                .lock()
                .expect("collecting outbound mutex poisoned")
                .push(envelope);
            Ok(())
        }
    }

    fn envelope(recipient: &str, value: u8) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new(recipient).unwrap(),
            None,
            SerializedMessage::new(
                702,
                Manifest::new("kairo.remote.test.AssociationCache"),
                1,
                Bytes::from(vec![value]),
            ),
        )
    }

    #[test]
    fn routes_by_recipient_remote_address() {
        let cache = RemoteAssociationCache::new();
        let outbound = Arc::new(CollectingOutbound::default());
        let target = envelope("kairo://remote@127.0.0.1:25520/user/target", 1);

        cache
            .insert_route_for_actor_ref(
                &target.recipient,
                outbound.clone() as Arc<dyn RemoteOutbound>,
            )
            .unwrap();
        cache.send(target.clone()).unwrap();

        let sent = outbound.sent();
        assert_eq!(sent, vec![target]);
    }

    #[test]
    fn cloned_caches_share_routes_inserted_later() {
        let cache = RemoteAssociationCache::new();
        let cloned = cache.clone();
        let outbound = Arc::new(CollectingOutbound::default());
        let target = envelope("kairo://remote@127.0.0.1:25520/user/late", 2);

        cache
            .insert_route_for_actor_ref(
                &target.recipient,
                outbound.clone() as Arc<dyn RemoteOutbound>,
            )
            .unwrap();
        cloned.send(target.clone()).unwrap();

        assert_eq!(cloned.route_count(), 1);
        assert_eq!(outbound.sent(), vec![target]);
    }

    #[test]
    fn rejects_local_only_recipient() {
        let cache = RemoteAssociationCache::new();
        let target = envelope("kairo://local/user/target", 3);

        let error = cache
            .send(target)
            .expect_err("local-only recipient must not be routed remotely");

        assert!(matches!(error, RemoteError::LocalAddress(_)));
    }

    #[test]
    fn reports_missing_association_route() {
        let cache = RemoteAssociationCache::new();
        let target = envelope("kairo://missing@127.0.0.1:25521/user/target", 4);

        let error = cache
            .send(target)
            .expect_err("missing association route should be explicit");

        assert!(matches!(
            error,
            RemoteError::AssociationUnavailable { remote }
                if remote == "kairo://missing@127.0.0.1:25521"
        ));
    }

    #[test]
    fn preserves_association_send_state_checks() {
        let cache = RemoteAssociationCache::new();
        let outbound = Arc::new(CollectingOutbound::default());
        let target = envelope("kairo://closed@127.0.0.1:25522/user/target", 5);
        let mut association = RemoteAssociation::new("kairo://closed@127.0.0.1:25522");
        association.close("transport stopped");
        let guarded = AssociationRemoteOutbound::new(
            association,
            outbound.clone() as Arc<dyn RemoteOutbound>,
        );

        cache
            .insert_route_for_actor_ref(&target.recipient, Arc::new(guarded))
            .unwrap();
        let error = cache
            .send(target)
            .expect_err("closed association should reject sends through cache");

        assert!(matches!(error, RemoteError::AssociationClosed { .. }));
        assert!(outbound.sent().is_empty());
    }
}
