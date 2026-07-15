#![deny(missing_docs)]

use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
    str::FromStr,
    sync::{Arc, RwLock, Weak},
};

use kairo_serialization::{ActorRefWireData, RemoteEnvelope};

use crate::{RemoteError, RemoteOutbound, Result};

/// Canonical transport address shared by every actor reference on one remote
/// actor system.
///
/// Actor paths and actor incarnations are deliberately excluded from this
/// association cache key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RemoteAssociationAddress {
    protocol: String,
    system: String,
    host: String,
    port: Option<u16>,
}

impl RemoteAssociationAddress {
    /// Creates and validates a canonical remote association address.
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
        if !valid_address_part(&address.protocol, &[':', '/', '@', '#'])
            || !valid_address_part(&address.system, &[':', '/', '@', '#'])
            || !valid_address_part(&address.host, &['/', '@', '#'])
        {
            return Err(RemoteError::InvalidRemoteRef(
                address.to_string(),
                "remote association address requires valid protocol, system, and host".to_string(),
            ));
        }
        Ok(address)
    }

    /// Extracts the remote actor-system address from actor-reference wire data.
    ///
    /// Local-only references are rejected because they have no transport
    /// association address.
    pub fn from_actor_ref(wire: &ActorRefWireData) -> Result<Self> {
        let Some(host) = wire.host() else {
            return Err(RemoteError::LocalAddress(wire.path().to_string()));
        };
        Self::new(wire.protocol(), wire.system(), host, wire.port())
    }

    /// Returns the actor protocol, such as `kairo`.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// Returns the remote actor-system name.
    pub fn system(&self) -> &str {
        &self.system
    }

    /// Returns the remote transport host.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the remote transport port, if present.
    pub fn port(&self) -> Option<u16> {
        self.port
    }
}

fn valid_address_part(value: &str, separators: &[char]) -> bool {
    !value.is_empty()
        && value.trim() == value
        && !value.chars().any(char::is_whitespace)
        && !value.chars().any(|ch| separators.contains(&ch))
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

impl FromStr for RemoteAssociationAddress {
    type Err = RemoteError;

    fn from_str(value: &str) -> Result<Self> {
        let (protocol, rest) = value.split_once("://").ok_or_else(|| {
            RemoteError::InvalidRemoteRef(
                value.to_string(),
                "remote association address must start with protocol://".to_string(),
            )
        })?;
        if rest.contains('/') {
            return Err(RemoteError::InvalidRemoteRef(
                value.to_string(),
                "remote association address must not include an actor path".to_string(),
            ));
        }
        let authority = rest;
        let (system, host_port) = authority.split_once('@').ok_or_else(|| {
            RemoteError::InvalidRemoteRef(
                value.to_string(),
                "remote association address must include system@host".to_string(),
            )
        })?;
        let (host, port) = if let Some((host, port)) = host_port.rsplit_once(':') {
            let port = port.parse::<u16>().map_err(|_| {
                RemoteError::InvalidRemoteRef(
                    value.to_string(),
                    "remote association port must fit in u16".to_string(),
                )
            })?;
            (host, Some(port))
        } else {
            (host_port, None)
        };
        Self::new(protocol, system, host, port)
    }
}

/// Shared address-to-outbound routing table for remote associations.
///
/// Clones share the same table. Replacing or removing a route does not close it
/// unless the method name explicitly includes `close`.
#[derive(Clone, Default)]
pub struct RemoteAssociationCache {
    routes: Arc<RemoteAssociationRoutes>,
}

type RemoteAssociationRoutes = RwLock<BTreeMap<RemoteAssociationAddress, Arc<dyn RemoteOutbound>>>;

#[derive(Clone)]
pub(crate) struct RemoteAssociationCacheWeak {
    routes: Weak<RemoteAssociationRoutes>,
}

impl RemoteAssociationCache {
    /// Creates an empty association route cache.
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn downgrade(&self) -> RemoteAssociationCacheWeak {
        RemoteAssociationCacheWeak {
            routes: Arc::downgrade(&self.routes),
        }
    }

    /// Inserts or replaces the outbound route for `address`.
    ///
    /// Returns the previous route without closing it.
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

    /// Extracts an association address from `wire` and inserts its route.
    ///
    /// Returns the previous route without closing it.
    pub fn insert_route_for_actor_ref(
        &self,
        wire: &ActorRefWireData,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Result<Option<Arc<dyn RemoteOutbound>>> {
        let address = RemoteAssociationAddress::from_actor_ref(wire)?;
        Ok(self.insert_route(address, outbound))
    }

    /// Removes and returns the route for `address` without closing it.
    pub fn remove_route(
        &self,
        address: &RemoteAssociationAddress,
    ) -> Option<Arc<dyn RemoteOutbound>> {
        self.routes
            .write()
            .expect("remote association cache lock poisoned")
            .remove(address)
    }

    pub(crate) fn remove_route_if_same(
        &self,
        address: &RemoteAssociationAddress,
        expected: &Arc<dyn RemoteOutbound>,
    ) -> Option<Arc<dyn RemoteOutbound>> {
        let mut routes = self
            .routes
            .write()
            .expect("remote association cache lock poisoned");
        let route = routes.get(address)?;
        if !Arc::ptr_eq(route, expected) {
            return None;
        }
        routes.remove(address)
    }

    /// Removes and closes the route for `address`.
    ///
    /// The outer `Option` indicates whether a route existed; the inner result
    /// is the route's close result.
    pub fn remove_route_and_close(
        &self,
        address: &RemoteAssociationAddress,
        reason: &str,
    ) -> Option<Result<()>> {
        self.remove_route(address).map(|route| route.close(reason))
    }

    /// Removes every route without closing them and returns the removed count.
    pub fn clear_routes(&self) -> usize {
        let mut routes = self
            .routes
            .write()
            .expect("remote association cache lock poisoned");
        let len = routes.len();
        routes.clear();
        len
    }

    /// Atomically removes every route, closes each one, and returns every close
    /// result.
    pub fn clear_routes_and_close(&self, reason: &str) -> Vec<Result<()>> {
        let routes = std::mem::take(
            &mut *self
                .routes
                .write()
                .expect("remote association cache lock poisoned"),
        );
        routes
            .into_values()
            .map(|route| route.close(reason))
            .collect()
    }

    /// Returns the number of installed association routes.
    pub fn route_count(&self) -> usize {
        self.routes
            .read()
            .expect("remote association cache lock poisoned")
            .len()
    }

    /// Returns a sorted snapshot of installed association addresses.
    pub fn route_addresses(&self) -> Vec<RemoteAssociationAddress> {
        self.routes
            .read()
            .expect("remote association cache lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Returns whether a route exists for `address`.
    pub fn contains_route(&self, address: &RemoteAssociationAddress) -> bool {
        self.routes
            .read()
            .expect("remote association cache lock poisoned")
            .contains_key(address)
    }

    /// Extracts the association cache key for a recipient.
    pub fn address_for_recipient(
        &self,
        recipient: &ActorRefWireData,
    ) -> Result<RemoteAssociationAddress> {
        RemoteAssociationAddress::from_actor_ref(recipient)
    }

    /// Resolves the outbound route for a recipient.
    ///
    /// Returns [`RemoteError::AssociationUnavailable`] when no route is
    /// installed for the recipient's remote actor system.
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

    /// Resolves an envelope's recipient association and sends through its
    /// current route.
    pub fn send_to_recipient(&self, envelope: RemoteEnvelope) -> Result<()> {
        let route = self.route_for_recipient(&envelope.recipient)?;
        route.send(envelope)
    }
}

impl RemoteAssociationCacheWeak {
    pub(crate) fn remove_route_if_same(
        &self,
        address: &RemoteAssociationAddress,
        expected: &Arc<dyn RemoteOutbound>,
    ) -> Option<Arc<dyn RemoteOutbound>> {
        let routes = self.routes.upgrade()?;
        let mut routes = routes
            .write()
            .expect("remote association cache lock poisoned");
        let route = routes.get(address)?;
        if !Arc::ptr_eq(route, expected) {
            return None;
        }
        routes.remove(address)
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

    #[derive(Default)]
    struct CloseTrackingOutbound {
        closed: Mutex<Vec<String>>,
    }

    impl CloseTrackingOutbound {
        fn closed(&self) -> Vec<String> {
            self.closed
                .lock()
                .expect("close tracking outbound mutex poisoned")
                .clone()
        }
    }

    impl RemoteOutbound for CloseTrackingOutbound {
        fn send(&self, _envelope: RemoteEnvelope) -> Result<()> {
            Ok(())
        }

        fn close(&self, reason: &str) -> Result<()> {
            self.closed
                .lock()
                .expect("close tracking outbound mutex poisoned")
                .push(reason.to_string());
            Ok(())
        }
    }

    #[test]
    fn association_address_parses_contact_string() {
        let address: RemoteAssociationAddress =
            "kairo://cluster@seed.example.test:25520".parse().unwrap();

        assert_eq!(address.protocol(), "kairo");
        assert_eq!(address.system(), "cluster");
        assert_eq!(address.host(), "seed.example.test");
        assert_eq!(address.port(), Some(25520));
        assert_eq!(
            address.to_string(),
            "kairo://cluster@seed.example.test:25520"
        );

        let without_port: RemoteAssociationAddress =
            "kairo://cluster@seed.example.test".parse().unwrap();
        assert_eq!(without_port.port(), None);
    }

    #[test]
    fn association_address_rejects_actor_paths_and_invalid_ports() {
        assert!(matches!(
            "kairo://cluster@seed.example.test:25520/system/cluster"
                .parse::<RemoteAssociationAddress>(),
            Err(RemoteError::InvalidRemoteRef(_, _))
        ));
        assert!(matches!(
            "kairo://cluster@seed.example.test:25520/".parse::<RemoteAssociationAddress>(),
            Err(RemoteError::InvalidRemoteRef(_, _))
        ));
        assert!(matches!(
            "kairo://cluster@seed.example.test:not-a-port".parse::<RemoteAssociationAddress>(),
            Err(RemoteError::InvalidRemoteRef(_, _))
        ));
    }

    #[test]
    fn association_address_rejects_malformed_authority_parts() {
        for result in [
            RemoteAssociationAddress::new("kai ro", "cluster", "seed.example.test", Some(25520)),
            RemoteAssociationAddress::new("kairo", "clu/ster", "seed.example.test", Some(25520)),
            RemoteAssociationAddress::new("kairo", "cluster", " seed.example.test", Some(25520)),
            RemoteAssociationAddress::new("kairo", "cluster", "seed.example.test\t", Some(25520)),
            "kai ro://cluster@seed.example.test:25520".parse::<RemoteAssociationAddress>(),
            "kairo://clu/ster@seed.example.test:25520".parse::<RemoteAssociationAddress>(),
            "kairo://cluster@ seed.example.test:25520".parse::<RemoteAssociationAddress>(),
        ] {
            assert!(matches!(result, Err(RemoteError::InvalidRemoteRef(_, _))));
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
        assert!(cache.contains_route(
            &RemoteAssociationAddress::new("kairo", "remote", "127.0.0.1", Some(25520)).unwrap()
        ));
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
    fn clear_routes_removes_all_cached_associations() {
        let cache = RemoteAssociationCache::new();
        cache.insert_route(
            RemoteAssociationAddress::new("kairo", "one", "127.0.0.1", Some(25521)).unwrap(),
            Arc::new(CollectingOutbound::default()),
        );
        cache.insert_route(
            RemoteAssociationAddress::new("kairo", "two", "127.0.0.1", Some(25522)).unwrap(),
            Arc::new(CollectingOutbound::default()),
        );

        assert_eq!(cache.clear_routes(), 2);
        assert_eq!(cache.route_count(), 0);
    }

    #[test]
    fn clear_routes_and_close_closes_all_cached_associations() {
        let cache = RemoteAssociationCache::new();
        let first = Arc::new(CloseTrackingOutbound::default());
        let second = Arc::new(CloseTrackingOutbound::default());
        cache.insert_route(
            RemoteAssociationAddress::new("kairo", "one", "127.0.0.1", Some(25521)).unwrap(),
            first.clone() as Arc<dyn RemoteOutbound>,
        );
        cache.insert_route(
            RemoteAssociationAddress::new("kairo", "two", "127.0.0.1", Some(25522)).unwrap(),
            second.clone() as Arc<dyn RemoteOutbound>,
        );

        let results = cache.clear_routes_and_close("tcp remote actor system shutdown");

        assert_eq!(results.len(), 2);
        assert!(results.into_iter().all(|result| result.is_ok()));
        assert_eq!(
            first.closed(),
            vec!["tcp remote actor system shutdown".to_string()]
        );
        assert_eq!(
            second.closed(),
            vec!["tcp remote actor system shutdown".to_string()]
        );
        assert_eq!(cache.route_count(), 0);
    }

    #[test]
    fn remove_route_and_close_closes_removed_route() {
        let cache = RemoteAssociationCache::new();
        let outbound = Arc::new(CloseTrackingOutbound::default());
        let address =
            RemoteAssociationAddress::new("kairo", "close", "127.0.0.1", Some(25520)).unwrap();
        cache.insert_route(address.clone(), outbound.clone() as Arc<dyn RemoteOutbound>);

        cache
            .remove_route_and_close(&address, "peer route removed")
            .expect("route should be removed")
            .unwrap();

        assert_eq!(outbound.closed(), vec!["peer route removed".to_string()]);
        assert_eq!(cache.route_count(), 0);
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
