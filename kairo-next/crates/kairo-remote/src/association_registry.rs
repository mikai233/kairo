use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{RemoteAssociation, RemoteAssociationAddress, RemoteError, Result};

pub type RemoteAssociationHandle = Arc<Mutex<RemoteAssociation>>;

#[derive(Clone, Default)]
pub struct RemoteAssociationRegistry {
    state: Arc<RwLock<RemoteAssociationRegistryState>>,
}

#[derive(Default)]
struct RemoteAssociationRegistryState {
    by_address: BTreeMap<RemoteAssociationAddress, RemoteAssociationHandle>,
    by_uid: BTreeMap<u64, RemoteAssociationAddress>,
}

impl RemoteAssociationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn association(&self, address: RemoteAssociationAddress) -> RemoteAssociationHandle {
        let mut state = self
            .state
            .write()
            .expect("remote association registry lock poisoned");
        state
            .by_address
            .entry(address.clone())
            .or_insert_with(|| {
                let mut association = RemoteAssociation::new(address.to_string());
                association.start_handshake();
                Arc::new(Mutex::new(association))
            })
            .clone()
    }

    pub fn complete_handshake(
        &self,
        address: RemoteAssociationAddress,
        uid: u64,
    ) -> Result<RemoteAssociationHandle> {
        let association = self.association(address.clone());

        let mut state = self
            .state
            .write()
            .expect("remote association registry lock poisoned");
        let mut association_guard = association
            .lock()
            .expect("remote association lock poisoned");
        association_guard.ensure_send_allowed()?;
        match state.by_uid.get(&uid) {
            Some(existing) if existing == &address => {}
            Some(existing) => {
                return Err(RemoteError::AssociationUidCollision {
                    uid,
                    existing: existing.to_string(),
                    attempted: address.to_string(),
                });
            }
            None => {
                state.by_uid.insert(uid, address);
            }
        }
        association_guard.activate(Some(uid));
        drop(association_guard);
        drop(state);
        Ok(association)
    }

    pub fn association_by_uid(&self, uid: u64) -> Option<RemoteAssociationHandle> {
        let state = self
            .state
            .read()
            .expect("remote association registry lock poisoned");
        let address = state.by_uid.get(&uid)?;
        state.by_address.get(address).cloned()
    }

    pub fn all_associations(&self) -> Vec<RemoteAssociationHandle> {
        self.state
            .read()
            .expect("remote association registry lock poisoned")
            .by_address
            .values()
            .cloned()
            .collect()
    }

    pub fn association_count(&self) -> usize {
        self.state
            .read()
            .expect("remote association registry lock poisoned")
            .by_address
            .len()
    }

    pub fn uid_count(&self) -> usize {
        self.state
            .read()
            .expect("remote association registry lock poisoned")
            .by_uid
            .len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::AssociationState;

    use super::*;

    fn address(system: &str, port: u16) -> RemoteAssociationAddress {
        RemoteAssociationAddress::new("kairo", system, "127.0.0.1", Some(port)).unwrap()
    }

    #[test]
    fn association_reuses_existing_handle_by_address() {
        let registry = RemoteAssociationRegistry::new();
        let remote = address("remote", 25520);

        let first = registry.association(remote.clone());
        let second = registry.association(remote);

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(registry.association_count(), 1);
        assert_eq!(
            first
                .lock()
                .expect("remote association lock poisoned")
                .state(),
            &AssociationState::Handshaking
        );
    }

    #[test]
    fn complete_handshake_indexes_uid_and_activates_association() {
        let registry = RemoteAssociationRegistry::new();
        let remote = address("remote", 25520);

        let association = registry.complete_handshake(remote, 42).unwrap();
        let by_uid = registry.association_by_uid(42).unwrap();

        assert!(Arc::ptr_eq(&association, &by_uid));
        assert_eq!(registry.association_count(), 1);
        assert_eq!(registry.uid_count(), 1);
        assert_eq!(
            association
                .lock()
                .expect("remote association lock poisoned")
                .state(),
            &AssociationState::Active {
                remote_uid: Some(42)
            }
        );
    }

    #[test]
    fn complete_handshake_is_idempotent_for_same_uid_and_address() {
        let registry = RemoteAssociationRegistry::new();
        let remote = address("remote", 25520);

        let first = registry.complete_handshake(remote.clone(), 42).unwrap();
        let second = registry.complete_handshake(remote, 42).unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(registry.association_count(), 1);
        assert_eq!(registry.uid_count(), 1);
    }

    #[test]
    fn complete_handshake_rejects_uid_collision_across_addresses() {
        let registry = RemoteAssociationRegistry::new();
        let first = address("first", 25520);
        let second = address("second", 25521);

        registry.complete_handshake(first.clone(), 42).unwrap();
        let error = registry.complete_handshake(second, 42).unwrap_err();

        assert!(matches!(
            error,
            RemoteError::AssociationUidCollision { uid: 42, .. }
        ));
        assert_eq!(registry.association_count(), 2);
        assert_eq!(registry.uid_count(), 1);
        let by_uid = registry.association_by_uid(42).unwrap();
        assert_eq!(
            by_uid
                .lock()
                .expect("remote association lock poisoned")
                .remote_address(),
            first.to_string()
        );
    }

    #[test]
    fn complete_handshake_does_not_index_terminal_association() {
        let registry = RemoteAssociationRegistry::new();
        let closed = address("closed", 25520);
        let quarantined = address("quarantined", 25521);

        registry
            .association(closed.clone())
            .lock()
            .expect("remote association lock poisoned")
            .close("transport stopped");
        let closed_error = registry.complete_handshake(closed.clone(), 42).unwrap_err();

        assert!(matches!(
            closed_error,
            RemoteError::AssociationClosed { .. }
        ));
        assert!(registry.association_by_uid(42).is_none());

        registry
            .association(quarantined.clone())
            .lock()
            .expect("remote association lock poisoned")
            .quarantine(Some(41), "uid mismatch");
        let quarantined_error = registry
            .complete_handshake(quarantined.clone(), 43)
            .unwrap_err();

        assert!(matches!(
            quarantined_error,
            RemoteError::AssociationQuarantined { .. }
        ));
        assert!(registry.association_by_uid(43).is_none());
        assert_eq!(registry.association_count(), 2);
        assert_eq!(registry.uid_count(), 0);
        assert_eq!(
            registry
                .association(closed)
                .lock()
                .expect("remote association lock poisoned")
                .state(),
            &AssociationState::Closed {
                reason: "transport stopped".to_string()
            }
        );
        assert_eq!(
            registry
                .association(quarantined)
                .lock()
                .expect("remote association lock poisoned")
                .state(),
            &AssociationState::Quarantined {
                remote_uid: Some(41),
                reason: "uid mismatch".to_string()
            }
        );
    }

    #[test]
    fn same_address_can_record_new_uid_incarnation() {
        let registry = RemoteAssociationRegistry::new();
        let remote = address("remote", 25520);

        let first = registry.complete_handshake(remote.clone(), 41).unwrap();
        let second = registry.complete_handshake(remote, 42).unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(
            &first,
            &registry.association_by_uid(41).unwrap()
        ));
        assert!(Arc::ptr_eq(
            &first,
            &registry.association_by_uid(42).unwrap()
        ));
        assert_eq!(registry.uid_count(), 2);
        assert_eq!(
            first
                .lock()
                .expect("remote association lock poisoned")
                .state(),
            &AssociationState::Active {
                remote_uid: Some(42)
            }
        );
    }
}
