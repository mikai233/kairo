#![deny(missing_docs)]

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{AssociationState, RemoteAssociation, RemoteAssociationAddress, RemoteError, Result};

/// Shared mutable handle to one remote association lifecycle.
pub type RemoteAssociationHandle = Arc<Mutex<RemoteAssociation>>;

/// Registry that indexes remote associations by canonical address and learned
/// actor-system incarnation.
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
    /// Creates an empty association registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the existing association for `address` or creates one in the
    /// handshaking state.
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

    /// Completes a handshake and indexes the association by remote `uid`.
    ///
    /// A UID already owned by another address is rejected. A new UID may
    /// replace a previously indexed closed or quarantined incarnation at the
    /// same address, while an unidentified closed association remains terminal.
    pub fn complete_handshake(
        &self,
        address: RemoteAssociationAddress,
        uid: u64,
    ) -> Result<RemoteAssociationHandle> {
        let mut state = self
            .state
            .write()
            .expect("remote association registry lock poisoned");
        if let Some(existing) = state.by_uid.get(&uid)
            && existing != &address
        {
            return Err(RemoteError::AssociationUidCollision {
                uid,
                existing: existing.to_string(),
                attempted: address.to_string(),
            });
        }

        let previous_uid = state
            .by_uid
            .iter()
            .find_map(|(indexed_uid, indexed_address)| {
                (indexed_address == &address).then_some(*indexed_uid)
            });
        let association = match state.by_address.get(&address).cloned() {
            Some(existing) => {
                let replace_terminal = {
                    let association = existing.lock().expect("remote association lock poisoned");
                    match association.state() {
                        AssociationState::Closed { .. } => previous_uid.is_some(),
                        AssociationState::Quarantined { .. } => {
                            previous_uid.is_some_and(|previous| previous != uid)
                        }
                        _ => false,
                    }
                };
                if replace_terminal {
                    let mut replacement = RemoteAssociation::new(address.to_string());
                    replacement.start_handshake();
                    let replacement = Arc::new(Mutex::new(replacement));
                    state
                        .by_address
                        .insert(address.clone(), replacement.clone());
                    replacement
                } else {
                    existing
                }
            }
            None => {
                let mut association = RemoteAssociation::new(address.to_string());
                association.start_handshake();
                let association = Arc::new(Mutex::new(association));
                state
                    .by_address
                    .insert(address.clone(), association.clone());
                association
            }
        };

        let mut association_guard = association
            .lock()
            .expect("remote association lock poisoned");
        association_guard.ensure_send_allowed()?;
        state.by_uid.retain(|indexed_uid, indexed_address| {
            *indexed_uid == uid || indexed_address != &address
        });
        state.by_uid.insert(uid, address);
        association_guard.activate(Some(uid));
        drop(association_guard);
        drop(state);
        Ok(association)
    }

    /// Returns the association indexed by remote actor-system UID.
    pub fn association_by_uid(&self, uid: u64) -> Option<RemoteAssociationHandle> {
        let state = self
            .state
            .read()
            .expect("remote association registry lock poisoned");
        let address = state.by_uid.get(&uid)?;
        state.by_address.get(address).cloned()
    }

    /// Returns the association stored for a canonical remote address.
    pub fn association_for_address(
        &self,
        address: &RemoteAssociationAddress,
    ) -> Option<RemoteAssociationHandle> {
        self.state
            .read()
            .expect("remote association registry lock poisoned")
            .by_address
            .get(address)
            .cloned()
    }

    /// Returns the active or quarantined remote UID stored for `address`.
    pub fn uid_for_address(&self, address: &RemoteAssociationAddress) -> Option<u64> {
        let association = self.association_for_address(address)?;
        let association = association
            .lock()
            .expect("remote association lock poisoned");
        association_uid(association.state())
    }

    /// Quarantines an association only when its current UID matches
    /// `expected_uid`.
    ///
    /// Returns whether the matching incarnation was found and quarantined.
    pub fn quarantine_if_uid(
        &self,
        address: &RemoteAssociationAddress,
        expected_uid: u64,
        reason: impl Into<String>,
    ) -> bool {
        let Some(association) = self.association_for_address(address) else {
            return false;
        };
        let mut association = association
            .lock()
            .expect("remote association lock poisoned");
        if association_uid(association.state()) != Some(expected_uid) {
            return false;
        }
        association.quarantine(Some(expected_uid), reason);
        true
    }

    /// Returns a snapshot of all association handles indexed by address.
    pub fn all_associations(&self) -> Vec<RemoteAssociationHandle> {
        self.state
            .read()
            .expect("remote association registry lock poisoned")
            .by_address
            .values()
            .cloned()
            .collect()
    }

    /// Returns the number of addresses with association handles.
    pub fn association_count(&self) -> usize {
        self.state
            .read()
            .expect("remote association registry lock poisoned")
            .by_address
            .len()
    }

    /// Returns the number of learned UID-to-address indexes.
    pub fn uid_count(&self) -> usize {
        self.state
            .read()
            .expect("remote association registry lock poisoned")
            .by_uid
            .len()
    }
}

fn association_uid(state: &AssociationState) -> Option<u64> {
    match state {
        AssociationState::Active { remote_uid }
        | AssociationState::Quarantined { remote_uid, .. } => *remote_uid,
        AssociationState::Idle
        | AssociationState::Handshaking
        | AssociationState::Closed { .. } => None,
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
    fn same_uid_handshake_replaces_closed_indexed_association() {
        let registry = RemoteAssociationRegistry::new();
        let remote = address("remote", 25520);
        let closed = registry.complete_handshake(remote.clone(), 42).unwrap();
        closed
            .lock()
            .expect("remote association lock poisoned")
            .close("transport failed");

        let replacement = registry.complete_handshake(remote, 42).unwrap();

        assert!(!Arc::ptr_eq(&closed, &replacement));
        assert!(Arc::ptr_eq(
            &replacement,
            &registry.association_by_uid(42).unwrap()
        ));
        assert_eq!(
            closed
                .lock()
                .expect("remote association lock poisoned")
                .state(),
            &AssociationState::Closed {
                reason: "transport failed".to_string()
            }
        );
        assert_eq!(
            replacement
                .lock()
                .expect("remote association lock poisoned")
                .state(),
            &AssociationState::Active {
                remote_uid: Some(42)
            }
        );
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
        assert_eq!(registry.association_count(), 1);
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
    fn new_uid_replaces_quarantined_incarnation_but_not_unidentified_closed_state() {
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

        let old = registry
            .complete_handshake(quarantined.clone(), 41)
            .unwrap();
        assert!(registry.quarantine_if_uid(&quarantined, 41, "uid mismatch"));
        assert!(
            registry
                .complete_handshake(quarantined.clone(), 41)
                .is_err()
        );
        let replacement = registry
            .complete_handshake(quarantined.clone(), 43)
            .unwrap();

        assert!(!Arc::ptr_eq(&old, &replacement));
        assert!(registry.association_by_uid(41).is_none());
        assert!(Arc::ptr_eq(
            &replacement,
            &registry.association_by_uid(43).unwrap()
        ));
        assert_eq!(registry.association_count(), 2);
        assert_eq!(registry.uid_count(), 1);
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
            replacement
                .lock()
                .expect("remote association lock poisoned")
                .state(),
            &AssociationState::Active {
                remote_uid: Some(43)
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
        assert!(registry.association_by_uid(41).is_none());
        assert!(Arc::ptr_eq(
            &first,
            &registry.association_by_uid(42).unwrap()
        ));
        assert_eq!(registry.uid_count(), 1);
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
