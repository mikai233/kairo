#![deny(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use kairo_serialization::ActorRefWireData;

use crate::{
    AddressTerminated, RemoteHeartbeat, RemoteHeartbeatAck, RemoteTerminated, UnwatchRemote,
    WatchRemote,
};

/// Side effect requested by the pure remote death-watch state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteDeathWatchEffect {
    /// Send a new watch request to the remote watcher.
    SendWatchRemote(WatchRemote),
    /// Send an unwatch request to the remote watcher.
    SendUnwatchRemote(UnwatchRemote),
    /// Start periodic heartbeat scheduling for an address.
    StartHeartbeat {
        /// Canonical remote actor-system address.
        address: String,
    },
    /// Stop heartbeat scheduling for an address.
    StopHeartbeat {
        /// Canonical remote actor-system address.
        address: String,
    },
    /// Clear failure-detector history before watching an address again.
    ResetFailureDetector {
        /// Canonical remote actor-system address.
        address: String,
    },
    /// Send a heartbeat to a remote watcher.
    SendHeartbeat {
        /// Canonical remote actor-system address.
        address: String,
        /// Heartbeat protocol message.
        message: RemoteHeartbeat,
    },
    /// Reply to a remote heartbeat.
    SendHeartbeatAck {
        /// Canonical remote actor-system address.
        address: String,
        /// Heartbeat acknowledgement carrying the local incarnation.
        message: RemoteHeartbeatAck,
    },
    /// Re-send a watch after first learning or changing the remote UID.
    RewatchRemote(WatchRemote),
    /// Notify a remote watcher that its locally hosted watchee terminated.
    SendRemoteTerminated {
        /// Remote actor that requested the watch.
        watcher: ActorRefWireData,
        /// Termination protocol message.
        message: RemoteTerminated,
    },
    /// Notify local death watch that one remote watchee terminated.
    RemoteTerminated(RemoteTerminated),
    /// Publish and quarantine an unreachable remote actor-system incarnation.
    AddressTerminated(AddressTerminated),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WatcheeEntry {
    watchee: ActorRefWireData,
    address: String,
    watchers: BTreeMap<String, ActorRefWireData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InboundWatchEntry {
    watchers: BTreeMap<String, ActorRefWireData>,
}

/// Pure remote death-watch bookkeeping for outbound watches, inbound watches,
/// heartbeat ownership, observed incarnations, and unreachable addresses.
///
/// State transitions return explicit [`RemoteDeathWatchEffect`] values; they do
/// not perform transport, actor, scheduler, or failure-detector work directly.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteDeathWatchState {
    watchees: BTreeMap<String, WatcheeEntry>,
    watchees_by_address: BTreeMap<String, BTreeSet<String>>,
    inbound_watches: BTreeMap<String, InboundWatchEntry>,
    address_uids: BTreeMap<String, u64>,
    unreachable: BTreeSet<String>,
}

impl RemoteDeathWatchState {
    /// Creates empty remote death-watch state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of outbound watchee/watcher pairs.
    pub fn watching_count(&self) -> usize {
        self.watchees
            .values()
            .map(|entry| entry.watchers.len())
            .sum()
    }

    /// Returns the number of remote addresses with outbound watches.
    pub fn watched_address_count(&self) -> usize {
        self.watchees_by_address.len()
    }

    /// Returns a deterministic snapshot of all outbound watch pairs.
    pub fn watching_refs(&self) -> Vec<WatchRemote> {
        self.watchees
            .values()
            .flat_map(|entry| {
                entry.watchers.values().map(|watcher| WatchRemote {
                    watchee: entry.watchee.clone(),
                    watcher: watcher.clone(),
                })
            })
            .collect()
    }

    /// Returns a sorted snapshot of addresses currently heartbeat-monitored.
    pub fn watching_addresses(&self) -> Vec<String> {
        self.watchees_by_address.keys().cloned().collect()
    }

    /// Returns the number of inbound remote-watcher/local-watchee pairs.
    pub fn inbound_watching_count(&self) -> usize {
        self.inbound_watches
            .values()
            .map(|entry| entry.watchers.len())
            .sum()
    }

    /// Returns the number of addresses marked unreachable.
    pub fn unreachable_address_count(&self) -> usize {
        self.unreachable.len()
    }

    /// Returns whether at least one outbound watchee exists at `address`.
    pub fn is_watching_address(&self, address: &str) -> bool {
        self.watchees_by_address.contains_key(address)
    }

    /// Returns whether `address` has been marked unreachable.
    pub fn is_unreachable(&self, address: &str) -> bool {
        self.unreachable.contains(address)
    }

    /// Returns the most recently acknowledged actor-system UID for `address`.
    pub fn address_uid(&self, address: &str) -> Option<u64> {
        self.address_uids.get(address).copied()
    }

    /// Adds an outbound watch pair and returns the required protocol and
    /// heartbeat effects.
    ///
    /// Duplicate pairs are idempotent. The first watch at an address starts
    /// heartbeats; a new watch after unreachable detection resets failure
    /// detector history before monitoring resumes.
    pub fn watch(
        &mut self,
        watchee: ActorRefWireData,
        watcher: ActorRefWireData,
    ) -> Vec<RemoteDeathWatchEffect> {
        let address = remote_address(&watchee);
        let watchee_path = watchee.path().to_string();
        let watcher_path = watcher.path().to_string();
        let was_watching_address = self.watchees_by_address.contains_key(&address);
        let was_unreachable = self.unreachable.contains(&address);

        let entry = self
            .watchees
            .entry(watchee_path.clone())
            .or_insert_with(|| WatcheeEntry {
                watchee: watchee.clone(),
                address: address.clone(),
                watchers: BTreeMap::new(),
            });

        if entry.watchers.contains_key(&watcher_path) {
            if was_unreachable {
                self.unreachable.remove(&address);
                self.address_uids.remove(&address);
                return vec![RemoteDeathWatchEffect::ResetFailureDetector { address }];
            }
            return Vec::new();
        }

        entry.watchers.insert(watcher_path, watcher.clone());
        self.watchees_by_address
            .entry(address.clone())
            .or_default()
            .insert(watchee_path);

        let mut effects = Vec::new();
        if was_unreachable {
            self.unreachable.remove(&address);
            self.address_uids.remove(&address);
            effects.push(RemoteDeathWatchEffect::ResetFailureDetector {
                address: address.clone(),
            });
        }
        if !was_watching_address {
            effects.push(RemoteDeathWatchEffect::StartHeartbeat {
                address: address.clone(),
            });
        }
        effects.push(RemoteDeathWatchEffect::SendWatchRemote(WatchRemote {
            watchee,
            watcher,
        }));
        effects
    }

    /// Removes one outbound watch pair.
    ///
    /// Removing the final watchee at an address also stops heartbeats and clears
    /// the observed UID and unreachable marker.
    pub fn unwatch(
        &mut self,
        watchee: &ActorRefWireData,
        watcher: &ActorRefWireData,
    ) -> Vec<RemoteDeathWatchEffect> {
        let watchee_path = watchee.path().to_string();
        let watcher_path = watcher.path().to_string();
        let Some(entry) = self.watchees.get_mut(&watchee_path) else {
            return Vec::new();
        };
        if entry.watchers.remove(&watcher_path).is_none() {
            return Vec::new();
        }

        let mut effects = vec![RemoteDeathWatchEffect::SendUnwatchRemote(UnwatchRemote {
            watchee: watchee.clone(),
            watcher: watcher.clone(),
        })];

        if entry.watchers.is_empty() {
            let address = entry.address.clone();
            self.watchees.remove(&watchee_path);
            let mut remove_address = false;
            if let Some(watchees) = self.watchees_by_address.get_mut(&address) {
                watchees.remove(&watchee_path);
                remove_address = watchees.is_empty();
            }
            if remove_address {
                self.watchees_by_address.remove(&address);
                self.address_uids.remove(&address);
                self.unreachable.remove(&address);
                effects.push(RemoteDeathWatchEffect::StopHeartbeat { address });
            }
        }

        effects
    }

    /// Records a remote watcher observing a locally hosted watchee.
    ///
    /// This bookkeeping transition has no outbound effect.
    pub fn inbound_watch(
        &mut self,
        watchee: ActorRefWireData,
        watcher: ActorRefWireData,
    ) -> Vec<RemoteDeathWatchEffect> {
        let watchee_path = watchee.path().to_string();
        let watcher_path = watcher.path().to_string();
        let entry = self
            .inbound_watches
            .entry(watchee_path)
            .or_insert_with(|| InboundWatchEntry {
                watchers: BTreeMap::new(),
            });
        entry.watchers.insert(watcher_path, watcher);
        Vec::new()
    }

    /// Removes a remote watcher from a locally hosted watchee.
    pub fn inbound_unwatch(
        &mut self,
        watchee: &ActorRefWireData,
        watcher: &ActorRefWireData,
    ) -> Vec<RemoteDeathWatchEffect> {
        let watchee_path = watchee.path().to_string();
        let watcher_path = watcher.path().to_string();
        let Some(entry) = self.inbound_watches.get_mut(&watchee_path) else {
            return Vec::new();
        };
        entry.watchers.remove(&watcher_path);
        if entry.watchers.is_empty() {
            self.inbound_watches.remove(&watchee_path);
        }
        Vec::new()
    }

    /// Removes all inbound watches for a terminated local watchee and returns
    /// one remote termination notification per watcher.
    pub fn local_watchee_terminated(
        &mut self,
        watchee: &ActorRefWireData,
        existence_confirmed: bool,
    ) -> Vec<RemoteDeathWatchEffect> {
        let watchee_path = watchee.path().to_string();
        let Some(entry) = self.inbound_watches.remove(&watchee_path) else {
            return Vec::new();
        };

        entry
            .watchers
            .into_values()
            .map(|watcher| RemoteDeathWatchEffect::SendRemoteTerminated {
                watcher,
                message: RemoteTerminated {
                    watchee: watchee.clone(),
                    existence_confirmed,
                },
            })
            .collect()
    }

    /// Records termination of a remote watchee and requests local notification.
    ///
    /// Heartbeats stop only when this was the final watched actor at the remote
    /// address.
    pub fn remote_watchee_terminated(
        &mut self,
        message: RemoteTerminated,
    ) -> Vec<RemoteDeathWatchEffect> {
        let watchee_path = message.watchee.path().to_string();
        let Some(entry) = self.watchees.remove(&watchee_path) else {
            return Vec::new();
        };

        let mut effects = vec![RemoteDeathWatchEffect::RemoteTerminated(message)];
        let address = entry.address;
        let mut remove_address = false;
        if let Some(watchees) = self.watchees_by_address.get_mut(&address) {
            watchees.remove(&watchee_path);
            remove_address = watchees.is_empty();
        }
        if remove_address {
            self.watchees_by_address.remove(&address);
            self.address_uids.remove(&address);
            self.unreachable.remove(&address);
            effects.push(RemoteDeathWatchEffect::StopHeartbeat { address });
        }
        effects
    }

    /// Returns heartbeat sends due for every watched address that is not marked
    /// unreachable.
    pub fn heartbeat_due(&self, local_uid: u64) -> Vec<RemoteDeathWatchEffect> {
        self.watchees_by_address
            .keys()
            .filter(|address| !self.unreachable.contains(*address))
            .map(|address| RemoteDeathWatchEffect::SendHeartbeat {
                address: address.clone(),
                message: RemoteHeartbeat {
                    from_uid: local_uid,
                },
            })
            .collect()
    }

    /// Records a heartbeat acknowledgement from `address`.
    ///
    /// The first observed UID and every UID change re-emit all watch pairs for
    /// the address so actor incarnations are revalidated remotely.
    pub fn heartbeat_ack(
        &mut self,
        address: impl Into<String>,
        uid: u64,
    ) -> Vec<RemoteDeathWatchEffect> {
        let address = address.into();
        if !self.watchees_by_address.contains_key(&address) || self.unreachable.contains(&address) {
            return Vec::new();
        }

        let previous_uid = self.address_uids.insert(address.clone(), uid);
        if previous_uid == Some(uid) {
            return Vec::new();
        }

        self.watch_pairs_for_address(&address)
            .into_iter()
            .map(RemoteDeathWatchEffect::RewatchRemote)
            .collect()
    }

    /// Marks an address unreachable using its last observed UID, if any.
    pub fn mark_unreachable(&mut self, address: impl Into<String>) -> Vec<RemoteDeathWatchEffect> {
        self.mark_unreachable_with_uid(address, None)
    }

    /// Marks an address unreachable with an optional explicit remote UID.
    ///
    /// All outbound watchees at the address are removed, address termination is
    /// emitted once, and heartbeat ownership stops. Repeated calls are
    /// idempotent.
    pub fn mark_unreachable_with_uid(
        &mut self,
        address: impl Into<String>,
        uid: Option<u64>,
    ) -> Vec<RemoteDeathWatchEffect> {
        let address = address.into();
        let Some(watchee_paths) = self.watchees_by_address.remove(&address) else {
            return Vec::new();
        };
        if !self.unreachable.insert(address.clone()) {
            self.watchees_by_address.insert(address, watchee_paths);
            return Vec::new();
        }

        for watchee_path in watchee_paths {
            self.watchees.remove(&watchee_path);
        }
        let observed_uid = uid.or_else(|| self.address_uids.remove(&address));

        vec![
            RemoteDeathWatchEffect::AddressTerminated(AddressTerminated {
                address: address.clone(),
                uid: observed_uid,
            }),
            RemoteDeathWatchEffect::StopHeartbeat { address },
        ]
    }

    fn watch_pairs_for_address(&self, address: &str) -> Vec<WatchRemote> {
        let Some(watchee_paths) = self.watchees_by_address.get(address) else {
            return Vec::new();
        };

        let mut pairs = Vec::new();
        for watchee_path in watchee_paths {
            if let Some(entry) = self.watchees.get(watchee_path) {
                for watcher in entry.watchers.values() {
                    pairs.push(WatchRemote {
                        watchee: entry.watchee.clone(),
                        watcher: watcher.clone(),
                    });
                }
            }
        }
        pairs
    }
}

fn remote_address(wire: &ActorRefWireData) -> String {
    let mut address = format!("{}://{}", wire.protocol(), wire.system());
    if let Some(host) = wire.host() {
        address.push('@');
        address.push_str(host);
        if let Some(port) = wire.port() {
            address.push(':');
            address.push_str(&port.to_string());
        }
    }
    address
}

#[cfg(test)]
mod tests;
