use std::collections::{BTreeMap, BTreeSet};

use kairo_serialization::ActorRefWireData;

use crate::{AddressTerminated, RemoteHeartbeat, UnwatchRemote, WatchRemote};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteDeathWatchEffect {
    SendWatchRemote(WatchRemote),
    SendUnwatchRemote(UnwatchRemote),
    StartHeartbeat {
        address: String,
    },
    StopHeartbeat {
        address: String,
    },
    ResetFailureDetector {
        address: String,
    },
    SendHeartbeat {
        address: String,
        message: RemoteHeartbeat,
    },
    RewatchRemote(WatchRemote),
    AddressTerminated(AddressTerminated),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WatcheeEntry {
    watchee: ActorRefWireData,
    address: String,
    watchers: BTreeMap<String, ActorRefWireData>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteDeathWatchState {
    watchees: BTreeMap<String, WatcheeEntry>,
    watchees_by_address: BTreeMap<String, BTreeSet<String>>,
    address_uids: BTreeMap<String, u64>,
    unreachable: BTreeSet<String>,
}

impl RemoteDeathWatchState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn watching_count(&self) -> usize {
        self.watchees
            .values()
            .map(|entry| entry.watchers.len())
            .sum()
    }

    pub fn watched_address_count(&self) -> usize {
        self.watchees_by_address.len()
    }

    pub fn unreachable_address_count(&self) -> usize {
        self.unreachable.len()
    }

    pub fn is_watching_address(&self, address: &str) -> bool {
        self.watchees_by_address.contains_key(address)
    }

    pub fn is_unreachable(&self, address: &str) -> bool {
        self.unreachable.contains(address)
    }

    pub fn address_uid(&self, address: &str) -> Option<u64> {
        self.address_uids.get(address).copied()
    }

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

    pub fn mark_unreachable(&mut self, address: impl Into<String>) -> Vec<RemoteDeathWatchEffect> {
        let address = address.into();
        if !self.watchees_by_address.contains_key(&address)
            || !self.unreachable.insert(address.clone())
        {
            return Vec::new();
        }

        vec![RemoteDeathWatchEffect::AddressTerminated(
            AddressTerminated {
                address: address.clone(),
                uid: self.address_uids.get(&address).copied(),
            },
        )]
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
mod tests {
    use super::*;

    fn watchee(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://remote@127.0.0.1:25520/user/{name}")).unwrap()
    }

    fn watcher(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://local@127.0.0.1:25521/user/{name}")).unwrap()
    }

    #[test]
    fn watch_records_pair_and_starts_heartbeat_for_first_address() {
        let mut state = RemoteDeathWatchState::new();
        let watchee = watchee("target");
        let watcher = watcher("observer");

        let effects = state.watch(watchee.clone(), watcher.clone());

        assert_eq!(state.watching_count(), 1);
        assert_eq!(state.watched_address_count(), 1);
        assert_eq!(
            effects,
            vec![
                RemoteDeathWatchEffect::StartHeartbeat {
                    address: "kairo://remote@127.0.0.1:25520".to_string()
                },
                RemoteDeathWatchEffect::SendWatchRemote(WatchRemote { watchee, watcher }),
            ]
        );
    }

    #[test]
    fn duplicate_watch_is_idempotent() {
        let mut state = RemoteDeathWatchState::new();
        let watchee = watchee("target");
        let watcher = watcher("observer");

        assert!(!state.watch(watchee.clone(), watcher.clone()).is_empty());
        assert!(state.watch(watchee, watcher).is_empty());
        assert_eq!(state.watching_count(), 1);
        assert_eq!(state.watched_address_count(), 1);
    }

    #[test]
    fn unwatch_stops_heartbeat_after_last_watchee_on_address() {
        let mut state = RemoteDeathWatchState::new();
        let watchee = watchee("target");
        let watcher = watcher("observer");
        state.watch(watchee.clone(), watcher.clone());

        let effects = state.unwatch(&watchee, &watcher);

        assert_eq!(
            effects,
            vec![
                RemoteDeathWatchEffect::SendUnwatchRemote(UnwatchRemote { watchee, watcher }),
                RemoteDeathWatchEffect::StopHeartbeat {
                    address: "kairo://remote@127.0.0.1:25520".to_string()
                },
            ]
        );
        assert_eq!(state.watching_count(), 0);
        assert_eq!(state.watched_address_count(), 0);
    }

    #[test]
    fn unwatch_keeps_heartbeat_while_other_watches_remain_on_address() {
        let mut state = RemoteDeathWatchState::new();
        let first = watchee("first");
        let second = watchee("second");
        let watcher = watcher("observer");
        state.watch(first.clone(), watcher.clone());
        state.watch(second, watcher.clone());

        let effects = state.unwatch(&first, &watcher);

        assert!(matches!(
            effects.as_slice(),
            [RemoteDeathWatchEffect::SendUnwatchRemote(_)]
        ));
        assert_eq!(state.watching_count(), 1);
        assert_eq!(state.watched_address_count(), 1);
    }

    #[test]
    fn heartbeat_due_skips_unreachable_addresses() {
        let mut state = RemoteDeathWatchState::new();
        state.watch(watchee("target"), watcher("observer"));

        assert_eq!(
            state.heartbeat_due(42),
            vec![RemoteDeathWatchEffect::SendHeartbeat {
                address: "kairo://remote@127.0.0.1:25520".to_string(),
                message: RemoteHeartbeat { from_uid: 42 },
            }]
        );

        state.mark_unreachable("kairo://remote@127.0.0.1:25520");
        assert!(state.heartbeat_due(42).is_empty());
    }

    #[test]
    fn heartbeat_ack_tracks_uid_and_rewatches_on_new_incarnation() {
        let mut state = RemoteDeathWatchState::new();
        let watchee = watchee("target");
        let watcher = watcher("observer");
        state.watch(watchee.clone(), watcher.clone());

        let first = state.heartbeat_ack("kairo://remote@127.0.0.1:25520", 7);

        assert_eq!(state.address_uid("kairo://remote@127.0.0.1:25520"), Some(7));
        assert_eq!(
            first,
            vec![RemoteDeathWatchEffect::RewatchRemote(WatchRemote {
                watchee: watchee.clone(),
                watcher: watcher.clone()
            })]
        );
        assert!(
            state
                .heartbeat_ack("kairo://remote@127.0.0.1:25520", 7)
                .is_empty()
        );

        let changed = state.heartbeat_ack("kairo://remote@127.0.0.1:25520", 8);
        assert_eq!(
            changed,
            vec![RemoteDeathWatchEffect::RewatchRemote(WatchRemote {
                watchee,
                watcher
            })]
        );
    }

    #[test]
    fn unreachable_address_publishes_termination_once() {
        let mut state = RemoteDeathWatchState::new();
        state.watch(watchee("target"), watcher("observer"));
        state.heartbeat_ack("kairo://remote@127.0.0.1:25520", 7);

        let effects = state.mark_unreachable("kairo://remote@127.0.0.1:25520");

        assert_eq!(
            effects,
            vec![RemoteDeathWatchEffect::AddressTerminated(
                AddressTerminated {
                    address: "kairo://remote@127.0.0.1:25520".to_string(),
                    uid: Some(7),
                }
            )]
        );
        assert!(state.is_unreachable("kairo://remote@127.0.0.1:25520"));
        assert!(
            state
                .mark_unreachable("kairo://remote@127.0.0.1:25520")
                .is_empty()
        );
    }

    #[test]
    fn new_watch_after_unreachable_resets_failure_detector() {
        let mut state = RemoteDeathWatchState::new();
        state.watch(watchee("first"), watcher("observer"));
        state.mark_unreachable("kairo://remote@127.0.0.1:25520");

        let effects = state.watch(watchee("second"), watcher("observer"));

        assert!(
            effects.contains(&RemoteDeathWatchEffect::ResetFailureDetector {
                address: "kairo://remote@127.0.0.1:25520".to_string()
            })
        );
        assert!(!state.is_unreachable("kairo://remote@127.0.0.1:25520"));
    }

    #[test]
    fn duplicate_watch_after_unreachable_resets_failure_detector() {
        let mut state = RemoteDeathWatchState::new();
        let watchee = watchee("target");
        let watcher = watcher("observer");
        state.watch(watchee.clone(), watcher.clone());
        state.heartbeat_ack("kairo://remote@127.0.0.1:25520", 7);
        state.mark_unreachable("kairo://remote@127.0.0.1:25520");

        let effects = state.watch(watchee, watcher);

        assert_eq!(
            effects,
            vec![RemoteDeathWatchEffect::ResetFailureDetector {
                address: "kairo://remote@127.0.0.1:25520".to_string()
            }]
        );
        assert_eq!(state.address_uid("kairo://remote@127.0.0.1:25520"), None);
        assert!(!state.is_unreachable("kairo://remote@127.0.0.1:25520"));
    }
}
