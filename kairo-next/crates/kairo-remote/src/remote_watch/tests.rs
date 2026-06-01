use kairo_serialization::ActorRefWireData;

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
fn inbound_watch_records_remote_watcher_without_outbound_effects() {
    let mut state = RemoteDeathWatchState::new();
    let effects = state.inbound_watch(watchee("target"), watcher("observer"));

    assert!(effects.is_empty());
    assert_eq!(state.inbound_watching_count(), 1);
    assert_eq!(state.watching_count(), 0);
    assert_eq!(state.watched_address_count(), 0);
    assert!(state.heartbeat_due(42).is_empty());
}

#[test]
fn inbound_unwatch_removes_remote_watcher_without_outbound_effects() {
    let mut state = RemoteDeathWatchState::new();
    let watchee = watchee("target");
    let watcher = watcher("observer");
    state.inbound_watch(watchee.clone(), watcher.clone());

    let effects = state.inbound_unwatch(&watchee, &watcher);

    assert!(effects.is_empty());
    assert_eq!(state.inbound_watching_count(), 0);
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
