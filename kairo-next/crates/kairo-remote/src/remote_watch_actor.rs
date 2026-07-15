#![deny(missing_docs)]

use std::sync::Arc;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};
use kairo_serialization::ActorRefWireData;

use crate::{
    RemoteDeathWatchEffect, RemoteDeathWatchState, RemoteError, RemoteHeartbeat,
    RemoteHeartbeatAck, RemoteTerminated, UnwatchRemote, WatchRemote,
};

/// Observable snapshot of remote death-watch state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDeathWatchStats {
    /// Number of outbound watchee/watcher pairs.
    pub watching: usize,
    /// Number of remote addresses with outbound watches.
    pub watched_addresses: usize,
    /// Number of inbound remote-watcher/local-watchee pairs.
    pub inbound_watching: usize,
    /// Number of addresses marked unreachable.
    pub unreachable_addresses: usize,
    /// Deterministic snapshot of outbound watch pairs.
    pub watching_refs: Vec<WatchRemote>,
    /// Sorted snapshot of watched remote addresses.
    pub watching_addresses: Vec<String>,
}

/// Commands processed serially by [`RemoteDeathWatchActor`].
#[derive(Debug, Clone)]
pub enum RemoteDeathWatchCommand {
    /// Add an outbound remote watch.
    Watch(WatchRemote),
    /// Remove an outbound remote watch.
    Unwatch(UnwatchRemote),
    /// Record a remote watcher of a locally hosted actor.
    InboundWatch(WatchRemote),
    /// Remove a remote watcher of a locally hosted actor.
    InboundUnwatch(UnwatchRemote),
    /// Report termination of a locally hosted actor with inbound remote
    /// watchers.
    LocalWatcheeTerminated {
        /// Locally hosted actor that terminated.
        watchee: ActorRefWireData,
        /// Whether local death watch confirmed that the actor existed.
        existence_confirmed: bool,
    },
    /// Report termination of an actor watched on a remote system.
    RemoteTerminated(RemoteTerminated),
    /// Request heartbeats for every currently watched reachable address.
    HeartbeatTick {
        /// Local actor-system incarnation placed in heartbeat messages.
        local_uid: u64,
    },
    /// Deliver a heartbeat received from a remote watcher.
    Heartbeat {
        /// Canonical address of the sending actor system.
        address: String,
        /// Received heartbeat protocol message.
        heartbeat: RemoteHeartbeat,
        /// Local actor-system incarnation returned in the acknowledgement.
        local_uid: u64,
    },
    /// Deliver a remote heartbeat acknowledgement.
    HeartbeatAck {
        /// Canonical address of the acknowledging actor system.
        address: String,
        /// Acknowledgement carrying the observed remote UID.
        ack: RemoteHeartbeatAck,
    },
    /// Report an address as unreachable.
    AddressUnreachable {
        /// Canonical remote actor-system address.
        address: String,
        /// Explicit unreachable incarnation, if known.
        uid: Option<u64>,
    },
    /// Request an observable state snapshot.
    GetStats {
        /// Local actor reference that receives the snapshot.
        reply_to: ActorRef<RemoteDeathWatchStats>,
    },
}

/// Applies side effects produced by the remote death-watch state machine.
pub trait RemoteDeathWatchEffectSink: Send + Sync + 'static {
    /// Applies an ordered batch of effects from one actor turn.
    fn apply(&self, effects: Vec<RemoteDeathWatchEffect>) -> crate::Result<()>;
}

/// Synchronous actor wrapper around [`RemoteDeathWatchState`].
///
/// Each command advances the pure state machine in one actor turn and then
/// applies its ordered effects through a [`RemoteDeathWatchEffectSink`].
pub struct RemoteDeathWatchActor {
    state: RemoteDeathWatchState,
    effect_sink: Arc<dyn RemoteDeathWatchEffectSink>,
}

impl RemoteDeathWatchActor {
    /// Creates a remote-watcher actor with empty state.
    pub fn new(effect_sink: Arc<dyn RemoteDeathWatchEffectSink>) -> Self {
        Self {
            state: RemoteDeathWatchState::new(),
            effect_sink,
        }
    }

    /// Creates a remote-watcher actor with preloaded state.
    pub fn with_state(
        state: RemoteDeathWatchState,
        effect_sink: Arc<dyn RemoteDeathWatchEffectSink>,
    ) -> Self {
        Self { state, effect_sink }
    }

    /// Creates actor properties that build an empty remote watcher sharing
    /// `effect_sink`.
    pub fn props(effect_sink: Arc<dyn RemoteDeathWatchEffectSink>) -> Props<Self> {
        Props::new(move || Self::new(effect_sink))
    }

    fn stats(&self) -> RemoteDeathWatchStats {
        RemoteDeathWatchStats {
            watching: self.state.watching_count(),
            watched_addresses: self.state.watched_address_count(),
            inbound_watching: self.state.inbound_watching_count(),
            unreachable_addresses: self.state.unreachable_address_count(),
            watching_refs: self.state.watching_refs(),
            watching_addresses: self.state.watching_addresses(),
        }
    }

    fn apply_effects(&self, effects: Vec<RemoteDeathWatchEffect>) -> ActorResult {
        if effects.is_empty() {
            return Ok(());
        }

        self.effect_sink
            .apply(effects)
            .map_err(remote_error_to_actor_error)
    }
}

impl Actor for RemoteDeathWatchActor {
    type Msg = RemoteDeathWatchCommand;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RemoteDeathWatchCommand::Watch(message) => {
                let effects = self.state.watch(message.watchee, message.watcher);
                self.apply_effects(effects)
            }
            RemoteDeathWatchCommand::Unwatch(message) => {
                let effects = self.state.unwatch(&message.watchee, &message.watcher);
                self.apply_effects(effects)
            }
            RemoteDeathWatchCommand::InboundWatch(message) => {
                let effects = self.state.inbound_watch(message.watchee, message.watcher);
                self.apply_effects(effects)
            }
            RemoteDeathWatchCommand::InboundUnwatch(message) => {
                let effects = self
                    .state
                    .inbound_unwatch(&message.watchee, &message.watcher);
                self.apply_effects(effects)
            }
            RemoteDeathWatchCommand::LocalWatcheeTerminated {
                watchee,
                existence_confirmed,
            } => {
                let effects = self
                    .state
                    .local_watchee_terminated(&watchee, existence_confirmed);
                self.apply_effects(effects)
            }
            RemoteDeathWatchCommand::RemoteTerminated(message) => {
                let effects = self.state.remote_watchee_terminated(message);
                self.apply_effects(effects)
            }
            RemoteDeathWatchCommand::HeartbeatTick { local_uid } => {
                self.apply_effects(self.state.heartbeat_due(local_uid))
            }
            RemoteDeathWatchCommand::Heartbeat {
                address,
                heartbeat: _,
                local_uid,
            } => self.apply_effects(vec![RemoteDeathWatchEffect::SendHeartbeatAck {
                address,
                message: RemoteHeartbeatAck { uid: local_uid },
            }]),
            RemoteDeathWatchCommand::HeartbeatAck { address, ack } => {
                let effects = self.state.heartbeat_ack(address, ack.uid);
                self.apply_effects(effects)
            }
            RemoteDeathWatchCommand::AddressUnreachable { address, uid } => {
                let effects = self.state.mark_unreachable_with_uid(address, uid);
                self.apply_effects(effects)
            }
            RemoteDeathWatchCommand::GetStats { reply_to } => reply_to
                .tell(self.stats())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }
}

fn remote_error_to_actor_error(error: RemoteError) -> ActorError {
    ActorError::Message(error.to_string())
}

#[cfg(test)]
mod tests;
