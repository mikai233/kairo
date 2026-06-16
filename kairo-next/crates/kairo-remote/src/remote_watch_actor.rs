use std::sync::Arc;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};
use kairo_serialization::ActorRefWireData;

use crate::{
    RemoteDeathWatchEffect, RemoteDeathWatchState, RemoteError, RemoteHeartbeat,
    RemoteHeartbeatAck, RemoteTerminated, UnwatchRemote, WatchRemote,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDeathWatchStats {
    pub watching: usize,
    pub watched_addresses: usize,
    pub inbound_watching: usize,
    pub unreachable_addresses: usize,
    pub watching_refs: Vec<WatchRemote>,
    pub watching_addresses: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum RemoteDeathWatchCommand {
    Watch(WatchRemote),
    Unwatch(UnwatchRemote),
    InboundWatch(WatchRemote),
    InboundUnwatch(UnwatchRemote),
    LocalWatcheeTerminated {
        watchee: ActorRefWireData,
        existence_confirmed: bool,
    },
    RemoteTerminated(RemoteTerminated),
    HeartbeatTick {
        local_uid: u64,
    },
    Heartbeat {
        address: String,
        heartbeat: RemoteHeartbeat,
        local_uid: u64,
    },
    HeartbeatAck {
        address: String,
        ack: RemoteHeartbeatAck,
    },
    AddressUnreachable {
        address: String,
        uid: Option<u64>,
    },
    GetStats {
        reply_to: ActorRef<RemoteDeathWatchStats>,
    },
}

pub trait RemoteDeathWatchEffectSink: Send + Sync + 'static {
    fn apply(&self, effects: Vec<RemoteDeathWatchEffect>) -> crate::Result<()>;
}

pub struct RemoteDeathWatchActor {
    state: RemoteDeathWatchState,
    effect_sink: Arc<dyn RemoteDeathWatchEffectSink>,
}

impl RemoteDeathWatchActor {
    pub fn new(effect_sink: Arc<dyn RemoteDeathWatchEffectSink>) -> Self {
        Self {
            state: RemoteDeathWatchState::new(),
            effect_sink,
        }
    }

    pub fn with_state(
        state: RemoteDeathWatchState,
        effect_sink: Arc<dyn RemoteDeathWatchEffectSink>,
    ) -> Self {
        Self { state, effect_sink }
    }

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
