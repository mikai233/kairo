use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};
use kairo_cluster::UniqueAddress;

use crate::{
    SingletonManagerEffect, SingletonManagerRuntime, SingletonManagerSettings,
    SingletonManagerState, SingletonOldestChange, SingletonOldestObservation,
};

const HAND_OVER_RETRY_TIMER_KEY: &str = "singleton-manager-handover-retry";

pub struct SingletonManagerActor {
    runtime: SingletonManagerRuntime,
    settings: SingletonManagerSettings,
    effect_sink: Option<ActorRef<Vec<SingletonManagerEffect>>>,
}

impl SingletonManagerActor {
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            runtime: SingletonManagerRuntime::new(self_node),
            settings: SingletonManagerSettings::default(),
            effect_sink: None,
        }
    }

    pub fn props(self_node: UniqueAddress) -> Props<Self> {
        Props::new(move || Self::new(self_node))
    }

    pub fn with_settings(self_node: UniqueAddress, settings: SingletonManagerSettings) -> Self {
        Self {
            runtime: SingletonManagerRuntime::new(self_node),
            settings,
            effect_sink: None,
        }
    }

    pub fn props_with_settings(
        self_node: UniqueAddress,
        settings: SingletonManagerSettings,
    ) -> Props<Self> {
        Props::new(move || Self::with_settings(self_node, settings))
    }

    pub fn with_effect_sink(
        self_node: UniqueAddress,
        settings: SingletonManagerSettings,
        effect_sink: ActorRef<Vec<SingletonManagerEffect>>,
    ) -> Self {
        Self {
            runtime: SingletonManagerRuntime::new(self_node),
            settings,
            effect_sink: Some(effect_sink),
        }
    }

    pub fn props_with_effect_sink(
        self_node: UniqueAddress,
        settings: SingletonManagerSettings,
        effect_sink: ActorRef<Vec<SingletonManagerEffect>>,
    ) -> Props<Self> {
        Props::new(move || Self::with_effect_sink(self_node, settings, effect_sink.clone()))
    }

    pub fn runtime(&self) -> &SingletonManagerRuntime {
        &self.runtime
    }

    fn emit_effects(
        &self,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
        effects: Vec<SingletonManagerEffect>,
    ) {
        if !effects.is_empty()
            && let Some(effect_sink) = &self.effect_sink
        {
            let _ = effect_sink.tell(effects.clone());
        }
        reply_effects(reply_to, effects);
    }

    fn reconcile_hand_over_retry_timer(&self, ctx: &mut Context<SingletonManagerMsg>) {
        let should_retry = self.settings.automatic_hand_over_retries()
            && self.runtime.hand_over_retry_target().is_some();
        if should_retry && !ctx.is_timer_active(HAND_OVER_RETRY_TIMER_KEY) {
            ctx.start_single_timer(
                HAND_OVER_RETRY_TIMER_KEY,
                self.settings.hand_over_retry_interval(),
                SingletonManagerMsg::HandOverRetry { reply_to: None },
            );
        } else if !should_retry {
            ctx.cancel_timer(HAND_OVER_RETRY_TIMER_KEY);
        }
    }
}

pub enum SingletonManagerMsg {
    ApplyInitialObservation {
        observation: SingletonOldestObservation,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    ApplyOldestChange {
        change: SingletonOldestChange,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    MarkRemoved {
        node: UniqueAddress,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    HandOverToMe {
        from: UniqueAddress,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    HandOverInProgress {
        from: UniqueAddress,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    HandOverDone {
        from: UniqueAddress,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    HandOverRetry {
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    TakeOverFromMe {
        from: UniqueAddress,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    SingletonTerminated {
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    StopManager {
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    GetState {
        reply_to: ActorRef<SingletonManagerSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonManagerSnapshot {
    pub self_node: UniqueAddress,
    pub state: SingletonManagerState,
    pub removed_members: Vec<UniqueAddress>,
}

impl Actor for SingletonManagerActor {
    type Msg = SingletonManagerMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            SingletonManagerMsg::ApplyInitialObservation {
                observation,
                reply_to,
            } => {
                let effects = self.runtime.apply_initial_observation(observation);
                self.reconcile_hand_over_retry_timer(ctx);
                self.emit_effects(reply_to, effects);
            }
            SingletonManagerMsg::ApplyOldestChange { change, reply_to } => {
                let effects = self.runtime.apply_oldest_change(change);
                self.reconcile_hand_over_retry_timer(ctx);
                self.emit_effects(reply_to, effects);
            }
            SingletonManagerMsg::MarkRemoved { node, reply_to } => {
                let effects = self.runtime.mark_removed(node);
                self.reconcile_hand_over_retry_timer(ctx);
                self.emit_effects(reply_to, effects);
            }
            SingletonManagerMsg::HandOverToMe { from, reply_to } => {
                let effects = self.runtime.hand_over_to_me(from);
                self.reconcile_hand_over_retry_timer(ctx);
                self.emit_effects(reply_to, effects);
            }
            SingletonManagerMsg::HandOverInProgress { from, reply_to } => {
                let effects = self.runtime.hand_over_in_progress(&from);
                self.reconcile_hand_over_retry_timer(ctx);
                self.emit_effects(reply_to, effects);
            }
            SingletonManagerMsg::HandOverDone { from, reply_to } => {
                let effects = self.runtime.hand_over_done(&from);
                self.reconcile_hand_over_retry_timer(ctx);
                self.emit_effects(reply_to, effects);
            }
            SingletonManagerMsg::HandOverRetry { reply_to } => {
                let effects = self.runtime.hand_over_retry();
                self.reconcile_hand_over_retry_timer(ctx);
                self.emit_effects(reply_to, effects);
            }
            SingletonManagerMsg::TakeOverFromMe { from, reply_to } => {
                let effects = self.runtime.take_over_from_me(from);
                self.reconcile_hand_over_retry_timer(ctx);
                self.emit_effects(reply_to, effects);
            }
            SingletonManagerMsg::SingletonTerminated { reply_to } => {
                let effects = self.runtime.singleton_terminated();
                self.reconcile_hand_over_retry_timer(ctx);
                self.emit_effects(reply_to, effects);
            }
            SingletonManagerMsg::StopManager { reply_to } => {
                let effects = self.runtime.stop_manager();
                self.reconcile_hand_over_retry_timer(ctx);
                self.emit_effects(reply_to, effects);
            }
            SingletonManagerMsg::GetState { reply_to } => {
                let _ = reply_to.tell(SingletonManagerSnapshot::from(&self.runtime));
            }
        }
        Ok(())
    }
}

impl From<&SingletonManagerRuntime> for SingletonManagerSnapshot {
    fn from(runtime: &SingletonManagerRuntime) -> Self {
        let mut removed_members: Vec<_> = runtime.removed_members().iter().cloned().collect();
        removed_members.sort_by_key(UniqueAddress::ordering_key);
        Self {
            self_node: runtime.self_node().clone(),
            state: runtime.state().clone(),
            removed_members,
        }
    }
}

fn reply_effects(
    reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    effects: Vec<SingletonManagerEffect>,
) {
    if let Some(reply_to) = reply_to {
        let _ = reply_to.tell(effects);
    }
}
