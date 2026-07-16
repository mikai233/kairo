#![deny(missing_docs)]

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};
use kairo_cluster::UniqueAddress;

use crate::{
    SingletonManagerEffect, SingletonManagerRuntime, SingletonManagerSettings,
    SingletonManagerState, SingletonOldestChange, SingletonOldestObservation,
};

const HAND_OVER_RETRY_TIMER_KEY: &str = "singleton-manager-handover-retry";
const TAKE_OVER_RETRY_TIMER_KEY: &str = "singleton-manager-takeover-retry";

/// Mailbox adapter for the pure [`SingletonManagerRuntime`] state machine.
///
/// This adapter owns retry timers and manager termination, but deliberately
/// leaves singleton-child and transport effects to an optional effect sink.
pub struct SingletonManagerActor {
    runtime: SingletonManagerRuntime,
    settings: SingletonManagerSettings,
    effect_sink: Option<ActorRef<Vec<SingletonManagerEffect>>>,
}

impl SingletonManagerActor {
    /// Creates a manager for `self_node` with default retry settings.
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            runtime: SingletonManagerRuntime::new(self_node),
            settings: SingletonManagerSettings::default(),
            effect_sink: None,
        }
    }

    /// Creates actor properties with default retry settings.
    pub fn props(self_node: UniqueAddress) -> Props<Self> {
        Props::new(move || Self::new(self_node))
    }

    /// Creates a manager with explicit retry settings.
    pub fn with_settings(self_node: UniqueAddress, settings: SingletonManagerSettings) -> Self {
        Self {
            runtime: SingletonManagerRuntime::new(self_node),
            settings,
            effect_sink: None,
        }
    }

    /// Creates actor properties with explicit retry settings.
    pub fn props_with_settings(
        self_node: UniqueAddress,
        settings: SingletonManagerSettings,
    ) -> Props<Self> {
        Props::new(move || Self::with_settings(self_node, settings))
    }

    /// Creates a manager that copies every non-empty effect batch to `effect_sink`.
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

    /// Creates actor properties that copy effect batches to `effect_sink`.
    pub fn props_with_effect_sink(
        self_node: UniqueAddress,
        settings: SingletonManagerSettings,
        effect_sink: ActorRef<Vec<SingletonManagerEffect>>,
    ) -> Props<Self> {
        Props::new(move || Self::with_effect_sink(self_node, settings, effect_sink.clone()))
    }

    /// Returns the underlying ownership state machine.
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

    fn reconcile_take_over_retry_timer(&self, ctx: &mut Context<SingletonManagerMsg>) {
        let should_retry = self.settings.automatic_hand_over_retries()
            && self.runtime.take_over_retry_target().is_some();
        if should_retry && !ctx.is_timer_active(TAKE_OVER_RETRY_TIMER_KEY) {
            ctx.start_single_timer(
                TAKE_OVER_RETRY_TIMER_KEY,
                self.settings.hand_over_retry_interval(),
                SingletonManagerMsg::TakeOverRetry { reply_to: None },
            );
        } else if !should_retry {
            ctx.cancel_timer(TAKE_OVER_RETRY_TIMER_KEY);
        }
    }

    fn reconcile_retry_timers(&self, ctx: &mut Context<SingletonManagerMsg>) {
        self.reconcile_hand_over_retry_timer(ctx);
        self.reconcile_take_over_retry_timer(ctx);
    }

    fn apply_effects(
        &self,
        ctx: &mut Context<SingletonManagerMsg>,
        effects: &[SingletonManagerEffect],
    ) -> ActorResult {
        for effect in effects {
            match effect {
                SingletonManagerEffect::StopManager => ctx.stop(ctx.myself())?,
                SingletonManagerEffect::StartSingleton
                | SingletonManagerEffect::StopSingleton
                | SingletonManagerEffect::SendHandOverToMe { .. }
                | SingletonManagerEffect::SendHandOverInProgress { .. }
                | SingletonManagerEffect::SendHandOverDone { .. }
                | SingletonManagerEffect::SendTakeOverFromMe { .. } => {}
            }
        }
        Ok(())
    }

    fn finish_turn(
        &self,
        ctx: &mut Context<SingletonManagerMsg>,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
        effects: Vec<SingletonManagerEffect>,
    ) -> ActorResult {
        self.apply_effects(ctx, &effects)?;
        self.reconcile_retry_timers(ctx);
        self.emit_effects(reply_to, effects);
        Ok(())
    }
}

/// Commands accepted by [`SingletonManagerActor`].
pub enum SingletonManagerMsg {
    /// Applies the first role-scoped oldest-member observation.
    ApplyInitialObservation {
        /// Initial ordered ownership observation.
        observation: SingletonOldestObservation,
        /// Optional recipient for the effects produced by this turn.
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    /// Applies a later role-scoped oldest-member change.
    ApplyOldestChange {
        /// Membership-derived ownership change.
        change: SingletonOldestChange,
        /// Optional recipient for the effects produced by this turn.
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    /// Records definitive removal of one member incarnation.
    MarkRemoved {
        /// Exact removed member incarnation.
        node: UniqueAddress,
        /// Optional recipient for the effects produced by this turn.
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    /// Requests handover from this manager to `from`.
    HandOverToMe {
        /// Exact requesting successor incarnation.
        from: UniqueAddress,
        /// Optional recipient for the effects produced by this turn.
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    /// Confirms that a prior owner has started handover.
    HandOverInProgress {
        /// Exact prior-owner incarnation.
        from: UniqueAddress,
        /// Optional recipient for the effects produced by this turn.
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    /// Confirms that a prior owner has completed handover.
    HandOverDone {
        /// Exact prior-owner incarnation.
        from: UniqueAddress,
        /// Optional recipient for the effects produced by this turn.
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    /// Retries the current request to a prior owner.
    HandOverRetry {
        /// Optional recipient for the effects produced by this turn.
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    /// Retries the current request to a newly selected owner.
    TakeOverRetry {
        /// Optional recipient for the effects produced by this turn.
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    /// Asks the previous owner to transfer responsibility to `from`.
    TakeOverFromMe {
        /// Exact newly selected owner incarnation.
        from: UniqueAddress,
        /// Optional recipient for the effects produced by this turn.
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    /// Reports that the singleton child has terminated.
    SingletonTerminated {
        /// Optional recipient for the effects produced by this turn.
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    /// Requests terminal manager shutdown.
    StopManager {
        /// Optional recipient for the effects produced by this turn.
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    /// Requests an immutable state snapshot.
    GetState {
        /// Recipient for the snapshot.
        reply_to: ActorRef<SingletonManagerSnapshot>,
    },
}

/// Observable state of a [`SingletonManagerActor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonManagerSnapshot {
    /// Exact local member incarnation.
    pub self_node: UniqueAddress,
    /// Current ownership and handover state.
    pub state: SingletonManagerState,
    /// Definitively removed member incarnations in deterministic order.
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
                self.finish_turn(ctx, reply_to, effects)?;
            }
            SingletonManagerMsg::ApplyOldestChange { change, reply_to } => {
                let effects = self.runtime.apply_oldest_change(change);
                self.finish_turn(ctx, reply_to, effects)?;
            }
            SingletonManagerMsg::MarkRemoved { node, reply_to } => {
                let effects = self.runtime.mark_removed(node);
                self.finish_turn(ctx, reply_to, effects)?;
            }
            SingletonManagerMsg::HandOverToMe { from, reply_to } => {
                let effects = self.runtime.hand_over_to_me(from);
                self.finish_turn(ctx, reply_to, effects)?;
            }
            SingletonManagerMsg::HandOverInProgress { from, reply_to } => {
                let effects = self.runtime.hand_over_in_progress(&from);
                self.finish_turn(ctx, reply_to, effects)?;
            }
            SingletonManagerMsg::HandOverDone { from, reply_to } => {
                let effects = self.runtime.hand_over_done(&from);
                self.finish_turn(ctx, reply_to, effects)?;
            }
            SingletonManagerMsg::HandOverRetry { reply_to } => {
                let effects = self.runtime.hand_over_retry();
                self.finish_turn(ctx, reply_to, effects)?;
            }
            SingletonManagerMsg::TakeOverRetry { reply_to } => {
                let effects = self.runtime.take_over_retry();
                self.finish_turn(ctx, reply_to, effects)?;
            }
            SingletonManagerMsg::TakeOverFromMe { from, reply_to } => {
                let effects = self.runtime.take_over_from_me(from);
                self.finish_turn(ctx, reply_to, effects)?;
            }
            SingletonManagerMsg::SingletonTerminated { reply_to } => {
                let effects = self.runtime.singleton_terminated();
                self.finish_turn(ctx, reply_to, effects)?;
            }
            SingletonManagerMsg::StopManager { reply_to } => {
                let effects = self.runtime.stop_manager();
                self.finish_turn(ctx, reply_to, effects)?;
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
