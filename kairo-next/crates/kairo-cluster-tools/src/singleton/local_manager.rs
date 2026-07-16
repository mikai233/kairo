#![deny(missing_docs)]

use std::sync::Arc;

use kairo_actor::{Actor, ActorPath, ActorRef, ActorResult, Context, Props, Recipient};
use kairo_cluster::UniqueAddress;

use crate::{
    SingletonManagerEffect, SingletonManagerRuntime, SingletonManagerSettings,
    SingletonManagerState, SingletonOldestChange, SingletonOldestObservation,
};

const HAND_OVER_RETRY_TIMER_KEY: &str = "local-singleton-manager-handover-retry";
const TAKE_OVER_RETRY_TIMER_KEY: &str = "local-singleton-manager-takeover-retry";

/// Singleton-manager actor that owns a real typed local singleton child.
///
/// Local start and stop effects are executed directly. Protocol effects are
/// optionally copied to a transport-facing recipient, while an independent
/// effect sink can observe complete effect batches for testing or diagnostics.
pub struct LocalSingletonManagerActor<A>
where
    A: Actor,
{
    runtime: SingletonManagerRuntime,
    settings: SingletonManagerSettings,
    effect_sink: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    remote_effect_sink: Option<Arc<dyn Recipient<Vec<SingletonManagerEffect>> + Send + Sync>>,
    singleton_name: String,
    singleton_props: Arc<dyn Fn() -> Props<A> + Send + Sync>,
    termination_message: A::Msg,
    singleton: Option<ActorRef<A::Msg>>,
}

impl<A> LocalSingletonManagerActor<A>
where
    A: Actor,
    A::Msg: Clone,
{
    /// Creates a local manager with default retry settings.
    ///
    /// The child is spawned lazily under `singleton_name` when ownership is
    /// acquired. A stop effect sends `termination_message` and waits for the
    /// watched child to terminate before completing handover.
    pub fn new<F>(
        self_node: UniqueAddress,
        singleton_name: impl Into<String>,
        singleton_props: F,
        termination_message: A::Msg,
    ) -> Self
    where
        F: Fn() -> Props<A> + Send + Sync + 'static,
    {
        Self {
            runtime: SingletonManagerRuntime::new(self_node),
            settings: SingletonManagerSettings::default(),
            effect_sink: None,
            remote_effect_sink: None,
            singleton_name: singleton_name.into(),
            singleton_props: Arc::new(singleton_props),
            termination_message,
            singleton: None,
        }
    }

    /// Creates actor properties with default retry settings.
    pub fn props<F>(
        self_node: UniqueAddress,
        singleton_name: impl Into<String>,
        singleton_props: F,
        termination_message: A::Msg,
    ) -> Props<Self>
    where
        F: Fn() -> Props<A> + Send + Sync + 'static,
    {
        let singleton_name = singleton_name.into();
        let singleton_props = Arc::new(singleton_props) as Arc<dyn Fn() -> Props<A> + Send + Sync>;
        Props::new(move || Self {
            runtime: SingletonManagerRuntime::new(self_node),
            settings: SingletonManagerSettings::default(),
            effect_sink: None,
            remote_effect_sink: None,
            singleton_name,
            singleton_props,
            termination_message: termination_message.clone(),
            singleton: None,
        })
    }

    /// Creates actor properties that copy every non-empty effect batch to a sink.
    pub fn props_with_effect_sink<F>(
        self_node: UniqueAddress,
        singleton_name: impl Into<String>,
        singleton_props: F,
        termination_message: A::Msg,
        settings: SingletonManagerSettings,
        effect_sink: ActorRef<Vec<SingletonManagerEffect>>,
    ) -> Props<Self>
    where
        F: Fn() -> Props<A> + Send + Sync + 'static,
    {
        let singleton_name = singleton_name.into();
        let singleton_props = Arc::new(singleton_props);
        Props::new(move || Self {
            runtime: SingletonManagerRuntime::new(self_node),
            settings,
            effect_sink: Some(effect_sink.clone()),
            remote_effect_sink: None,
            singleton_name,
            singleton_props,
            termination_message: termination_message.clone(),
            singleton: None,
        })
    }

    /// Creates actor properties that forward only remote protocol effects.
    ///
    /// Child lifecycle and manager-stop effects remain local to this adapter.
    pub fn props_with_remote_effect_sink<F, R>(
        self_node: UniqueAddress,
        singleton_name: impl Into<String>,
        singleton_props: F,
        termination_message: A::Msg,
        settings: SingletonManagerSettings,
        remote_effect_sink: R,
    ) -> Props<Self>
    where
        F: Fn() -> Props<A> + Send + Sync + 'static,
        R: Recipient<Vec<SingletonManagerEffect>> + Send + Sync + 'static,
    {
        let singleton_name = singleton_name.into();
        let singleton_props = Arc::new(singleton_props) as Arc<dyn Fn() -> Props<A> + Send + Sync>;
        let remote_effect_sink = Arc::new(remote_effect_sink)
            as Arc<dyn Recipient<Vec<SingletonManagerEffect>> + Send + Sync>;
        Props::new(move || Self {
            runtime: SingletonManagerRuntime::new(self_node.clone()),
            settings: settings.clone(),
            effect_sink: None,
            remote_effect_sink: Some(Arc::clone(&remote_effect_sink)),
            singleton_name: singleton_name.clone(),
            singleton_props: Arc::clone(&singleton_props),
            termination_message: termination_message.clone(),
            singleton: None,
        })
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
        let remote_effects: Vec<_> = effects
            .iter()
            .filter(|effect| is_remote_effect(effect))
            .cloned()
            .collect();
        if !remote_effects.is_empty()
            && let Some(remote_effect_sink) = &self.remote_effect_sink
        {
            let _ = remote_effect_sink.tell(remote_effects);
        }
        reply_effects(reply_to, effects);
    }

    fn reconcile_hand_over_retry_timer(&self, ctx: &mut Context<LocalSingletonManagerMsg<A::Msg>>) {
        let should_retry = self.settings.automatic_hand_over_retries()
            && self.runtime.hand_over_retry_target().is_some();
        if should_retry && !ctx.is_timer_active(HAND_OVER_RETRY_TIMER_KEY) {
            ctx.start_single_timer(
                HAND_OVER_RETRY_TIMER_KEY,
                self.settings.hand_over_retry_interval(),
                LocalSingletonManagerMsg::HandOverRetry { reply_to: None },
            );
        } else if !should_retry {
            ctx.cancel_timer(HAND_OVER_RETRY_TIMER_KEY);
        }
    }

    fn reconcile_take_over_retry_timer(&self, ctx: &mut Context<LocalSingletonManagerMsg<A::Msg>>) {
        let should_retry = self.settings.automatic_hand_over_retries()
            && self.runtime.take_over_retry_target().is_some();
        if should_retry && !ctx.is_timer_active(TAKE_OVER_RETRY_TIMER_KEY) {
            ctx.start_single_timer(
                TAKE_OVER_RETRY_TIMER_KEY,
                self.settings.hand_over_retry_interval(),
                LocalSingletonManagerMsg::TakeOverRetry { reply_to: None },
            );
        } else if !should_retry {
            ctx.cancel_timer(TAKE_OVER_RETRY_TIMER_KEY);
        }
    }

    fn reconcile_retry_timers(&self, ctx: &mut Context<LocalSingletonManagerMsg<A::Msg>>) {
        self.reconcile_hand_over_retry_timer(ctx);
        self.reconcile_take_over_retry_timer(ctx);
    }

    fn apply_effects(
        &mut self,
        ctx: &mut Context<LocalSingletonManagerMsg<A::Msg>>,
        effects: &[SingletonManagerEffect],
    ) -> ActorResult {
        for effect in effects {
            match effect {
                SingletonManagerEffect::StartSingleton => self.start_singleton(ctx)?,
                SingletonManagerEffect::StopSingleton => self.stop_singleton(ctx)?,
                SingletonManagerEffect::StopManager => ctx.stop(ctx.myself())?,
                SingletonManagerEffect::SendHandOverToMe { .. }
                | SingletonManagerEffect::SendHandOverInProgress { .. }
                | SingletonManagerEffect::SendHandOverDone { .. }
                | SingletonManagerEffect::SendTakeOverFromMe { .. } => {}
            }
        }
        Ok(())
    }

    fn start_singleton(
        &mut self,
        ctx: &mut Context<LocalSingletonManagerMsg<A::Msg>>,
    ) -> ActorResult {
        if self.singleton.is_some() {
            return Ok(());
        }

        let singleton = ctx.spawn(&self.singleton_name, (self.singleton_props)())?;
        ctx.watch_with(&singleton, LocalSingletonManagerMsg::SingletonTerminated)?;
        self.singleton = Some(singleton);
        Ok(())
    }

    fn stop_singleton(
        &mut self,
        ctx: &mut Context<LocalSingletonManagerMsg<A::Msg>>,
    ) -> ActorResult {
        if let Some(singleton) = &self.singleton {
            let _ = singleton.tell(self.termination_message.clone());
        } else {
            let effects = self.runtime.singleton_terminated();
            self.apply_effects(ctx, &effects)?;
        }
        Ok(())
    }
}

fn is_remote_effect(effect: &SingletonManagerEffect) -> bool {
    matches!(
        effect,
        SingletonManagerEffect::SendHandOverToMe { .. }
            | SingletonManagerEffect::SendHandOverInProgress { .. }
            | SingletonManagerEffect::SendHandOverDone { .. }
            | SingletonManagerEffect::SendTakeOverFromMe { .. }
    )
}

/// Commands accepted by [`LocalSingletonManagerActor`].
pub enum LocalSingletonManagerMsg<M: Send + 'static> {
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
    /// Reports watched singleton-child termination.
    SingletonTerminated,
    /// Requests terminal manager shutdown.
    StopManager {
        /// Optional recipient for the effects produced by this turn.
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    /// Requests an immutable manager and child state snapshot.
    GetState {
        /// Recipient for the snapshot.
        reply_to: ActorRef<LocalSingletonManagerSnapshot>,
    },
    /// Requests the currently owned singleton child, if any.
    GetSingleton {
        /// Recipient for the optional typed child reference.
        reply_to: ActorRef<Option<ActorRef<M>>>,
    },
}

/// Observable state of a [`LocalSingletonManagerActor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSingletonManagerSnapshot {
    /// Exact local member incarnation.
    pub self_node: UniqueAddress,
    /// Current ownership and handover state.
    pub state: SingletonManagerState,
    /// Definitively removed member incarnations in deterministic order.
    pub removed_members: Vec<UniqueAddress>,
    /// Current child path, or none when no singleton child is owned.
    pub singleton_path: Option<ActorPath>,
}

impl<A> Actor for LocalSingletonManagerActor<A>
where
    A: Actor,
    A::Msg: Clone,
{
    type Msg = LocalSingletonManagerMsg<A::Msg>;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            LocalSingletonManagerMsg::ApplyInitialObservation {
                observation,
                reply_to,
            } => {
                let effects = self.runtime.apply_initial_observation(observation);
                self.apply_effects(ctx, &effects)?;
                self.reconcile_retry_timers(ctx);
                self.emit_effects(reply_to, effects);
            }
            LocalSingletonManagerMsg::ApplyOldestChange { change, reply_to } => {
                let effects = self.runtime.apply_oldest_change(change);
                self.apply_effects(ctx, &effects)?;
                self.reconcile_retry_timers(ctx);
                self.emit_effects(reply_to, effects);
            }
            LocalSingletonManagerMsg::MarkRemoved { node, reply_to } => {
                let effects = self.runtime.mark_removed(node);
                self.apply_effects(ctx, &effects)?;
                self.reconcile_retry_timers(ctx);
                self.emit_effects(reply_to, effects);
            }
            LocalSingletonManagerMsg::HandOverToMe { from, reply_to } => {
                let effects = self.runtime.hand_over_to_me(from);
                self.apply_effects(ctx, &effects)?;
                self.reconcile_retry_timers(ctx);
                self.emit_effects(reply_to, effects);
            }
            LocalSingletonManagerMsg::HandOverInProgress { from, reply_to } => {
                let effects = self.runtime.hand_over_in_progress(&from);
                self.apply_effects(ctx, &effects)?;
                self.reconcile_retry_timers(ctx);
                self.emit_effects(reply_to, effects);
            }
            LocalSingletonManagerMsg::HandOverDone { from, reply_to } => {
                let effects = self.runtime.hand_over_done(&from);
                self.apply_effects(ctx, &effects)?;
                self.reconcile_retry_timers(ctx);
                self.emit_effects(reply_to, effects);
            }
            LocalSingletonManagerMsg::HandOverRetry { reply_to } => {
                let effects = self.runtime.hand_over_retry();
                self.apply_effects(ctx, &effects)?;
                self.reconcile_retry_timers(ctx);
                self.emit_effects(reply_to, effects);
            }
            LocalSingletonManagerMsg::TakeOverRetry { reply_to } => {
                let effects = self.runtime.take_over_retry();
                self.apply_effects(ctx, &effects)?;
                self.reconcile_retry_timers(ctx);
                self.emit_effects(reply_to, effects);
            }
            LocalSingletonManagerMsg::TakeOverFromMe { from, reply_to } => {
                let effects = self.runtime.take_over_from_me(from);
                self.apply_effects(ctx, &effects)?;
                self.reconcile_retry_timers(ctx);
                self.emit_effects(reply_to, effects);
            }
            LocalSingletonManagerMsg::SingletonTerminated => {
                self.singleton = None;
                let effects = self.runtime.singleton_terminated();
                self.apply_effects(ctx, &effects)?;
                self.reconcile_retry_timers(ctx);
                self.emit_effects(None, effects);
            }
            LocalSingletonManagerMsg::StopManager { reply_to } => {
                let effects = self.runtime.stop_manager();
                self.apply_effects(ctx, &effects)?;
                self.reconcile_retry_timers(ctx);
                self.emit_effects(reply_to, effects);
            }
            LocalSingletonManagerMsg::GetState { reply_to } => {
                let _ = reply_to.tell(LocalSingletonManagerSnapshot::from_manager(self));
            }
            LocalSingletonManagerMsg::GetSingleton { reply_to } => {
                let _ = reply_to.tell(self.singleton.clone());
            }
        }
        Ok(())
    }
}

impl LocalSingletonManagerSnapshot {
    fn from_manager<A>(manager: &LocalSingletonManagerActor<A>) -> Self
    where
        A: Actor,
    {
        let mut removed_members: Vec<_> =
            manager.runtime.removed_members().iter().cloned().collect();
        removed_members.sort_by_key(UniqueAddress::ordering_key);
        Self {
            self_node: manager.runtime.self_node().clone(),
            state: manager.runtime.state().clone(),
            removed_members,
            singleton_path: manager
                .singleton
                .as_ref()
                .map(|singleton| singleton.path().clone()),
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
