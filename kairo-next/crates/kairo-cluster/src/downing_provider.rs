#![deny(missing_docs)]

use std::collections::HashSet;
use std::time::Duration;

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};

use crate::{
    DowningDecision, DowningHook, Gossip, LeaderSelection, MemberStatus, UniqueAddress,
    membership_actor::ClusterMembershipMsg,
};

const STABLE_AFTER_TIMER: &str = "downing-stable-after";
const DECISION_DELAY_TIMER: &str = "downing-decision-delay";

/// Actor that gates a [`DowningHook`] behind stable-after and leader ownership.
///
/// The timer restarts when the relevant unreachable set changes or this node
/// becomes the responsible leader. Decisions are rechecked for responsibility
/// before being forwarded to membership.
pub struct DowningProviderActor<H>
where
    H: DowningHook + Send + 'static,
{
    self_node: UniqueAddress,
    hook: H,
    membership: ActorRef<ClusterMembershipMsg>,
    stable_after: Duration,
    latest_gossip: Gossip,
    relevant_unreachable: HashSet<UniqueAddress>,
    stable_timer_active: bool,
    decision_delay_active: bool,
}

impl<H> DowningProviderActor<H>
where
    H: DowningHook + Send + 'static,
{
    /// Creates a provider for `self_node` and one explicit downing hook.
    pub fn new(
        self_node: UniqueAddress,
        hook: H,
        membership: ActorRef<ClusterMembershipMsg>,
        stable_after: Duration,
    ) -> Self {
        Self {
            self_node,
            hook,
            membership,
            stable_after,
            latest_gossip: Gossip::new(),
            relevant_unreachable: HashSet::new(),
            stable_timer_active: false,
            decision_delay_active: false,
        }
    }

    /// Creates actor props for a provider with the supplied stable-after delay.
    pub fn props(
        self_node: UniqueAddress,
        hook: H,
        membership: ActorRef<ClusterMembershipMsg>,
        stable_after: Duration,
    ) -> Props<Self> {
        Props::new(move || Self::new(self_node, hook, membership, stable_after))
    }

    fn observe_gossip(&mut self, ctx: &mut Context<DowningProviderMsg>, gossip: Gossip) {
        let was_responsible = self.is_responsible();
        let next_unreachable = relevant_unreachable(&gossip, &self.self_node);
        let unreachable_changed = next_unreachable != self.relevant_unreachable;

        self.latest_gossip = gossip;
        self.relevant_unreachable = next_unreachable;

        if self.relevant_unreachable.is_empty() {
            ctx.cancel_timer(STABLE_AFTER_TIMER);
            ctx.cancel_timer(DECISION_DELAY_TIMER);
            self.stable_timer_active = false;
            self.decision_delay_active = false;
            return;
        }

        let became_responsible = !was_responsible && self.is_responsible();
        if unreachable_changed
            || became_responsible
            || (!self.stable_timer_active && !self.decision_delay_active)
        {
            ctx.cancel_timer(DECISION_DELAY_TIMER);
            ctx.start_single_timer(
                STABLE_AFTER_TIMER,
                self.stable_after,
                DowningProviderMsg::StableAfterElapsed,
            );
            self.stable_timer_active = true;
            self.decision_delay_active = false;
        }
    }

    fn stable_after_elapsed(&mut self, ctx: &mut Context<DowningProviderMsg>) {
        self.stable_timer_active = false;
        if !self.is_responsible() || self.relevant_unreachable.is_empty() {
            return;
        }

        let decision_delay = self
            .hook
            .decision_delay(&self.latest_gossip, &self.self_node);
        if !decision_delay.is_zero() {
            ctx.start_single_timer(
                DECISION_DELAY_TIMER,
                decision_delay,
                DowningProviderMsg::DecisionDelayElapsed,
            );
            self.decision_delay_active = true;
            return;
        }

        self.apply_decision();
    }

    fn decision_delay_elapsed(&mut self) {
        self.decision_delay_active = false;
        if !self.is_responsible() || self.relevant_unreachable.is_empty() {
            return;
        }

        self.apply_decision();
    }

    fn apply_decision(&self) {
        let decision = self.hook.decide(&self.latest_gossip, &self.self_node);
        if decision != DowningDecision::NoAction {
            let _ = self
                .membership
                .tell(ClusterMembershipMsg::ApplyDowningDecision(decision));
        }
    }

    fn snapshot(&self) -> DowningProviderSnapshot {
        let mut unreachable: Vec<_> = self.relevant_unreachable.iter().cloned().collect();
        unreachable.sort_by_key(UniqueAddress::ordering_key);
        DowningProviderSnapshot {
            responsible: self.is_responsible(),
            stable_timer_active: self.stable_timer_active,
            decision_delay_active: self.decision_delay_active,
            relevant_unreachable: unreachable,
        }
    }

    fn is_responsible(&self) -> bool {
        LeaderSelection::for_gossip(&self.latest_gossip, &self.self_node).leader()
            == Some(&self.self_node)
    }
}

#[derive(Debug, Clone)]
/// Commands accepted by [`DowningProviderActor`].
pub enum DowningProviderMsg {
    /// Replaces the latest gossip view and reconciles stable-after timing.
    ObserveGossip(Gossip),
    /// Indicates that the current unreachable set remained stable long enough.
    StableAfterElapsed,
    /// Indicates that a hook-specific decision delay elapsed.
    DecisionDelayElapsed,
    /// Requests an immutable provider snapshot.
    Snapshot {
        /// Recipient of the current snapshot.
        reply_to: ActorRef<DowningProviderSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Observable state of the downing-provider actor.
pub struct DowningProviderSnapshot {
    /// Whether the local node is currently the responsible leader.
    pub responsible: bool,
    /// Whether the stable-after timer is active.
    pub stable_timer_active: bool,
    /// Whether a hook-specific decision-delay timer is active.
    pub decision_delay_active: bool,
    /// Sorted relevant unreachable node incarnations.
    pub relevant_unreachable: Vec<UniqueAddress>,
}

impl<H> Actor for DowningProviderActor<H>
where
    H: DowningHook + Send + 'static,
{
    type Msg = DowningProviderMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            DowningProviderMsg::ObserveGossip(gossip) => self.observe_gossip(ctx, gossip),
            DowningProviderMsg::StableAfterElapsed => self.stable_after_elapsed(ctx),
            DowningProviderMsg::DecisionDelayElapsed => self.decision_delay_elapsed(),
            DowningProviderMsg::Snapshot { reply_to } => {
                let _ = reply_to.tell(self.snapshot());
            }
        }
        Ok(())
    }
}

fn relevant_unreachable(gossip: &Gossip, self_node: &UniqueAddress) -> HashSet<UniqueAddress> {
    gossip
        .reachability()
        .all_unreachable_or_terminated()
        .into_iter()
        .filter(|node| node != self_node)
        .filter(|node| {
            gossip.member(node).is_some_and(|member| {
                !matches!(
                    member.status,
                    MemberStatus::Down | MemberStatus::Exiting | MemberStatus::Removed
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests;
