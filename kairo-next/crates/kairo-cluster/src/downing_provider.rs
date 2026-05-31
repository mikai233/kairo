use std::collections::HashSet;
use std::time::Duration;

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};

use crate::{
    DowningDecision, DowningHook, Gossip, LeaderSelection, MemberStatus, UniqueAddress,
    membership_actor::ClusterMembershipMsg,
};

const STABLE_AFTER_TIMER: &str = "downing-stable-after";
const DECISION_DELAY_TIMER: &str = "downing-decision-delay";

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
pub enum DowningProviderMsg {
    ObserveGossip(Gossip),
    StableAfterElapsed,
    DecisionDelayElapsed,
    Snapshot {
        reply_to: ActorRef<DowningProviderSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DowningProviderSnapshot {
    pub responsible: bool,
    pub stable_timer_active: bool,
    pub decision_delay_active: bool,
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
mod tests {
    use std::time::Duration;

    use kairo_actor::Address;
    use kairo_testkit::{ActorSystemTestKit, await_assert};

    use super::*;
    use crate::{Member, Reachability, StaticDowningHook};

    #[test]
    fn downing_provider_waits_for_stable_after_before_applying_decision() {
        let (kit, manual_time) =
            ActorSystemTestKit::with_manual_time("downing-provider-stable-after").unwrap();
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let membership = kit
            .create_probe::<ClusterMembershipMsg>("membership")
            .unwrap();
        let provider = kit
            .system()
            .spawn(
                "downing-provider",
                DowningProviderActor::props(
                    self_node.clone(),
                    StaticDowningHook::new(DowningDecision::DownUnreachable),
                    membership.actor_ref(),
                    Duration::from_secs(1),
                ),
            )
            .unwrap();

        provider
            .tell(DowningProviderMsg::ObserveGossip(
                reachable_gossip(&self_node, &peer).with_reachability(
                    Reachability::new().unreachable(self_node.clone(), peer.clone()),
                ),
            ))
            .unwrap();

        membership
            .expect_no_msg(Duration::from_millis(100))
            .expect("downing decision must wait for stable-after");
        manual_time.advance(Duration::from_secs(1));

        let ClusterMembershipMsg::ApplyDowningDecision(decision) =
            membership.expect_msg(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected downing decision");
        };
        assert_eq!(decision, DowningDecision::DownUnreachable);
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn downing_provider_resets_stable_after_when_unreachable_set_changes() {
        let (kit, manual_time) =
            ActorSystemTestKit::with_manual_time("downing-provider-stable-reset").unwrap();
        let self_node = node("self", 1);
        let peer_a = node("peer-a", 2);
        let peer_b = node("peer-b", 3);
        let membership = kit
            .create_probe::<ClusterMembershipMsg>("membership")
            .unwrap();
        let snapshots = kit
            .create_probe::<DowningProviderSnapshot>("snapshots")
            .unwrap();
        let provider = kit
            .system()
            .spawn(
                "downing-provider",
                DowningProviderActor::props(
                    self_node.clone(),
                    StaticDowningHook::new(DowningDecision::DownUnreachable),
                    membership.actor_ref(),
                    Duration::from_secs(1),
                ),
            )
            .unwrap();

        provider
            .tell(DowningProviderMsg::ObserveGossip(
                reachable_gossip3(&self_node, &peer_a, &peer_b).with_reachability(
                    Reachability::new().unreachable(self_node.clone(), peer_a.clone()),
                ),
            ))
            .unwrap();
        expect_snapshot(&provider, &snapshots, |snapshot| {
            snapshot.relevant_unreachable == vec![peer_a.clone()]
        });
        manual_time.advance(Duration::from_millis(500));
        provider
            .tell(DowningProviderMsg::ObserveGossip(
                reachable_gossip3(&self_node, &peer_a, &peer_b).with_reachability(
                    Reachability::new()
                        .unreachable(self_node.clone(), peer_a.clone())
                        .unreachable(self_node.clone(), peer_b.clone()),
                ),
            ))
            .unwrap();
        expect_snapshot(&provider, &snapshots, |snapshot| {
            snapshot.relevant_unreachable == vec![peer_a.clone(), peer_b.clone()]
        });
        manual_time.advance(Duration::from_millis(500));

        membership
            .expect_no_msg(Duration::from_millis(100))
            .expect("changed unreachable set must reset stable-after");
        manual_time.advance(Duration::from_millis(500));

        let ClusterMembershipMsg::ApplyDowningDecision(decision) =
            membership.expect_msg(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected downing decision after reset stable-after");
        };
        assert_eq!(decision, DowningDecision::DownUnreachable);
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn downing_provider_cancels_pending_decision_when_reachability_heals() {
        let (kit, manual_time) =
            ActorSystemTestKit::with_manual_time("downing-provider-healed").unwrap();
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let membership = kit
            .create_probe::<ClusterMembershipMsg>("membership")
            .unwrap();
        let snapshots = kit
            .create_probe::<DowningProviderSnapshot>("snapshots")
            .unwrap();
        let provider = kit
            .system()
            .spawn(
                "downing-provider",
                DowningProviderActor::props(
                    self_node.clone(),
                    StaticDowningHook::new(DowningDecision::DownUnreachable),
                    membership.actor_ref(),
                    Duration::from_secs(1),
                ),
            )
            .unwrap();

        provider
            .tell(DowningProviderMsg::ObserveGossip(
                reachable_gossip(&self_node, &peer).with_reachability(
                    Reachability::new().unreachable(self_node.clone(), peer.clone()),
                ),
            ))
            .unwrap();
        provider
            .tell(DowningProviderMsg::ObserveGossip(reachable_gossip(
                &self_node, &peer,
            )))
            .unwrap();
        manual_time.advance(Duration::from_secs(1));

        membership
            .expect_no_msg(Duration::from_millis(100))
            .expect("healed reachability must cancel pending downing");
        provider
            .tell(DowningProviderMsg::Snapshot {
                reply_to: snapshots.actor_ref(),
            })
            .unwrap();
        let snapshot = snapshots.expect_msg(Duration::from_secs(1)).unwrap();
        assert!(!snapshot.stable_timer_active);
        assert!(snapshot.relevant_unreachable.is_empty());
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn downing_provider_only_applies_decision_when_self_is_leader() {
        let (kit, manual_time) =
            ActorSystemTestKit::with_manual_time("downing-provider-not-leader").unwrap();
        let self_node = node("z-self", 3);
        let leader = node("a-leader", 1);
        let unreachable = node("m-unreachable", 2);
        let membership = kit
            .create_probe::<ClusterMembershipMsg>("membership")
            .unwrap();
        let snapshots = kit
            .create_probe::<DowningProviderSnapshot>("snapshots")
            .unwrap();
        let provider = kit
            .system()
            .spawn(
                "downing-provider",
                DowningProviderActor::props(
                    self_node.clone(),
                    StaticDowningHook::new(DowningDecision::DownUnreachable),
                    membership.actor_ref(),
                    Duration::from_secs(1),
                ),
            )
            .unwrap();

        provider
            .tell(DowningProviderMsg::ObserveGossip(
                reachable_gossip3(&self_node, &leader, &unreachable).with_reachability(
                    Reachability::new().unreachable(self_node.clone(), unreachable.clone()),
                ),
            ))
            .unwrap();
        expect_snapshot(&provider, &snapshots, |snapshot| {
            !snapshot.responsible && !snapshot.relevant_unreachable.is_empty()
        });
        manual_time.advance(Duration::from_secs(1));

        membership
            .expect_no_msg(Duration::from_millis(100))
            .expect("non-leader downing provider must not apply decisions");
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn downing_provider_honors_hook_decision_delay() {
        let (kit, manual_time) =
            ActorSystemTestKit::with_manual_time("downing-provider-decision-delay").unwrap();
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let membership = kit
            .create_probe::<ClusterMembershipMsg>("membership")
            .unwrap();
        let snapshots = kit
            .create_probe::<DowningProviderSnapshot>("snapshots")
            .unwrap();
        let provider = kit
            .system()
            .spawn(
                "downing-provider",
                DowningProviderActor::props(
                    self_node.clone(),
                    DelayedHook {
                        decision: DowningDecision::DownUnreachable,
                        delay: Duration::from_secs(2),
                    },
                    membership.actor_ref(),
                    Duration::from_secs(1),
                ),
            )
            .unwrap();

        provider
            .tell(DowningProviderMsg::ObserveGossip(
                reachable_gossip(&self_node, &peer)
                    .with_reachability(Reachability::new().unreachable(self_node.clone(), peer)),
            ))
            .unwrap();
        await_assert(
            Duration::from_secs(1),
            Duration::from_millis(10),
            || -> Result<(), String> {
                provider
                    .tell(DowningProviderMsg::Snapshot {
                        reply_to: snapshots.actor_ref(),
                    })
                    .map_err(|error| error.reason().to_string())?;
                let snapshot = snapshots
                    .expect_msg(Duration::from_millis(100))
                    .map_err(|error| error.to_string())?;
                if snapshot.stable_timer_active && !snapshot.decision_delay_active {
                    Ok(())
                } else {
                    Err(format!("unexpected snapshot: {snapshot:?}"))
                }
            },
        )
        .unwrap();

        manual_time.advance(Duration::from_secs(1));
        membership
            .expect_no_msg(Duration::from_millis(100))
            .expect("decision delay must defer the downing decision");
        await_assert(
            Duration::from_secs(1),
            Duration::from_millis(10),
            || -> Result<(), String> {
                provider
                    .tell(DowningProviderMsg::Snapshot {
                        reply_to: snapshots.actor_ref(),
                    })
                    .map_err(|error| error.reason().to_string())?;
                let snapshot = snapshots
                    .expect_msg(Duration::from_millis(100))
                    .map_err(|error| error.to_string())?;
                if !snapshot.stable_timer_active && snapshot.decision_delay_active {
                    Ok(())
                } else {
                    Err(format!("unexpected snapshot: {snapshot:?}"))
                }
            },
        )
        .unwrap();

        manual_time.advance(Duration::from_secs(2));
        await_assert(
            Duration::from_secs(1),
            Duration::from_millis(10),
            || -> Result<(), String> {
                let ClusterMembershipMsg::ApplyDowningDecision(decision) = membership
                    .expect_msg(Duration::from_millis(100))
                    .map_err(|error| error.to_string())?
                else {
                    return Err("expected delayed downing decision".to_string());
                };
                if decision == DowningDecision::DownUnreachable {
                    Ok(())
                } else {
                    Err(format!("unexpected decision: {decision:?}"))
                }
            },
        )
        .unwrap();
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    fn reachable_gossip(self_node: &UniqueAddress, peer: &UniqueAddress) -> Gossip {
        Gossip::from_members([
            Member::new(self_node.clone(), vec![]).with_status(MemberStatus::Up),
            Member::new(peer.clone(), vec![]).with_status(MemberStatus::Up),
        ])
        .seen(self_node.clone())
    }

    fn reachable_gossip3(
        self_node: &UniqueAddress,
        peer_a: &UniqueAddress,
        peer_b: &UniqueAddress,
    ) -> Gossip {
        Gossip::from_members([
            Member::new(self_node.clone(), vec![]).with_status(MemberStatus::Up),
            Member::new(peer_a.clone(), vec![]).with_status(MemberStatus::Up),
            Member::new(peer_b.clone(), vec![]).with_status(MemberStatus::Up),
        ])
        .seen(self_node.clone())
    }

    fn node(name: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::new("kairo", name, None, None), uid)
    }

    fn expect_snapshot(
        provider: &ActorRef<DowningProviderMsg>,
        snapshots: &kairo_testkit::TestProbe<DowningProviderSnapshot>,
        predicate: impl FnOnce(&DowningProviderSnapshot) -> bool,
    ) {
        provider
            .tell(DowningProviderMsg::Snapshot {
                reply_to: snapshots.actor_ref(),
            })
            .unwrap();
        let snapshot = snapshots.expect_msg(Duration::from_secs(1)).unwrap();
        assert!(predicate(&snapshot), "unexpected snapshot: {snapshot:?}");
    }

    #[derive(Debug, Clone, Copy)]
    struct DelayedHook {
        decision: DowningDecision,
        delay: Duration,
    }

    impl DowningHook for DelayedHook {
        fn decide(&self, _gossip: &Gossip, _self_node: &UniqueAddress) -> DowningDecision {
            self.decision
        }

        fn decision_delay(&self, _gossip: &Gossip, _self_node: &UniqueAddress) -> Duration {
            self.delay
        }
    }
}
