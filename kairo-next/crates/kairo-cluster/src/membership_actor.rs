use kairo_actor::{Actor, ActorRef, ActorResult, Context};

use crate::{
    ClusterEventPublisherMsg, CurrentClusterState, DowningDecision, DowningPlan,
    DowningProviderMsg, Gossip, GossipEnvelope, Join, LeaderSelection, Member, ReachabilityStatus,
    UniqueAddress, VectorClockOrdering, Welcome,
};

#[derive(Debug, Clone)]
pub enum ClusterMembershipMsg {
    JoinSelf,
    Join {
        join: Join,
        reply_to: Option<ActorRef<ClusterMembershipMsg>>,
    },
    Welcome(Box<Welcome>),
    Gossip {
        envelope: Box<GossipEnvelope>,
        reply_to: Option<ActorRef<ClusterMembershipMsg>>,
    },
    MarkUnreachable {
        observer: UniqueAddress,
        subject: UniqueAddress,
    },
    MarkReachable {
        observer: UniqueAddress,
        subject: UniqueAddress,
    },
    Down {
        node: UniqueAddress,
    },
    ApplyDowningDecision(DowningDecision),
    RegisterDowningProvider {
        provider: ActorRef<DowningProviderMsg>,
    },
    LeaderActionsTick,
    SendCurrentGossip {
        reply_to: ActorRef<Gossip>,
    },
    SendCurrentState {
        reply_to: ActorRef<CurrentClusterState>,
    },
}

pub struct ClusterMembership {
    self_node: UniqueAddress,
    roles: Vec<String>,
    gossip: Gossip,
    event_publisher: ActorRef<ClusterEventPublisherMsg>,
    sequence_nr: u64,
    timestamp: u64,
    initialized: bool,
    downing_provider: Option<ActorRef<DowningProviderMsg>>,
}

impl ClusterMembership {
    pub fn new(
        self_node: UniqueAddress,
        roles: Vec<String>,
        event_publisher: ActorRef<ClusterEventPublisherMsg>,
    ) -> Self {
        Self {
            self_node,
            roles,
            gossip: Gossip::new(),
            event_publisher,
            sequence_nr: 0,
            timestamp: 0,
            initialized: false,
            downing_provider: None,
        }
    }

    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }

    fn join_self(&mut self) {
        self.join(
            Join {
                node: self.self_node.clone(),
                roles: self.roles.clone(),
            },
            None,
        );
    }

    fn join(&mut self, join: Join, reply_to: Option<ActorRef<ClusterMembershipMsg>>) {
        let existing_same_address = self
            .gossip
            .members()
            .iter()
            .find(|member| member.unique_address.address == join.node.address);

        if let Some(existing) = existing_same_address {
            if existing.unique_address == join.node {
                self.reply_welcome(reply_to);
            } else {
                let reachability = self
                    .gossip
                    .reachability()
                    .terminated(self.self_node.clone(), existing.unique_address.clone());
                let gossip = self
                    .gossip
                    .with_reachability(reachability)
                    .mark_down(&existing.unique_address);
                self.update_latest_gossip(gossip);
            }
            return;
        }

        let mut gossip = self
            .gossip
            .add_member(Member::new(join.node.clone(), join.roles));
        if !gossip.has_member(&self.self_node) {
            gossip = gossip.add_member(Member::new(self.self_node.clone(), self.roles.clone()));
        }
        self.update_latest_gossip(gossip);
        self.initialized |= self.gossip.has_member(&self.self_node);

        if join.node == self.self_node {
            self.run_leader_actions();
        } else {
            self.reply_welcome(reply_to);
        }
    }

    fn welcome(&mut self, welcome: Welcome) {
        if self.initialized
            || welcome.from == self.self_node
            || !welcome.gossip.has_member(&self.self_node)
        {
            return;
        }

        self.gossip = welcome.gossip.seen(self.self_node.clone());
        self.initialized = true;
        self.publish_current_gossip();
    }

    fn receive_gossip(
        &mut self,
        envelope: GossipEnvelope,
        reply_to: Option<ActorRef<ClusterMembershipMsg>>,
    ) {
        if envelope.to != self.self_node
            || !self.gossip.has_member(&envelope.from)
            || self
                .gossip
                .reachability()
                .status(&self.self_node, &envelope.from)
                != ReachabilityStatus::Reachable
            || !envelope.gossip.has_member(&self.self_node)
        {
            return;
        }

        let comparison = envelope.gossip.version().compare(self.gossip.version());
        let (winning_gossip, talkback) = match comparison {
            VectorClockOrdering::Same => (
                envelope.gossip.merge_seen(&self.gossip),
                !envelope.gossip.seen_by().contains(&self.self_node),
            ),
            VectorClockOrdering::Before => (self.gossip.clone(), true),
            VectorClockOrdering::After => (
                envelope.gossip.clone(),
                !envelope.gossip.seen_by().contains(&self.self_node),
            ),
            VectorClockOrdering::Concurrent => (envelope.gossip.merge(&self.gossip), true),
        };

        let changed = winning_gossip.seen(self.self_node.clone());
        if changed != self.gossip {
            self.gossip = changed;
            self.publish_current_gossip();
        }

        if talkback {
            self.reply_gossip(envelope.from, reply_to);
        }
    }

    fn mark_unreachable(&mut self, observer: UniqueAddress, subject: UniqueAddress) {
        let reachability = self.gossip.reachability().unreachable(observer, subject);
        self.update_latest_gossip(self.gossip.with_reachability(reachability));
    }

    fn mark_reachable(&mut self, observer: UniqueAddress, subject: UniqueAddress) {
        let reachability = self.gossip.reachability().reachable(observer, subject);
        self.update_latest_gossip(self.gossip.with_reachability(reachability));
    }

    fn down(&mut self, node: &UniqueAddress) {
        if self
            .gossip
            .member(node)
            .is_some_and(|member| member.status != crate::MemberStatus::Down)
        {
            self.update_latest_gossip(self.gossip.mark_down(node));
        }
    }

    fn apply_downing_decision(&mut self, decision: DowningDecision) {
        let plan = DowningPlan::from_decision(decision, &self.gossip, &self.self_node);
        let changed = plan.apply_to(&self.gossip, &self.self_node);
        if changed != self.gossip {
            self.gossip = changed;
            self.publish_current_gossip();
        }
    }

    fn register_downing_provider(&mut self, provider: ActorRef<DowningProviderMsg>) {
        let _ = provider.tell(DowningProviderMsg::ObserveGossip(self.gossip.clone()));
        self.downing_provider = Some(provider);
    }

    fn run_leader_actions(&mut self) {
        if LeaderSelection::for_gossip(&self.gossip, &self.self_node).leader()
            != Some(&self.self_node)
        {
            return;
        }

        let removal_timestamp = self.next_timestamp();
        if let Ok(outcome) = crate::LeaderActions::on_convergence(
            &self.gossip,
            &self.self_node,
            removal_timestamp,
            [],
        ) && outcome.changed()
        {
            self.gossip = outcome.gossip;
            self.publish_current_gossip();
        }
    }

    fn update_latest_gossip(&mut self, gossip: Gossip) {
        let gossip = gossip
            .increment_version(&self.self_node)
            .only_seen(self.self_node.clone());
        if gossip != self.gossip {
            self.gossip = gossip;
            self.publish_current_gossip();
        }
    }

    fn publish_current_gossip(&self) {
        let _ = self
            .event_publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                self.gossip.clone(),
            ));
        if let Some(provider) = &self.downing_provider {
            let _ = provider.tell(DowningProviderMsg::ObserveGossip(self.gossip.clone()));
        }
    }

    fn reply_welcome(&self, reply_to: Option<ActorRef<ClusterMembershipMsg>>) {
        if let Some(reply_to) = reply_to {
            let _ = reply_to.tell(ClusterMembershipMsg::Welcome(Box::new(Welcome {
                from: self.self_node.clone(),
                gossip: self.gossip.clone(),
            })));
        }
    }

    fn reply_gossip(
        &mut self,
        to: UniqueAddress,
        reply_to: Option<ActorRef<ClusterMembershipMsg>>,
    ) {
        if let Some(reply_to) = reply_to {
            self.sequence_nr += 1;
            let _ = reply_to.tell(ClusterMembershipMsg::Gossip {
                envelope: Box::new(GossipEnvelope {
                    from: self.self_node.clone(),
                    to,
                    sequence_nr: self.sequence_nr,
                    gossip: self.gossip.clone(),
                }),
                reply_to: None,
            });
        }
    }

    fn next_timestamp(&mut self) -> u64 {
        self.timestamp += 1;
        self.timestamp
    }
}

impl Actor for ClusterMembership {
    type Msg = ClusterMembershipMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterMembershipMsg::JoinSelf => self.join_self(),
            ClusterMembershipMsg::Join { join, reply_to } => self.join(join, reply_to),
            ClusterMembershipMsg::Welcome(welcome) => self.welcome(*welcome),
            ClusterMembershipMsg::Gossip { envelope, reply_to } => {
                self.receive_gossip(*envelope, reply_to);
            }
            ClusterMembershipMsg::MarkUnreachable { observer, subject } => {
                self.mark_unreachable(observer, subject);
            }
            ClusterMembershipMsg::MarkReachable { observer, subject } => {
                self.mark_reachable(observer, subject);
            }
            ClusterMembershipMsg::Down { node } => self.down(&node),
            ClusterMembershipMsg::ApplyDowningDecision(decision) => {
                self.apply_downing_decision(decision);
            }
            ClusterMembershipMsg::RegisterDowningProvider { provider } => {
                self.register_downing_provider(provider);
            }
            ClusterMembershipMsg::LeaderActionsTick => self.run_leader_actions(),
            ClusterMembershipMsg::SendCurrentGossip { reply_to } => {
                let _ = reply_to.tell(self.gossip.clone());
            }
            ClusterMembershipMsg::SendCurrentState { reply_to } => {
                let _ = reply_to.tell(CurrentClusterState::from_gossip(
                    &self.gossip,
                    &self.self_node,
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kairo_actor::{Address, Props};
    use kairo_testkit::{ActorSystemTestKit, await_assert};

    use super::*;
    use crate::{
        ClusterEvent, ClusterEventPublisher, DowningProviderActor, DowningProviderSnapshot,
        MemberEvent, MemberStatus, ReachabilityEvent, StaticDowningHook, SubscriptionInitialState,
    };

    #[test]
    fn join_self_forms_cluster_and_promotes_self_to_up() {
        let kit = ActorSystemTestKit::new("cluster-membership-join-self").unwrap();
        let self_node = node("self", 1);
        let (membership, event_probe) = spawn_membership(&kit, self_node.clone(), "membership");
        let gossip_probe = kit.create_probe::<Gossip>("gossip").unwrap();

        membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
        membership
            .tell(ClusterMembershipMsg::SendCurrentGossip {
                reply_to: gossip_probe.actor_ref(),
            })
            .unwrap();

        let gossip = gossip_probe.expect_msg(Duration::from_secs(1)).unwrap();
        assert_eq!(
            gossip.member(&self_node).map(|member| member.status),
            Some(MemberStatus::Up)
        );
        expect_event(&event_probe, |event| {
            matches!(event, ClusterEvent::Member(MemberEvent::Joined(_)))
        });
        expect_event(&event_probe, |event| {
            matches!(event, ClusterEvent::Member(MemberEvent::Up(_)))
        });
    }

    #[test]
    fn remote_join_adds_joining_member_and_replies_with_welcome() {
        let kit = ActorSystemTestKit::new("cluster-membership-remote-join").unwrap();
        let self_node = node("self", 1);
        let joining = node("joining", 2);
        let (membership, _events) = spawn_membership(&kit, self_node.clone(), "membership");
        let reply_probe = kit.create_probe::<ClusterMembershipMsg>("welcome").unwrap();

        membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
        membership
            .tell(ClusterMembershipMsg::Join {
                join: Join {
                    node: joining.clone(),
                    roles: vec!["backend".to_string()],
                },
                reply_to: Some(reply_probe.actor_ref()),
            })
            .unwrap();

        let ClusterMembershipMsg::Welcome(welcome) =
            reply_probe.expect_msg(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected welcome");
        };
        assert_eq!(welcome.from, self_node);
        assert_eq!(
            welcome.gossip.member(&joining).map(|member| member.status),
            Some(MemberStatus::Joining)
        );
    }

    #[test]
    fn welcome_initializes_empty_joining_node() {
        let kit = ActorSystemTestKit::new("cluster-membership-welcome").unwrap();
        let seed = node("seed", 1);
        let joining = node("joining", 2);
        let (membership, _events) = spawn_membership(&kit, joining.clone(), "membership");
        let gossip_probe = kit.create_probe::<Gossip>("gossip").unwrap();
        let gossip = Gossip::from_members([
            member(seed.clone(), MemberStatus::Up),
            member(joining.clone(), MemberStatus::Joining),
        ]);

        membership
            .tell(ClusterMembershipMsg::Welcome(Box::new(Welcome {
                from: seed,
                gossip,
            })))
            .unwrap();
        membership
            .tell(ClusterMembershipMsg::SendCurrentGossip {
                reply_to: gossip_probe.actor_ref(),
            })
            .unwrap();

        let gossip = gossip_probe.expect_msg(Duration::from_secs(1)).unwrap();
        assert!(gossip.has_member(&joining));
        assert!(gossip.seen_by().contains(&joining));
    }

    #[test]
    fn gossip_merge_updates_local_state_and_talks_back_when_remote_has_old_view() {
        let kit = ActorSystemTestKit::new("cluster-membership-gossip").unwrap();
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let joining = node("joining", 3);
        let (membership, _events) = spawn_membership(&kit, self_node.clone(), "membership");
        let reply_probe = kit
            .create_probe::<ClusterMembershipMsg>("gossip-reply")
            .unwrap();
        let gossip_probe = kit.create_probe::<Gossip>("gossip").unwrap();

        membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
        membership
            .tell(ClusterMembershipMsg::Join {
                join: Join {
                    node: peer.clone(),
                    roles: Vec::new(),
                },
                reply_to: None,
            })
            .unwrap();
        let remote_gossip = Gossip::from_members([
            member(self_node.clone(), MemberStatus::Up),
            member(peer.clone(), MemberStatus::Up),
            member(joining.clone(), MemberStatus::Joining),
        ])
        .increment_version(&peer)
        .only_seen(peer.clone());
        membership
            .tell(ClusterMembershipMsg::Gossip {
                envelope: Box::new(GossipEnvelope {
                    from: peer.clone(),
                    to: self_node.clone(),
                    sequence_nr: 1,
                    gossip: remote_gossip,
                }),
                reply_to: Some(reply_probe.actor_ref()),
            })
            .unwrap();

        membership
            .tell(ClusterMembershipMsg::SendCurrentGossip {
                reply_to: gossip_probe.actor_ref(),
            })
            .unwrap();
        let gossip = gossip_probe.expect_msg(Duration::from_secs(1)).unwrap();
        assert!(gossip.has_member(&joining));
        assert!(gossip.seen_by().contains(&self_node));

        let ClusterMembershipMsg::Gossip { envelope, .. } =
            reply_probe.expect_msg(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected gossip talkback");
        };
        assert_eq!(envelope.from, self_node);
        assert_eq!(envelope.to, peer);
    }

    #[test]
    fn down_marks_member_down_and_publishes_event() {
        let kit = ActorSystemTestKit::new("cluster-membership-down").unwrap();
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let (membership, events) = spawn_membership(&kit, self_node, "membership");

        membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
        let _ = events.expect_msg(Duration::from_secs(1)).unwrap();
        let _ = events.expect_msg(Duration::from_secs(1)).unwrap();
        membership
            .tell(ClusterMembershipMsg::Join {
                join: Join {
                    node: peer.clone(),
                    roles: Vec::new(),
                },
                reply_to: None,
            })
            .unwrap();
        let _ = events.expect_msg(Duration::from_secs(1)).unwrap();
        membership
            .tell(ClusterMembershipMsg::Down { node: peer.clone() })
            .unwrap();

        expect_event(&events, |event| {
            matches!(
                event,
                ClusterEvent::Member(MemberEvent::Downed(member))
                    if member.unique_address == peer
            )
        });
    }

    #[test]
    fn reachability_updates_publish_unreachable_and_reachable_events() {
        let kit = ActorSystemTestKit::new("cluster-membership-reachability").unwrap();
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let (membership, events) = spawn_membership(&kit, self_node.clone(), "membership");

        membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
        let _ = events.expect_msg(Duration::from_secs(1)).unwrap();
        let _ = events.expect_msg(Duration::from_secs(1)).unwrap();
        membership
            .tell(ClusterMembershipMsg::Join {
                join: Join {
                    node: peer.clone(),
                    roles: Vec::new(),
                },
                reply_to: None,
            })
            .unwrap();
        let _ = events.expect_msg(Duration::from_secs(1)).unwrap();

        membership
            .tell(ClusterMembershipMsg::MarkUnreachable {
                observer: self_node.clone(),
                subject: peer.clone(),
            })
            .unwrap();
        expect_event(&events, |event| {
            matches!(
                event,
                ClusterEvent::Reachability(ReachabilityEvent::Unreachable(member))
                    if member.unique_address == peer
            )
        });

        membership
            .tell(ClusterMembershipMsg::MarkReachable {
                observer: self_node,
                subject: peer.clone(),
            })
            .unwrap();
        expect_event(&events, |event| {
            matches!(
                event,
                ClusterEvent::Reachability(ReachabilityEvent::Reachable(member))
                    if member.unique_address == peer
            )
        });
    }

    #[test]
    fn registered_downing_provider_observes_gossip_and_applies_stable_decision() {
        let (kit, manual_time) =
            ActorSystemTestKit::with_manual_time("cluster-membership-downing-provider").unwrap();
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let (membership, _events) = spawn_membership(&kit, self_node.clone(), "membership");
        let snapshots = kit
            .create_probe::<DowningProviderSnapshot>("downing-snapshots")
            .unwrap();
        let gossip_probe = kit.create_probe::<Gossip>("downing-gossip").unwrap();
        let provider = kit
            .system()
            .spawn(
                "downing-provider",
                DowningProviderActor::props(
                    self_node.clone(),
                    StaticDowningHook::new(DowningDecision::DownUnreachable),
                    membership.clone(),
                    Duration::from_secs(1),
                ),
            )
            .unwrap();

        membership
            .tell(ClusterMembershipMsg::RegisterDowningProvider {
                provider: provider.clone(),
            })
            .unwrap();
        membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
        membership
            .tell(ClusterMembershipMsg::Join {
                join: Join {
                    node: peer.clone(),
                    roles: Vec::new(),
                },
                reply_to: None,
            })
            .unwrap();
        membership
            .tell(ClusterMembershipMsg::MarkUnreachable {
                observer: self_node,
                subject: peer.clone(),
            })
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
                if snapshot.responsible
                    && snapshot.stable_timer_active
                    && snapshot.relevant_unreachable == vec![peer.clone()]
                {
                    Ok(())
                } else {
                    Err(format!("unexpected downing snapshot: {snapshot:?}"))
                }
            },
        )
        .unwrap();

        manual_time.advance(Duration::from_secs(1));

        await_assert(
            Duration::from_secs(1),
            Duration::from_millis(10),
            || -> Result<(), String> {
                membership
                    .tell(ClusterMembershipMsg::SendCurrentGossip {
                        reply_to: gossip_probe.actor_ref(),
                    })
                    .map_err(|error| error.reason().to_string())?;
                let gossip = gossip_probe
                    .expect_msg(Duration::from_millis(100))
                    .map_err(|error| error.to_string())?;
                match gossip.member(&peer).map(|member| member.status) {
                    Some(MemberStatus::Down) => Ok(()),
                    other => Err(format!("expected peer down, got {other:?}")),
                }
            },
        )
        .unwrap();
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    fn spawn_membership(
        kit: &ActorSystemTestKit,
        self_node: UniqueAddress,
        name: &str,
    ) -> (
        ActorRef<ClusterMembershipMsg>,
        kairo_testkit::TestProbe<ClusterEvent>,
    ) {
        let publisher = kit
            .system()
            .spawn(
                format!("{name}-publisher"),
                Props::new({
                    let self_node = self_node.clone();
                    move || ClusterEventPublisher::new(self_node.clone())
                }),
            )
            .unwrap();
        let events = kit
            .create_probe::<ClusterEvent>(format!("{name}-events"))
            .unwrap();
        publisher
            .tell(ClusterEventPublisherMsg::Subscribe {
                subscriber: events.actor_ref(),
                initial_state: SubscriptionInitialState::None,
            })
            .unwrap();
        let membership = kit
            .system()
            .spawn(
                name,
                Props::new(move || {
                    ClusterMembership::new(self_node.clone(), Vec::new(), publisher.clone())
                }),
            )
            .unwrap();
        (membership, events)
    }

    fn member(unique_address: UniqueAddress, status: MemberStatus) -> Member {
        Member::new(unique_address, Vec::new()).with_status(status)
    }

    fn expect_event(
        probe: &kairo_testkit::TestProbe<ClusterEvent>,
        matches: impl Fn(&ClusterEvent) -> bool,
    ) -> ClusterEvent {
        for _ in 0..16 {
            let event = probe.expect_msg(Duration::from_secs(1)).unwrap();
            if matches(&event) {
                return event;
            }
        }
        panic!("expected matching cluster event");
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }
}
