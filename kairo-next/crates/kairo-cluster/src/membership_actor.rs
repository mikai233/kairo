#![deny(missing_docs)]

use std::collections::HashSet;

use kairo_actor::{Actor, ActorRef, ActorResult, Address, Context};

use crate::{
    ClusterEventPublisherMsg, ClusterGossipProcessMsg, ClusterInitJoinLifecycle,
    ClusterInitJoinResponderMsg, CurrentClusterState, DowningDecision, DowningPlan,
    DowningProviderMsg, Gossip, GossipEnvelope, Join, LeaderSelection, Member, ReachabilityStatus,
    UniqueAddress, VectorClockOrdering, Welcome,
};

#[derive(Debug, Clone)]
/// Typed protocol for the actor that owns local cluster membership gossip.
pub enum ClusterMembershipMsg {
    /// Forms a new cluster by joining the local node to itself.
    JoinSelf,
    /// Applies a local or remote membership join request.
    Join {
        /// Joining node incarnation and advertised roles.
        join: Join,
        /// Optional route for the resulting welcome.
        reply_to: Option<ActorRef<ClusterMembershipMsg>>,
    },
    /// Initializes an unjoined node from a seed's current gossip state.
    Welcome(Box<Welcome>),
    /// Applies one full-state gossip exchange.
    Gossip {
        /// Addressed gossip state from the remote node.
        envelope: Box<GossipEnvelope>,
        /// Optional route used when causal negotiation requires talkback.
        reply_to: Option<ActorRef<ClusterMembershipMsg>>,
    },
    /// Records one observer-owned unreachable failure-detector observation.
    MarkUnreachable {
        /// Node incarnation that made the observation.
        observer: UniqueAddress,
        /// Node incarnation observed as unreachable.
        subject: UniqueAddress,
    },
    /// Clears one observer-owned unreachable observation.
    MarkReachable {
        /// Node incarnation that made the observation.
        observer: UniqueAddress,
        /// Node incarnation observed as reachable again.
        subject: UniqueAddress,
    },
    /// Moves an eligible member at a canonical address into `Leaving`.
    Leave {
        /// Canonical member address to leave.
        address: Address,
    },
    /// Marks one exact node incarnation down.
    Down {
        /// Node incarnation to down.
        node: UniqueAddress,
    },
    /// Resolves a canonical address to its current incarnation and downs it.
    DownAddress {
        /// Canonical member address to down.
        address: Address,
    },
    /// Records that an exiting node completed its local handoff work.
    ExitingConfirmed {
        /// Node incarnation that confirmed exit.
        node: UniqueAddress,
    },
    /// Resolves and applies an abstract downing-provider decision.
    ApplyDowningDecision(DowningDecision),
    /// Registers the provider that observes each current gossip view.
    RegisterDowningProvider {
        /// Downing provider actor.
        provider: ActorRef<DowningProviderMsg>,
    },
    /// Registers the responder whose joinability follows local membership state.
    RegisterInitJoinResponder {
        /// Initial seed-contact responder actor.
        responder: ActorRef<ClusterInitJoinResponderMsg>,
    },
    /// Registers the periodic gossip process used for leave acceleration.
    RegisterGossipProcess {
        /// Gossip process actor.
        process: ActorRef<ClusterGossipProcessMsg>,
    },
    /// Runs convergence-gated promotion, exit, and removal actions when leader.
    LeaderActionsTick,
    /// Sends the latest raw gossip state to one recipient.
    SendCurrentGossip {
        /// Recipient of the current gossip value.
        reply_to: ActorRef<Gossip>,
    },
    /// Sends the latest derived public cluster state to one recipient.
    SendCurrentState {
        /// Recipient of the current cluster snapshot.
        reply_to: ActorRef<CurrentClusterState>,
    },
}

/// Actor that owns local gossip and serializes all membership state transitions.
///
/// Local mutations are stamped with the local vector-clock entry and reset the
/// seen table to the local node. Remote gossip is accepted only when addressed
/// to this exact incarnation and sent by a known, locally reachable member.
pub struct ClusterMembership {
    self_node: UniqueAddress,
    roles: Vec<String>,
    app_version: crate::ApplicationVersion,
    gossip: Gossip,
    event_publisher: ActorRef<ClusterEventPublisherMsg>,
    sequence_nr: u64,
    timestamp: u64,
    initialized: bool,
    downing_provider: Option<ActorRef<DowningProviderMsg>>,
    init_join_responder: Option<ActorRef<ClusterInitJoinResponderMsg>>,
    gossip_process: Option<ActorRef<ClusterGossipProcessMsg>>,
    exiting_confirmed: HashSet<UniqueAddress>,
}

impl ClusterMembership {
    /// Creates uninitialized membership state for `self_node` and its roles.
    pub fn new(
        self_node: UniqueAddress,
        roles: Vec<String>,
        event_publisher: ActorRef<ClusterEventPublisherMsg>,
    ) -> Self {
        Self {
            self_node,
            roles,
            app_version: crate::ApplicationVersion::default(),
            gossip: Gossip::new(),
            event_publisher,
            sequence_nr: 0,
            timestamp: 0,
            initialized: false,
            downing_provider: None,
            init_join_responder: None,
            gossip_process: None,
            exiting_confirmed: HashSet::new(),
        }
    }

    /// Sets the application version advertised for the local member.
    pub fn with_app_version(mut self, app_version: crate::ApplicationVersion) -> Self {
        self.app_version = app_version;
        self
    }

    /// Returns the latest locally owned gossip view.
    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }

    fn join_self(&mut self) {
        self.join(
            Join {
                node: self.self_node.clone(),
                roles: self.roles.clone(),
                app_version: self.app_version.clone(),
            },
            None,
        );
    }

    fn join(&mut self, join: Join, reply_to: Option<ActorRef<ClusterMembershipMsg>>) {
        let existing_same_address = self
            .gossip
            .members()
            .iter()
            .find(|member| member.unique_address.address == join.node.address)
            .cloned();

        if let Some(existing) = existing_same_address {
            if existing.unique_address == join.node {
                self.reply_welcome(reply_to);
            } else if existing.status == crate::MemberStatus::Down {
                let removal_timestamp = self.next_timestamp();
                let gossip = self
                    .gossip
                    .remove(&existing.unique_address, removal_timestamp)
                    .add_member(
                        Member::new(join.node.clone(), join.roles)
                            .with_app_version(join.app_version),
                    );
                self.update_latest_gossip(gossip);
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

        let mut gossip = self.gossip.add_member(
            Member::new(join.node.clone(), join.roles).with_app_version(join.app_version),
        );
        if !gossip.has_member(&self.self_node) {
            gossip = gossip.add_member(
                Member::new(self.self_node.clone(), self.roles.clone())
                    .with_app_version(self.app_version.clone()),
            );
        }
        self.initialized = true;
        self.update_latest_gossip(gossip);

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

    fn leave(&mut self, address: &Address) {
        let Some(member) = self
            .gossip
            .members()
            .iter()
            .find(|member| &member.unique_address.address == address)
            .cloned()
        else {
            return;
        };
        if matches!(
            member.status,
            crate::MemberStatus::Joining | crate::MemberStatus::WeaklyUp | crate::MemberStatus::Up
        ) {
            self.update_latest_gossip(
                self.gossip
                    .update_members([member.with_status(crate::MemberStatus::Leaving)]),
            );
            if let Some(process) = &self.gossip_process {
                let _ = process.tell(ClusterGossipProcessMsg::Tick);
            }
        }
    }

    fn down_address(&mut self, address: &Address) {
        if let Some(node) = self
            .gossip
            .members()
            .iter()
            .find(|member| &member.unique_address.address == address)
            .map(|member| member.unique_address.clone())
        {
            self.down(&node);
        }
    }

    fn exiting_confirmed(&mut self, node: UniqueAddress) {
        if self.gossip.has_member(&node) {
            self.exiting_confirmed.insert(node);
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

    fn register_init_join_responder(&mut self, responder: ActorRef<ClusterInitJoinResponderMsg>) {
        self.init_join_responder = Some(responder);
        self.publish_init_join_lifecycle();
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
            self.exiting_confirmed.iter().cloned(),
        ) {
            let changed = outcome.changed();
            if changed {
                self.gossip = outcome.gossip;
            }
            self.exiting_confirmed
                .retain(|node| self.gossip.has_member(node));
            if changed {
                self.publish_current_gossip();
            }
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
        self.publish_init_join_lifecycle();
    }

    fn publish_init_join_lifecycle(&self) {
        let Some(responder) = &self.init_join_responder else {
            return;
        };
        let lifecycle = if self.initialized {
            self.gossip
                .member(&self.self_node)
                .map(|member| ClusterInitJoinLifecycle::Initialized {
                    self_status: member.status,
                })
                .unwrap_or(ClusterInitJoinLifecycle::Uninitialized)
        } else {
            ClusterInitJoinLifecycle::Uninitialized
        };
        let _ = responder.tell(ClusterInitJoinResponderMsg::SetLifecycle(lifecycle));
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
            ClusterMembershipMsg::Leave { address } => self.leave(&address),
            ClusterMembershipMsg::Down { node } => self.down(&node),
            ClusterMembershipMsg::DownAddress { address } => self.down_address(&address),
            ClusterMembershipMsg::ExitingConfirmed { node } => self.exiting_confirmed(node),
            ClusterMembershipMsg::ApplyDowningDecision(decision) => {
                self.apply_downing_decision(decision);
            }
            ClusterMembershipMsg::RegisterDowningProvider { provider } => {
                self.register_downing_provider(provider);
            }
            ClusterMembershipMsg::RegisterInitJoinResponder { responder } => {
                self.register_init_join_responder(responder);
            }
            ClusterMembershipMsg::RegisterGossipProcess { process } => {
                self.gossip_process = Some(process);
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
mod tests;
