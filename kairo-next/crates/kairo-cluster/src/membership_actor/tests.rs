use std::time::Duration;

use kairo_actor::{ActorRef, Address, Props};
use kairo_testkit::{ActorSystemTestKit, await_assert};

use super::*;
use crate::{
    ClusterEvent, ClusterEventPublisher, DowningProviderActor, DowningProviderSnapshot,
    MemberEvent, MemberStatus, ReachabilityEvent, ReachabilityStatus, StaticDowningHook,
    SubscriptionInitialState,
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
fn membership_feeds_seed_responder_from_uninitialized_through_up_and_down() {
    let kit = ActorSystemTestKit::new("cluster-membership-seed-responder").unwrap();
    let self_node = node("self", 1);
    let (membership, _events) = spawn_membership(&kit, self_node.clone(), "membership");
    let responder = kit
        .create_probe::<ClusterInitJoinResponderMsg>("init-join-responder")
        .unwrap();

    membership
        .tell(ClusterMembershipMsg::RegisterInitJoinResponder {
            responder: responder.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        responder.expect_msg(Duration::from_secs(1)).unwrap(),
        ClusterInitJoinResponderMsg::SetLifecycle(ClusterInitJoinLifecycle::Uninitialized)
    ));

    membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
    assert!(matches!(
        responder.expect_msg(Duration::from_secs(1)).unwrap(),
        ClusterInitJoinResponderMsg::SetLifecycle(ClusterInitJoinLifecycle::Initialized {
            self_status: MemberStatus::Joining
        })
    ));
    assert!(matches!(
        responder.expect_msg(Duration::from_secs(1)).unwrap(),
        ClusterInitJoinResponderMsg::SetLifecycle(ClusterInitJoinLifecycle::Initialized {
            self_status: MemberStatus::Up
        })
    ));

    membership
        .tell(ClusterMembershipMsg::Down { node: self_node })
        .unwrap();
    assert!(matches!(
        responder.expect_msg(Duration::from_secs(1)).unwrap(),
        ClusterInitJoinResponderMsg::SetLifecycle(ClusterInitJoinLifecycle::Initialized {
            self_status: MemberStatus::Down
        })
    ));
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
fn retried_join_from_existing_member_replies_with_current_welcome() {
    let kit = ActorSystemTestKit::new("cluster-membership-retried-join").unwrap();
    let self_node = node("self", 1);
    let joining = node("joining", 2);
    let (membership, events) = spawn_membership(&kit, self_node.clone(), "membership");
    let reply_probe = kit
        .create_probe::<ClusterMembershipMsg>("retry-welcome")
        .unwrap();

    membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
    expect_event(
        &events,
        |event| matches!(event, ClusterEvent::Member(MemberEvent::Joined(member)) if member.unique_address == self_node),
    );
    expect_event(
        &events,
        |event| matches!(event, ClusterEvent::Member(MemberEvent::Up(member)) if member.unique_address == self_node),
    );

    membership
        .tell(ClusterMembershipMsg::Join {
            join: Join {
                node: joining.clone(),
                roles: vec!["backend".to_string()],
            },
            reply_to: None,
        })
        .unwrap();
    expect_event(
        &events,
        |event| matches!(event, ClusterEvent::Member(MemberEvent::Joined(member)) if member.unique_address == joining),
    );
    drain_events(&events);

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
        panic!("expected welcome for retried join");
    };
    assert_eq!(welcome.from, self_node);
    assert_eq!(
        welcome.gossip.member(&joining).map(|member| member.status),
        Some(MemberStatus::Joining)
    );
    assert_eq!(
        welcome
            .gossip
            .members()
            .iter()
            .filter(|member| member.unique_address == joining)
            .count(),
        1
    );
    events.expect_no_msg(Duration::from_millis(50)).unwrap();
}

#[test]
fn new_incarnation_join_downs_existing_same_address_without_welcome() {
    let kit = ActorSystemTestKit::new("cluster-membership-new-incarnation").unwrap();
    let self_node = node("self", 1);
    let old_peer = node("peer", 2);
    let new_peer = node("peer", 3);
    let (membership, events) = spawn_membership(&kit, self_node.clone(), "membership");
    let reply_probe = kit
        .create_probe::<ClusterMembershipMsg>("new-welcome")
        .unwrap();
    let gossip_probe = kit
        .create_probe::<Gossip>("new-incarnation-gossip")
        .unwrap();

    membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
    membership
        .tell(ClusterMembershipMsg::Join {
            join: Join {
                node: old_peer.clone(),
                roles: vec!["backend".to_string()],
            },
            reply_to: None,
        })
        .unwrap();
    membership
        .tell(ClusterMembershipMsg::Join {
            join: Join {
                node: new_peer.clone(),
                roles: vec!["backend".to_string()],
            },
            reply_to: Some(reply_probe.actor_ref()),
        })
        .unwrap();
    membership
        .tell(ClusterMembershipMsg::SendCurrentGossip {
            reply_to: gossip_probe.actor_ref(),
        })
        .unwrap();

    let gossip = gossip_probe.expect_msg(Duration::from_secs(1)).unwrap();
    assert_eq!(
        gossip.member(&old_peer).map(|member| member.status),
        Some(MemberStatus::Down)
    );
    assert!(!gossip.has_member(&new_peer));
    assert_eq!(
        gossip.reachability().status(&self_node, &old_peer),
        ReachabilityStatus::Terminated
    );
    reply_probe
        .expect_no_msg(Duration::from_millis(50))
        .unwrap();
    expect_event(&events, |event| {
        matches!(
            event,
            ClusterEvent::Member(MemberEvent::Downed(member))
                if member.unique_address == old_peer
        )
    });
}

#[test]
fn new_incarnation_retry_after_downing_rejoins_same_address() {
    let kit = ActorSystemTestKit::new("cluster-membership-new-incarnation-retry").unwrap();
    let self_node = node("self", 1);
    let old_peer = node("peer", 2);
    let new_peer = node("peer", 3);
    let (membership, events) = spawn_membership(&kit, self_node.clone(), "membership");
    let reply_probe = kit
        .create_probe::<ClusterMembershipMsg>("retry-welcome")
        .unwrap();
    let gossip_probe = kit
        .create_probe::<Gossip>("new-incarnation-retry-gossip")
        .unwrap();

    membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
    membership
        .tell(ClusterMembershipMsg::Join {
            join: Join {
                node: old_peer.clone(),
                roles: vec!["backend".to_string()],
            },
            reply_to: None,
        })
        .unwrap();
    membership
        .tell(ClusterMembershipMsg::Join {
            join: Join {
                node: new_peer.clone(),
                roles: vec!["backend".to_string()],
            },
            reply_to: None,
        })
        .unwrap();
    expect_event(&events, |event| {
        matches!(
            event,
            ClusterEvent::Member(MemberEvent::Downed(member))
                if member.unique_address == old_peer
        )
    });

    membership
        .tell(ClusterMembershipMsg::Join {
            join: Join {
                node: new_peer.clone(),
                roles: vec!["backend".to_string()],
            },
            reply_to: Some(reply_probe.actor_ref()),
        })
        .unwrap();
    membership
        .tell(ClusterMembershipMsg::SendCurrentGossip {
            reply_to: gossip_probe.actor_ref(),
        })
        .unwrap();

    let ClusterMembershipMsg::Welcome(welcome) =
        reply_probe.expect_msg(Duration::from_secs(1)).unwrap()
    else {
        panic!("expected welcome for retried incarnation");
    };
    assert_eq!(welcome.from, self_node);
    assert!(!welcome.gossip.has_member(&old_peer));
    assert_eq!(
        welcome.gossip.member(&new_peer).map(|member| member.status),
        Some(MemberStatus::Joining)
    );

    let gossip = gossip_probe.expect_msg(Duration::from_secs(1)).unwrap();
    assert!(!gossip.has_member(&old_peer));
    assert_eq!(
        gossip.member(&new_peer).map(|member| member.status),
        Some(MemberStatus::Joining)
    );
    assert_eq!(
        gossip.reachability().status(&self_node, &old_peer),
        ReachabilityStatus::Reachable
    );
    expect_event(&events, |event| {
        matches!(
            event,
            ClusterEvent::Member(MemberEvent::Removed {
                member,
                previous_status: MemberStatus::Down,
            }) if member.unique_address == old_peer
        )
    });
    expect_event(&events, |event| {
        matches!(
            event,
            ClusterEvent::Member(MemberEvent::Joined(member))
                if member.unique_address == new_peer
        )
    });
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
fn leave_address_moves_member_to_leaving_idempotently() {
    let kit = ActorSystemTestKit::new("cluster-membership-leave").unwrap();
    let self_node = node("self", 1);
    let peer = node("peer", 2);
    let (membership, events) = spawn_membership(&kit, self_node, "membership");
    let gossip_probe = kit.create_probe::<Gossip>("leave-gossip").unwrap();

    membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
    drain_events(&events);
    membership
        .tell(ClusterMembershipMsg::Join {
            join: Join {
                node: peer.clone(),
                roles: Vec::new(),
            },
            reply_to: None,
        })
        .unwrap();
    drain_events(&events);

    membership
        .tell(ClusterMembershipMsg::Leave {
            address: peer.address.clone(),
        })
        .unwrap();
    membership
        .tell(ClusterMembershipMsg::SendCurrentGossip {
            reply_to: gossip_probe.actor_ref(),
        })
        .unwrap();

    assert_eq!(
        gossip_probe
            .expect_msg(Duration::from_secs(1))
            .unwrap()
            .member(&peer)
            .map(|member| member.status),
        Some(MemberStatus::Leaving)
    );
    expect_event(&events, |event| {
        matches!(
            event,
            ClusterEvent::Member(MemberEvent::Left(member))
                if member.unique_address == peer
        )
    });
    drain_events(&events);
    membership
        .tell(ClusterMembershipMsg::Leave {
            address: peer.address.clone(),
        })
        .unwrap();
    events.expect_no_msg(Duration::from_millis(50)).unwrap();
}

#[test]
fn leader_removes_confirmed_exiting_member() {
    let kit = ActorSystemTestKit::new("cluster-membership-exiting-confirmed").unwrap();
    let self_node = node("a", 1);
    let peer = node("b", 2);
    let (membership, events) = spawn_membership(&kit, self_node.clone(), "membership");
    let gossip_probe = kit.create_probe::<Gossip>("confirmed-gossip").unwrap();
    let converged = Gossip::from_members([
        member(self_node.clone(), MemberStatus::Up),
        member(peer.clone(), MemberStatus::Exiting),
    ])
    .seen(self_node.clone())
    .seen(peer.clone())
    .increment_version(&peer);

    membership
        .tell(ClusterMembershipMsg::Welcome(Box::new(Welcome {
            from: peer.clone(),
            gossip: converged,
        })))
        .unwrap();
    drain_events(&events);
    membership
        .tell(ClusterMembershipMsg::ExitingConfirmed { node: peer.clone() })
        .unwrap();
    membership
        .tell(ClusterMembershipMsg::LeaderActionsTick)
        .unwrap();
    membership
        .tell(ClusterMembershipMsg::SendCurrentGossip {
            reply_to: gossip_probe.actor_ref(),
        })
        .unwrap();

    let gossip = gossip_probe.expect_msg(Duration::from_secs(1)).unwrap();
    assert!(!gossip.has_member(&peer));
    assert!(gossip.tombstones().contains_key(&peer));
    expect_event(&events, |event| {
        matches!(
            event,
            ClusterEvent::Member(MemberEvent::Removed {
                member,
                previous_status: MemberStatus::Exiting,
            }) if member.unique_address == peer
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

fn drain_events(probe: &kairo_testkit::TestProbe<ClusterEvent>) {
    while probe.expect_msg(Duration::from_millis(20)).is_ok() {}
}

fn node(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(Address::local(system), uid)
}
