use std::collections::HashSet;

use kairo_actor::Address;

use super::*;
use crate::{Member, Reachability};

#[test]
fn no_downing_hook_produces_no_action_plan() {
    let node_a = node("a", 1);
    let gossip = Gossip::from_members([member(node_a.clone(), MemberStatus::Up)]);

    let plan = DowningPlan::from_hook(&NoDowning, &gossip, &node_a);

    assert_eq!(plan.decision(), DowningDecision::NoAction);
    assert!(plan.nodes_to_down().is_empty());
    assert!(!plan.down_self());
}

#[test]
fn down_unreachable_targets_only_unreachable_downable_members() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let gossip = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Up),
        member(node_c.clone(), MemberStatus::Exiting),
    ])
    .with_reachability(
        Reachability::new()
            .unreachable(node_a.clone(), node_b.clone())
            .unreachable(node_a.clone(), node_c),
    );

    let plan = DowningPlan::from_decision(DowningDecision::DownUnreachable, &gossip, &node_a);

    assert_eq!(plan.nodes_to_down(), &[node_b]);
    assert!(!plan.down_self());
}

#[test]
fn down_reachable_targets_reachable_downable_members() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let gossip = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Up),
        member(node_c.clone(), MemberStatus::Joining),
    ])
    .with_reachability(Reachability::new().unreachable(node_a.clone(), node_b));

    let plan = DowningPlan::from_decision(DowningDecision::DownReachable, &gossip, &node_a);

    assert_eq!(plan.nodes_to_down(), &[node_a.clone(), node_c]);
    assert!(plan.down_self());
}

#[test]
fn down_all_excludes_members_already_down_or_exiting() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let gossip = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b, MemberStatus::Down),
        member(node_c, MemberStatus::Exiting),
    ]);

    let plan = DowningPlan::from_decision(DowningDecision::DownAll, &gossip, &node_a);

    assert_eq!(plan.nodes_to_down(), &[node_a]);
}

#[test]
fn down_self_quarantined_by_remote_targets_only_self() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let gossip = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b, MemberStatus::Up),
    ]);

    let plan = DowningPlan::from_decision(
        DowningDecision::DownSelfQuarantinedByRemote,
        &gossip,
        &node_a,
    );

    assert_eq!(plan.nodes_to_down(), &[node_a]);
    assert!(plan.down_self());
}

#[test]
fn applying_plan_marks_nodes_down_resets_seen_and_stamps_version() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let gossip = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Up),
    ])
    .seen(node_b.clone());
    let plan = DowningPlan::from_decision(
        DowningDecision::DownUnreachable,
        &gossip.with_reachability(Reachability::new().unreachable(node_a.clone(), node_b.clone())),
        &node_a,
    );

    let changed = plan.apply_to(&gossip, &node_a);

    assert_eq!(
        changed.member(&node_b).map(|member| member.status),
        Some(MemberStatus::Down)
    );
    assert_eq!(changed.seen_by(), &HashSet::from([node_a.clone()]));
    assert_eq!(
        changed.version().get(&crate::VectorClockNode::new(format!(
            "{}-{}",
            node_a.address, node_a.uid
        ))),
        1
    );
}

#[test]
fn split_brain_down_all_strategy_downs_all_downable_members() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let hook = SplitBrainResolverHook::down_all();
    let gossip = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Joining),
    ]);

    let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

    assert_eq!(plan.decision(), DowningDecision::DownAll);
    assert_eq!(plan.nodes_to_down(), &[node_a, node_b]);
}

#[test]
fn keep_majority_downs_unreachable_when_self_side_is_larger() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let gossip = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Up),
        member(node_c.clone(), MemberStatus::Up),
    ])
    .with_reachability(Reachability::new().unreachable(node_a.clone(), node_c.clone()));
    let hook = SplitBrainResolverHook::keep_majority(None);

    let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

    assert_eq!(plan.decision(), DowningDecision::DownUnreachable);
    assert_eq!(plan.nodes_to_down(), &[node_c]);
}

#[test]
fn keep_majority_downs_reachable_on_minority_side() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let gossip = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Up),
        member(node_c.clone(), MemberStatus::Up),
    ])
    .with_reachability(
        Reachability::new()
            .unreachable(node_a.clone(), node_b)
            .unreachable(node_a.clone(), node_c),
    );
    let hook = SplitBrainResolverHook::keep_majority(None);

    let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

    assert_eq!(plan.decision(), DowningDecision::DownReachable);
    assert_eq!(plan.nodes_to_down(), &[node_a]);
    assert!(plan.down_self());
}

#[test]
fn keep_majority_tie_keeps_lowest_address_side() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let gossip = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Up),
    ])
    .with_reachability(Reachability::new().unreachable(node_b.clone(), node_a.clone()));
    let hook = SplitBrainResolverHook::keep_majority(None);

    let plan = DowningPlan::from_hook(&hook, &gossip, &node_b);

    assert_eq!(plan.decision(), DowningDecision::DownReachable);
    assert_eq!(plan.nodes_to_down(), &[node_b]);
}

#[test]
fn keep_majority_role_counts_only_members_with_role() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let gossip = Gossip::from_members([
        member_with_roles(node_a.clone(), MemberStatus::Up, ["backend"]),
        member_with_roles(node_b.clone(), MemberStatus::Up, ["frontend"]),
        member_with_roles(node_c.clone(), MemberStatus::Up, ["backend"]),
    ])
    .with_reachability(Reachability::new().unreachable(node_a.clone(), node_c.clone()));
    let hook = SplitBrainResolverHook::keep_majority(Some("backend".to_string()));

    let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

    assert_eq!(plan.decision(), DowningDecision::DownUnreachable);
    assert_eq!(plan.nodes_to_down(), &[node_c]);
}

#[test]
fn keep_majority_with_no_matching_role_downs_all_downable_members() {
    let node_a = node("a", 1);
    let gossip = Gossip::from_members([member(node_a.clone(), MemberStatus::Up)]);
    let hook = SplitBrainResolverHook::keep_majority(Some("missing".to_string()));

    let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

    assert_eq!(plan.decision(), DowningDecision::DownAll);
    assert_eq!(plan.nodes_to_down(), &[node_a]);
}

#[test]
fn keep_majority_downs_indirectly_connected_cycle() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let gossip = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Up),
        member(node_c, MemberStatus::Up),
    ])
    .with_reachability(
        Reachability::new()
            .unreachable(node_a.clone(), node_b.clone())
            .unreachable(node_b.clone(), node_a.clone()),
    );
    let hook = SplitBrainResolverHook::keep_majority(None);

    let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

    assert_eq!(plan.decision(), DowningDecision::DownIndirectlyConnected);
    assert_eq!(plan.nodes_to_down(), &[node_a, node_b]);
    assert!(plan.down_self());
}

#[test]
fn keep_majority_combines_indirect_cycle_with_clean_partition_decision() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let node_d = node("d", 4);
    let gossip = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Up),
        member(node_c, MemberStatus::Up),
        member(node_d.clone(), MemberStatus::Up),
    ])
    .with_reachability(
        Reachability::new()
            .unreachable(node_a.clone(), node_b.clone())
            .unreachable(node_b.clone(), node_a.clone())
            .unreachable(node_a.clone(), node_d.clone()),
    );
    let hook = SplitBrainResolverHook::keep_majority(None);

    let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

    assert_eq!(plan.decision(), DowningDecision::DownIndirectlyConnected);
    assert_eq!(plan.nodes_to_down(), &[node_a, node_b, node_d]);
}

#[test]
fn keep_oldest_treats_seen_unreachable_node_as_indirectly_connected() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let gossip = Gossip::from_members([
        member_with_age(node_a.clone(), MemberStatus::Up, 1),
        member_with_age(node_b.clone(), MemberStatus::Up, 2),
        member_with_age(node_c, MemberStatus::Up, 3),
    ])
    .with_reachability(Reachability::new().unreachable(node_a.clone(), node_b.clone()))
    .seen(node_b.clone());
    let hook = SplitBrainResolverHook::keep_oldest(None, false);

    let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

    assert_eq!(plan.decision(), DowningDecision::DownIndirectlyConnected);
    assert_eq!(plan.nodes_to_down(), &[node_a, node_b]);
    assert!(plan.down_self());
}

#[test]
fn keep_oldest_keeps_side_with_oldest_member() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let gossip = Gossip::from_members([
        member_with_age(node_a.clone(), MemberStatus::Up, 1),
        member_with_age(node_b.clone(), MemberStatus::Up, 2),
        member_with_age(node_c.clone(), MemberStatus::Up, 3),
    ])
    .with_reachability(
        Reachability::new()
            .unreachable(node_b.clone(), node_a.clone())
            .unreachable(node_b.clone(), node_c.clone()),
    );
    let hook = SplitBrainResolverHook::keep_oldest(None, false);

    let plan = DowningPlan::from_hook(&hook, &gossip, &node_b);

    assert_eq!(plan.decision(), DowningDecision::DownReachable);
    assert_eq!(plan.nodes_to_down(), &[node_b]);
}

#[test]
fn keep_oldest_down_if_alone_downs_oldest_when_it_is_alone_against_larger_side() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let gossip = Gossip::from_members([
        member_with_age(node_a.clone(), MemberStatus::Up, 1),
        member_with_age(node_b.clone(), MemberStatus::Up, 2),
        member_with_age(node_c.clone(), MemberStatus::Up, 3),
    ])
    .with_reachability(
        Reachability::new()
            .unreachable(node_a.clone(), node_b)
            .unreachable(node_a.clone(), node_c),
    );
    let hook = SplitBrainResolverHook::keep_oldest(None, true);

    let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

    assert_eq!(plan.decision(), DowningDecision::DownReachable);
    assert_eq!(plan.nodes_to_down(), &[node_a]);
}

fn member(unique_address: UniqueAddress, status: MemberStatus) -> Member {
    Member::new(unique_address, Vec::new()).with_status(status)
}

fn member_with_roles(
    unique_address: UniqueAddress,
    status: MemberStatus,
    roles: impl IntoIterator<Item = &'static str>,
) -> Member {
    Member::new(
        unique_address,
        roles.into_iter().map(String::from).collect(),
    )
    .with_status(status)
}

fn member_with_age(unique_address: UniqueAddress, status: MemberStatus, up_number: u64) -> Member {
    Member::new(unique_address, Vec::new())
        .with_status(status)
        .with_up_number(up_number)
}

fn node(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(Address::local(system), uid)
}
