use std::collections::HashSet;

use super::{
    DowningDecision, SplitBrainStrategy, downable_nodes, keep_majority_decision,
    keep_oldest_decision,
};
use crate::{Gossip, Reachability, ReachabilityStatus, UniqueAddress};

pub(super) fn decision(gossip: &Gossip, strategy: &SplitBrainStrategy) -> DowningDecision {
    match strategy {
        SplitBrainStrategy::DownAll => DowningDecision::DownAll,
        SplitBrainStrategy::KeepMajority { role } => {
            if has_indirectly_connected(gossip) {
                DowningDecision::DownIndirectlyConnected
            } else {
                keep_majority_decision(gossip, role)
            }
        }
        SplitBrainStrategy::KeepOldest {
            role,
            down_if_alone,
        } => {
            if has_indirectly_connected(gossip) {
                DowningDecision::DownIndirectlyConnected
            } else {
                keep_oldest_decision(gossip, role, *down_if_alone)
            }
        }
    }
}

pub(super) fn indirectly_connected_nodes_to_down(
    gossip: &Gossip,
    strategy: &SplitBrainStrategy,
) -> HashSet<UniqueAddress> {
    let downable: HashSet<_> = downable_nodes(gossip).into_iter().collect();
    let indirectly_connected = indirectly_connected_nodes(gossip);
    let additional = additional_nodes_to_down_when_indirectly_connected(
        gossip,
        strategy,
        &downable,
        &indirectly_connected,
    );
    let mut targets = indirectly_connected;
    targets.extend(additional);

    downable.intersection(&targets).cloned().collect()
}

pub(super) fn nodes_to_down_for_decision(
    decision: DowningDecision,
    gossip: &Gossip,
) -> Vec<UniqueAddress> {
    match decision {
        DowningDecision::NoAction => Vec::new(),
        DowningDecision::DownUnreachable => {
            let unreachable = gossip.reachability().all_unreachable_or_terminated();
            downable_nodes(gossip)
                .into_iter()
                .filter(|node| unreachable.contains(node))
                .collect()
        }
        DowningDecision::DownReachable => {
            let unreachable = gossip.reachability().all_unreachable_or_terminated();
            downable_nodes(gossip)
                .into_iter()
                .filter(|node| !unreachable.contains(node))
                .collect()
        }
        DowningDecision::DownAll => downable_nodes(gossip),
        DowningDecision::DownIndirectlyConnected => {
            let downable: HashSet<_> = downable_nodes(gossip).into_iter().collect();
            indirectly_connected_nodes(gossip)
                .intersection(&downable)
                .cloned()
                .collect()
        }
        DowningDecision::ReverseDownIndirectlyConnected => {
            let unreachable = gossip.reachability().all_unreachable_or_terminated();
            let indirectly_connected = indirectly_connected_nodes(gossip);
            downable_nodes(gossip)
                .into_iter()
                .filter(|node| indirectly_connected.contains(node) || !unreachable.contains(node))
                .collect()
        }
        DowningDecision::DownSelfQuarantinedByRemote => Vec::new(),
    }
}

fn additional_nodes_to_down_when_indirectly_connected(
    gossip: &Gossip,
    strategy: &SplitBrainStrategy,
    downable: &HashSet<UniqueAddress>,
    indirectly_connected: &HashSet<UniqueAddress>,
) -> HashSet<UniqueAddress> {
    let unreachable = gossip.reachability().all_unreachable_or_terminated();
    if unreachable
        .difference(indirectly_connected)
        .next()
        .is_none()
    {
        return HashSet::new();
    }

    let observer_subject_intersection =
        indirectly_connected_from_observer_subject_intersection(gossip);
    let seen_current_gossip = indirectly_connected_from_seen_current_gossip(gossip);
    let filtered_records = gossip.reachability().records().iter().filter(|record| {
        downable.contains(&record.observer)
            && downable.contains(&record.subject)
            && matches!(
                record.status,
                ReachabilityStatus::Unreachable | ReachabilityStatus::Terminated
            )
            && !(observer_subject_intersection.contains(&record.observer)
                && observer_subject_intersection.contains(&record.subject)
                || seen_current_gossip.contains(&record.observer)
                    && seen_current_gossip.contains(&record.subject))
    });
    let filtered_reachability = Reachability::from_parts(
        filtered_records.cloned(),
        gossip
            .reachability()
            .versions()
            .iter()
            .filter(|(observer, _)| downable.contains(observer))
            .map(|(observer, version)| (observer.clone(), *version)),
    );
    let filtered_gossip = gossip.with_reachability(filtered_reachability);
    let additional_decision = decision_without_indirect_check(&filtered_gossip, strategy);

    if additional_decision == DowningDecision::DownIndirectlyConnected {
        return downable.clone();
    }

    nodes_to_down_for_decision(additional_decision, &filtered_gossip)
        .into_iter()
        .collect()
}

fn decision_without_indirect_check(
    gossip: &Gossip,
    strategy: &SplitBrainStrategy,
) -> DowningDecision {
    match strategy {
        SplitBrainStrategy::DownAll => DowningDecision::DownAll,
        SplitBrainStrategy::KeepMajority { role } => keep_majority_decision(gossip, role),
        SplitBrainStrategy::KeepOldest {
            role,
            down_if_alone,
        } => keep_oldest_decision(gossip, role, *down_if_alone),
    }
}

pub(super) fn has_indirectly_connected(gossip: &Gossip) -> bool {
    !indirectly_connected_nodes(gossip).is_empty()
}

fn indirectly_connected_nodes(gossip: &Gossip) -> HashSet<UniqueAddress> {
    let mut nodes = indirectly_connected_from_observer_subject_intersection(gossip);
    nodes.extend(indirectly_connected_from_seen_current_gossip(gossip));
    nodes
}

fn indirectly_connected_from_observer_subject_intersection(
    gossip: &Gossip,
) -> HashSet<UniqueAddress> {
    let observers = gossip.reachability().all_observers();
    let unreachable = gossip.reachability().all_unreachable_or_terminated();
    observers.intersection(&unreachable).cloned().collect()
}

fn indirectly_connected_from_seen_current_gossip(gossip: &Gossip) -> HashSet<UniqueAddress> {
    let mut nodes = HashSet::new();
    for record in gossip.reachability().records() {
        if !matches!(
            record.status,
            ReachabilityStatus::Unreachable | ReachabilityStatus::Terminated
        ) {
            continue;
        }
        if gossip.seen_by().contains(&record.subject) {
            nodes.insert(record.observer.clone());
            nodes.insert(record.subject.clone());
        }
    }
    nodes
}
