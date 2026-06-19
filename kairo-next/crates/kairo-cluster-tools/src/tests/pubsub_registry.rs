use super::*;

#[test]
fn pubsub_registry_collects_and_merges_versioned_topic_deltas() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let orders = TopicName::new("orders");
    let jobs = TopicName::new("jobs");
    let mut source = PubSubRegistryState::new(node_a.clone());
    let mut target = PubSubRegistryState::new(node_b.clone());

    source.register_local_topic(orders.clone());
    let initial_delta = source.collect_delta(&target.versions(), 10);
    source.unregister_local_topic(orders.clone());
    source.register_local_group(jobs.clone(), "workers");
    target.merge_delta(source.collect_delta(&target.versions(), 10));

    assert!(target.broadcast_targets(&orders, true).is_empty());
    assert_eq!(target.broadcast_targets(&jobs, false), vec![node_a.clone()]);
    assert_eq!(
        target.one_per_group_targets(&jobs).get("workers"),
        Some(&node_a)
    );

    target.merge_delta(initial_delta);
    assert!(target.broadcast_targets(&orders, true).is_empty());
}

#[test]
fn pubsub_registry_collect_delta_respects_peer_versions_and_entry_limit() {
    let node_a = node("a", 1);
    let topic = TopicName::new("jobs");
    let mut registry = PubSubRegistryState::new(node_a.clone());
    registry.register_local_group(topic.clone(), "red");
    registry.register_local_group(topic.clone(), "blue");

    let limited = registry.collect_delta(&BTreeMap::new(), 1);
    assert_eq!(limited.buckets.len(), 1);
    assert_eq!(limited.buckets[0].entries.len(), 1);

    let full = registry.collect_delta(&BTreeMap::new(), 10);
    let peer_versions = BTreeMap::from([(node_a.ordering_key(), full.buckets[0].version)]);
    assert!(
        registry
            .collect_delta(&peer_versions, 10)
            .buckets
            .is_empty()
    );
}

#[test]
fn pubsub_registry_limited_delta_sends_lowest_versions_first() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let mut source = PubSubRegistryState::new(node_a.clone());
    let mut target = PubSubRegistryState::new(node_b);

    source.register_local_path("z-first");
    source.register_local_path("a-second");

    let first_delta = source.collect_delta(&target.versions(), 1);
    assert_eq!(first_delta.buckets.len(), 1);
    assert_eq!(first_delta.buckets[0].version, 1);
    assert!(
        first_delta.buckets[0]
            .entries
            .contains_key(&PubSubRegistryKey::path("z-first"))
    );
    target.merge_delta(first_delta);
    assert_eq!(target.path_targets("z-first", true), vec![node_a.clone()]);
    assert!(target.path_targets("a-second", true).is_empty());

    let second_delta = source.collect_delta(&target.versions(), 1);
    assert_eq!(second_delta.buckets.len(), 1);
    assert_eq!(second_delta.buckets[0].version, 2);
    assert!(
        second_delta.buckets[0]
            .entries
            .contains_key(&PubSubRegistryKey::path("a-second"))
    );
    target.merge_delta(second_delta);
    assert_eq!(target.path_targets("z-first", true), vec![node_a.clone()]);
    assert_eq!(target.path_targets("a-second", true), vec![node_a]);
}

#[test]
fn pubsub_registry_plans_one_remote_target_per_group_deterministically() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let topic = TopicName::new("jobs");
    let mut node_a_registry = PubSubRegistryState::new(node_a.clone());
    let mut node_b_registry = PubSubRegistryState::new(node_b.clone());
    let mut merged = PubSubRegistryState::new(node_c);

    node_a_registry.register_local_group(topic.clone(), "workers");
    node_b_registry.register_local_group(topic.clone(), "workers");
    merged.merge_delta(node_b_registry.collect_delta(&BTreeMap::new(), 10));
    merged.merge_delta(node_a_registry.collect_delta(&BTreeMap::new(), 10));

    assert_eq!(
        merged.one_per_group_targets(&topic),
        BTreeMap::from([("workers".to_string(), node_a)])
    );
}

#[test]
fn pubsub_registry_prunes_old_tombstones_without_dropping_present_entries() {
    let node_a = node("a", 1);
    let orders = TopicName::new("orders");
    let jobs = TopicName::new("jobs");
    let mut registry = PubSubRegistryState::new(node_a);

    registry.register_local_topic(orders.clone());
    registry.unregister_local_topic(orders.clone());
    registry.register_local_topic(jobs.clone());
    registry.prune_tombstones_older_than(0);

    let bucket = registry.bucket(registry.self_node()).unwrap();
    assert!(
        !bucket
            .entries
            .contains_key(&PubSubRegistryKey::topic(orders))
    );
    assert!(bucket.entries.contains_key(&PubSubRegistryKey::topic(jobs)));
}

#[test]
fn pubsub_registry_ignores_same_address_replacement_self_delta() {
    let self_node = node("self", 1);
    let replacement_self = UniqueAddress::new(self_node.address.clone(), 2);
    let local_topic = TopicName::new("local");
    let stale_topic = TopicName::new("stale");
    let mut registry = PubSubRegistryState::new(self_node.clone());
    let mut replacement_registry = PubSubRegistryState::new(replacement_self);

    registry.register_local_topic(local_topic.clone());
    replacement_registry.register_local_topic(stale_topic.clone());
    registry.merge_delta(replacement_registry.collect_delta(&BTreeMap::new(), 10));

    assert_eq!(
        registry.broadcast_targets(&local_topic, true),
        vec![self_node]
    );
    assert!(registry.broadcast_targets(&stale_topic, true).is_empty());
}
