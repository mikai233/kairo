use super::*;

#[test]
fn shard_allocations_track_single_region_owner_per_shard() {
    let mut allocations =
        ShardAllocations::from_regions(["region-a".to_string(), "region-b".to_string()]);
    let region_a = "region-a".to_string();
    let region_b = "region-b".to_string();
    let shard = "shard-1".to_string();

    assert!(
        allocations
            .allocate_shard(&region_a, shard.clone())
            .unwrap()
    );
    assert!(
        !allocations
            .allocate_shard(&region_a, shard.clone())
            .unwrap()
    );
    assert_eq!(allocations.region_for_shard(&shard), Some(&region_a));

    assert!(
        allocations
            .allocate_shard(&region_b, shard.clone())
            .unwrap()
    );
    assert_eq!(allocations.region_for_shard(&shard), Some(&region_b));
    assert_eq!(allocations.shards_for(&region_a), Some([].as_slice()));
    assert_eq!(allocations.shards_for(&region_b), Some([shard].as_slice()));
}

#[test]
fn least_shard_strategy_allocates_to_region_with_fewest_shards() {
    let strategy = LeastShardAllocationStrategy::default();
    let mut allocations = ShardAllocations::from_regions([
        "region-a".to_string(),
        "region-b".to_string(),
        "region-c".to_string(),
    ]);
    allocations
        .allocate_shard(&"region-a".to_string(), "1")
        .unwrap();
    allocations
        .allocate_shard(&"region-a".to_string(), "2")
        .unwrap();
    allocations
        .allocate_shard(&"region-b".to_string(), "3")
        .unwrap();

    let allocated = strategy
        .allocate_shard(&"region-a".to_string(), &"4".to_string(), &allocations)
        .unwrap();

    assert_eq!(allocated, "region-c");
}

#[test]
fn least_shard_strategy_rebalances_from_overloaded_regions() {
    let strategy = LeastShardAllocationStrategy::new(3, 1.0).unwrap();
    let mut allocations = ShardAllocations::from_regions([
        "region-a".to_string(),
        "region-b".to_string(),
        "region-c".to_string(),
    ]);
    for shard in ["1", "2", "3", "4", "5"] {
        allocations
            .allocate_shard(&"region-a".to_string(), shard)
            .unwrap();
    }
    allocations
        .allocate_shard(&"region-b".to_string(), "6")
        .unwrap();

    let rebalanced = strategy.rebalance(&allocations, &BTreeSet::new()).unwrap();

    assert_eq!(
        rebalanced,
        BTreeSet::from(["1".to_string(), "2".to_string(), "3".to_string()])
    );
}

#[test]
fn least_shard_strategy_limits_rebalance_and_skips_when_in_progress() {
    let strategy = LeastShardAllocationStrategy::new(2, 0.25).unwrap();
    let mut allocations =
        ShardAllocations::from_regions(["region-a".to_string(), "region-b".to_string()]);
    for shard in ["1", "2", "3", "4", "5", "6", "7", "8"] {
        allocations
            .allocate_shard(&"region-a".to_string(), shard)
            .unwrap();
    }

    let rebalanced = strategy.rebalance(&allocations, &BTreeSet::new()).unwrap();
    assert_eq!(rebalanced.len(), 2);

    let skipped = strategy
        .rebalance(&allocations, &BTreeSet::from(["1".to_string()]))
        .unwrap();
    assert!(skipped.is_empty());
}

#[test]
fn least_shard_strategy_rejects_invalid_rebalance_limits() {
    for (absolute_limit, relative_limit) in [(0, 0.1), (1, 0.0), (1, -0.1), (1, f64::NAN)] {
        assert_eq!(
            LeastShardAllocationStrategy::new(absolute_limit, relative_limit).unwrap_err(),
            ShardingError::InvalidRebalanceLimit
        );
    }
}

#[test]
fn least_shard_strategy_phase_two_moves_one_shard_to_empty_region() {
    let strategy = LeastShardAllocationStrategy::new(10, 1.0).unwrap();
    let mut allocations = ShardAllocations::from_regions([
        "region-a".to_string(),
        "region-b".to_string(),
        "region-c".to_string(),
    ]);
    allocations
        .allocate_shard(&"region-a".to_string(), "1")
        .unwrap();
    allocations
        .allocate_shard(&"region-a".to_string(), "2")
        .unwrap();
    allocations
        .allocate_shard(&"region-b".to_string(), "3")
        .unwrap();
    allocations
        .allocate_shard(&"region-b".to_string(), "4")
        .unwrap();

    let rebalanced = strategy.rebalance(&allocations, &BTreeSet::new()).unwrap();

    assert_eq!(rebalanced, BTreeSet::from(["1".to_string()]));
}
