use std::collections::BTreeSet;
use std::sync::mpsc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorResult, ActorSystem, Context, Props};

use crate::{
    EntityRef, LeastShardAllocationStrategy, ShardAllocationStrategy, ShardAllocations,
    ShardingEnvelope, ShardingError, default_shard_id_for, shard_id_for, stable_hash_entity_id,
};

#[test]
fn sharding_envelope_keeps_entity_id_outside_business_message() {
    let envelope = ShardingEnvelope::new("counter-1", "increment");

    assert_eq!(envelope.entity_id(), "counter-1");
    assert_eq!(envelope.message(), &"increment");
    assert_eq!(
        envelope.into_parts(),
        ("counter-1".to_string(), "increment")
    );
}

#[test]
fn entity_ref_wraps_business_message_in_sharding_envelope() {
    let system = ActorSystem::builder("sharding").build().unwrap();
    let (tx, rx) = mpsc::channel();
    let region = system
        .spawn("region", Props::new(move || RegionProbe { observed: tx }))
        .unwrap();
    let entity_ref = EntityRef::new("counter-1", region);

    entity_ref.tell("increment").unwrap();

    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("counter-1".to_string(), "increment")
    );
}

#[test]
fn shard_ids_use_documented_stable_hash() {
    assert_eq!(stable_hash_entity_id("counter-1"), 0x31c4c004cce265c1);
    assert_eq!(shard_id_for("counter-1", 100).unwrap(), "65");
    assert_eq!(default_shard_id_for("counter-1"), "65");
}

#[test]
fn shard_id_rejects_zero_shards() {
    assert_eq!(
        shard_id_for("counter-1", 0),
        Err(ShardingError::InvalidShardCount)
    );
}

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

struct RegionProbe {
    observed: mpsc::Sender<(String, &'static str)>,
}

impl Actor for RegionProbe {
    type Msg = ShardingEnvelope<&'static str>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        let (entity_id, message) = msg.into_parts();
        self.observed
            .send((entity_id, message))
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}
