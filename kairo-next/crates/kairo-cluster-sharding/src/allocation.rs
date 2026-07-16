#![deny(missing_docs)]

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use crate::{RegionId, ShardId, ShardingError};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// Deterministic mapping from registered regions to their currently owned shards.
///
/// A shard has at most one owner. Regions are ordered by stable [`RegionId`],
/// while each region's shard slice preserves allocation order.
pub struct ShardAllocations {
    regions: BTreeMap<RegionId, Vec<ShardId>>,
    shard_homes: BTreeMap<ShardId, RegionId>,
}

impl ShardAllocations {
    /// Creates an allocation table with no registered regions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty shard allocation for every distinct supplied region.
    pub fn from_regions(regions: impl IntoIterator<Item = RegionId>) -> Self {
        let mut allocations = Self::new();
        for region in regions {
            allocations.insert_region(region);
        }
        allocations
    }

    /// Registers an empty region without disturbing an existing allocation.
    ///
    /// Returns `true` only when the region was newly inserted.
    pub fn insert_region(&mut self, region: impl Into<RegionId>) -> bool {
        match self.regions.entry(region.into()) {
            Entry::Vacant(entry) => {
                entry.insert(Vec::new());
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Returns whether `region` is registered.
    pub fn contains_region(&self, region: &RegionId) -> bool {
        self.regions.contains_key(region)
    }

    /// Returns whether no regions are registered.
    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }

    /// Removes a region and returns the shards it owned in allocation order.
    pub fn remove_region(&mut self, region: &RegionId) -> Option<Vec<ShardId>> {
        let shards = self.regions.remove(region)?;
        for shard in &shards {
            let removed = self.shard_homes.remove(shard);
            debug_assert_eq!(removed.as_ref(), Some(region));
        }
        Some(shards)
    }

    /// Assigns `shard` exclusively to `region`.
    ///
    /// An assignment to the current owner is an unchanged `Ok(false)`. Moving
    /// from another owner returns `Ok(true)`. Unknown regions are rejected.
    pub fn allocate_shard(
        &mut self,
        region: &RegionId,
        shard: impl Into<ShardId>,
    ) -> Result<bool, ShardingError> {
        if !self.regions.contains_key(region) {
            return Err(ShardingError::UnknownRegion(region.clone()));
        }
        let shard = shard.into();
        if self
            .shard_homes
            .get(&shard)
            .is_some_and(|owner| owner == region)
        {
            return Ok(false);
        }
        self.deallocate_shard(&shard);
        let shards = self
            .regions
            .get_mut(region)
            .expect("region existence checked before deallocation");
        shards.push(shard.clone());
        let previous = self.shard_homes.insert(shard, region.clone());
        debug_assert!(previous.is_none());
        Ok(true)
    }

    /// Removes a shard assignment and returns its former owner.
    pub fn deallocate_shard(&mut self, shard: &ShardId) -> Option<RegionId> {
        let region = self.shard_homes.remove(shard)?;
        let shards = self
            .regions
            .get_mut(&region)
            .expect("indexed shard home must be a registered region");
        let index = shards
            .iter()
            .position(|existing| existing == shard)
            .expect("indexed shard must be present in its region allocation");
        shards.remove(index);
        Some(region)
    }

    /// Returns the current owner of `shard`.
    pub fn region_for_shard(&self, shard: &ShardId) -> Option<&RegionId> {
        self.shard_homes.get(shard)
    }

    /// Iterates registered regions in stable identifier order.
    pub fn regions(&self) -> impl Iterator<Item = &RegionId> {
        self.regions.keys()
    }

    /// Iterates allocated shards by stable region order and per-region allocation order.
    pub fn shards(&self) -> impl Iterator<Item = &ShardId> {
        self.regions.values().flat_map(|shards| shards.iter())
    }

    /// Returns the shards assigned to `region` in allocation order.
    pub fn shards_for(&self, region: &RegionId) -> Option<&[ShardId]> {
        self.regions.get(region).map(Vec::as_slice)
    }

    /// Returns the number of registered regions.
    pub fn region_count(&self) -> usize {
        self.regions.len()
    }

    /// Returns the total number of allocated shards.
    pub fn shard_count(&self) -> usize {
        self.shard_homes.len()
    }

    fn sorted_entries(&self) -> Vec<(&RegionId, &[ShardId])> {
        let mut entries = self
            .regions
            .iter()
            .map(|(region, shards)| (region, shards.as_slice()))
            .collect::<Vec<_>>();
        entries.sort_by(|(left_region, left_shards), (right_region, right_shards)| {
            left_shards
                .len()
                .cmp(&right_shards.len())
                .then_with(|| left_region.cmp(right_region))
        });
        entries
    }
}

/// Policy used by the coordinator to place new shards and select rebalance candidates.
///
/// Implementations receive immutable allocation snapshots and must return
/// region/shard identifiers already present in those snapshots. The coordinator
/// owns handoff and state mutation after a policy decision.
pub trait ShardAllocationStrategy {
    /// Selects a registered region for an unallocated shard request.
    fn allocate_shard(
        &self,
        requester: &RegionId,
        shard: &ShardId,
        current: &ShardAllocations,
    ) -> Result<RegionId, ShardingError>;

    /// Selects allocated shards to migrate during one rebalance round.
    ///
    /// `in_progress` identifies shards already undergoing handoff and must not
    /// be selected again.
    fn rebalance(
        &self,
        current: &ShardAllocations,
        in_progress: &BTreeSet<ShardId>,
    ) -> Result<BTreeSet<ShardId>, ShardingError>;
}

#[derive(Debug, Clone)]
/// Pekko-aligned least-shards policy with bounded two-phase rebalancing.
///
/// New shards go to the least-loaded region, breaking ties by stable region id.
/// Rebalancing first removes load above the ceiling-optimal count, then fills
/// regions materially below that count. It starts no work while any prior
/// rebalance is in progress.
pub struct LeastShardAllocationStrategy {
    absolute_limit: usize,
    relative_limit: f64,
}

impl LeastShardAllocationStrategy {
    /// Creates a strategy with absolute and relative per-round move limits.
    ///
    /// The absolute limit must be non-zero and the relative limit must be
    /// positive and finite.
    pub fn new(absolute_limit: usize, relative_limit: f64) -> Result<Self, ShardingError> {
        if absolute_limit == 0 {
            return Err(ShardingError::InvalidRebalanceLimit);
        }
        if !relative_limit.is_finite() || relative_limit <= 0.0 {
            return Err(ShardingError::InvalidRebalanceLimit);
        }
        Ok(Self {
            absolute_limit,
            relative_limit,
        })
    }

    /// Returns the maximum number of shards moved in one round.
    pub fn absolute_limit(&self) -> usize {
        self.absolute_limit
    }

    /// Returns the total-shard fraction used to bound one round.
    pub fn relative_limit(&self) -> f64 {
        self.relative_limit
    }

    fn rebalance_limit(&self, shard_count: usize) -> usize {
        let relative = (self.relative_limit * shard_count as f64) as usize;
        self.absolute_limit.min(relative).max(1)
    }
}

impl Default for LeastShardAllocationStrategy {
    fn default() -> Self {
        Self {
            absolute_limit: 10,
            relative_limit: 0.1,
        }
    }
}

impl ShardAllocationStrategy for LeastShardAllocationStrategy {
    fn allocate_shard(
        &self,
        _requester: &RegionId,
        _shard: &ShardId,
        current: &ShardAllocations,
    ) -> Result<RegionId, ShardingError> {
        current
            .regions
            .iter()
            .min_by(|(left_region, left_shards), (right_region, right_shards)| {
                left_shards
                    .len()
                    .cmp(&right_shards.len())
                    .then_with(|| left_region.cmp(right_region))
            })
            .map(|(region, _)| region.clone())
            .ok_or(ShardingError::NoShardRegions)
    }

    fn rebalance(
        &self,
        current: &ShardAllocations,
        in_progress: &BTreeSet<ShardId>,
    ) -> Result<BTreeSet<ShardId>, ShardingError> {
        if !in_progress.is_empty() {
            return Ok(BTreeSet::new());
        }

        let shard_count = current.shard_count();
        let region_count = current.region_count();
        if shard_count == 0 || region_count == 0 {
            return Ok(BTreeSet::new());
        }

        let optimal_per_region = shard_count.div_ceil(region_count);
        let limit = self.rebalance_limit(shard_count);
        let entries = current.sorted_entries();

        let mut selected = Vec::new();
        for (_, shards) in &entries {
            if shards.len() > optimal_per_region {
                selected.extend(
                    shards
                        .iter()
                        .take(shards.len() - optimal_per_region)
                        .cloned(),
                );
            }
        }
        if !selected.is_empty() {
            return Ok(selected.into_iter().take(limit).collect());
        }

        let count_below_optimal = entries
            .iter()
            .map(|(_, shards)| {
                optimal_per_region
                    .saturating_sub(1)
                    .saturating_sub(shards.len())
            })
            .sum::<usize>();
        if count_below_optimal == 0 {
            return Ok(BTreeSet::new());
        }

        Ok(entries
            .into_iter()
            .filter(|(_, shards)| shards.len() >= optimal_per_region)
            .filter_map(|(_, shards)| shards.first().cloned())
            .take(count_below_optimal.min(limit))
            .collect())
    }
}
