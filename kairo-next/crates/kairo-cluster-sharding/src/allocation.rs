use std::collections::{BTreeMap, BTreeSet};

use crate::{RegionId, ShardId, ShardingError};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShardAllocations {
    regions: BTreeMap<RegionId, Vec<ShardId>>,
}

impl ShardAllocations {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_regions(regions: impl IntoIterator<Item = RegionId>) -> Self {
        let mut allocations = Self::new();
        for region in regions {
            allocations.insert_region(region);
        }
        allocations
    }

    pub fn insert_region(&mut self, region: impl Into<RegionId>) -> bool {
        self.regions.insert(region.into(), Vec::new()).is_none()
    }

    pub fn remove_region(&mut self, region: &RegionId) -> Option<Vec<ShardId>> {
        self.regions.remove(region)
    }

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
            .region_for_shard(&shard)
            .is_some_and(|owner| owner == region)
        {
            return Ok(false);
        }
        self.deallocate_shard(&shard);
        let shards = self
            .regions
            .get_mut(region)
            .expect("region existence checked before deallocation");
        if shards.contains(&shard) {
            return Ok(false);
        }
        shards.push(shard);
        Ok(true)
    }

    pub fn deallocate_shard(&mut self, shard: &ShardId) -> Option<RegionId> {
        for (region, shards) in &mut self.regions {
            if let Some(index) = shards.iter().position(|existing| existing == shard) {
                shards.remove(index);
                return Some(region.clone());
            }
        }
        None
    }

    pub fn region_for_shard(&self, shard: &ShardId) -> Option<&RegionId> {
        self.regions
            .iter()
            .find_map(|(region, shards)| shards.contains(shard).then_some(region))
    }

    pub fn regions(&self) -> impl Iterator<Item = &RegionId> {
        self.regions.keys()
    }

    pub fn shards_for(&self, region: &RegionId) -> Option<&[ShardId]> {
        self.regions.get(region).map(Vec::as_slice)
    }

    pub fn region_count(&self) -> usize {
        self.regions.len()
    }

    pub fn shard_count(&self) -> usize {
        self.regions.values().map(Vec::len).sum()
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

pub trait ShardAllocationStrategy {
    fn allocate_shard(
        &self,
        requester: &RegionId,
        shard: &ShardId,
        current: &ShardAllocations,
    ) -> Result<RegionId, ShardingError>;

    fn rebalance(
        &self,
        current: &ShardAllocations,
        in_progress: &BTreeSet<ShardId>,
    ) -> Result<BTreeSet<ShardId>, ShardingError>;
}

#[derive(Debug, Clone)]
pub struct LeastShardAllocationStrategy {
    absolute_limit: usize,
    relative_limit: f64,
}

impl LeastShardAllocationStrategy {
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

    pub fn absolute_limit(&self) -> usize {
        self.absolute_limit
    }

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
            .sorted_entries()
            .into_iter()
            .next()
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
