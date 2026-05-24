use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CoordinatorEvent, CoordinatorState, HostShard, RegionId, ShardAllocationStrategy,
    ShardAllocations, ShardId, ShardingError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetShardHomePlan {
    Reply {
        shard: ShardId,
        region: RegionId,
    },
    Allocated {
        event: CoordinatorEvent,
        host_region: RegionId,
        host_shard: HostShard,
    },
    Deferred {
        shard: ShardId,
        requester: RegionId,
    },
    Ignored {
        shard: ShardId,
        reason: GetShardHomeIgnoreReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetShardHomeIgnoreReason {
    NotAllRegionsRegistered,
    NoActiveRegions,
    HomeRegionTerminating { region: RegionId },
    AllocatedRegionNoLongerActive { region: RegionId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorRuntime {
    state: CoordinatorState,
    all_regions_registered: bool,
    graceful_shutdown_regions: BTreeSet<RegionId>,
    terminating_regions: BTreeSet<RegionId>,
    rebalance_in_progress: BTreeMap<ShardId, BTreeSet<RegionId>>,
}

impl CoordinatorRuntime {
    pub fn new(state: CoordinatorState) -> Self {
        Self {
            state,
            all_regions_registered: true,
            graceful_shutdown_regions: BTreeSet::new(),
            terminating_regions: BTreeSet::new(),
            rebalance_in_progress: BTreeMap::new(),
        }
    }

    pub fn state(&self) -> &CoordinatorState {
        &self.state
    }

    pub fn apply_event(&mut self, event: CoordinatorEvent) -> Result<(), ShardingError> {
        self.state.apply(event)
    }

    pub fn into_state(self) -> CoordinatorState {
        self.state
    }

    pub fn set_all_regions_registered(&mut self, all_regions_registered: bool) {
        self.all_regions_registered = all_regions_registered;
    }

    pub fn mark_graceful_shutdown(&mut self, region: impl Into<RegionId>) {
        self.graceful_shutdown_regions.insert(region.into());
    }

    pub fn unmark_graceful_shutdown(&mut self, region: &RegionId) {
        self.graceful_shutdown_regions.remove(region);
    }

    pub fn mark_region_terminating(&mut self, region: impl Into<RegionId>) {
        self.terminating_regions.insert(region.into());
    }

    pub fn unmark_region_terminating(&mut self, region: &RegionId) {
        self.terminating_regions.remove(region);
    }

    pub fn begin_rebalance(&mut self, shard: impl Into<ShardId>) -> bool {
        self.rebalance_in_progress
            .insert(shard.into(), BTreeSet::new())
            .is_none()
    }

    pub fn clear_rebalance(&mut self, shard: &ShardId) -> Vec<RegionId> {
        self.rebalance_in_progress
            .remove(shard)
            .map(|requesters| requesters.into_iter().collect())
            .unwrap_or_default()
    }

    pub fn pending_rebalance_requesters(&self, shard: &ShardId) -> Option<&BTreeSet<RegionId>> {
        self.rebalance_in_progress.get(shard)
    }

    pub fn request_shard_home<S>(
        &mut self,
        requester: impl Into<RegionId>,
        shard: impl Into<ShardId>,
        strategy: &S,
    ) -> Result<GetShardHomePlan, ShardingError>
    where
        S: ShardAllocationStrategy,
    {
        let requester = requester.into();
        let shard = shard.into();

        if self.rebalance_in_progress.contains_key(&shard) {
            self.defer_request(shard.clone(), requester.clone());
            return Ok(GetShardHomePlan::Deferred { shard, requester });
        }

        if !self.all_regions_registered {
            return Ok(GetShardHomePlan::Ignored {
                shard,
                reason: GetShardHomeIgnoreReason::NotAllRegionsRegistered,
            });
        }

        if let Some(region) = self.state.shard_home(&shard).cloned() {
            if self.terminating_regions.contains(&region) {
                return Ok(GetShardHomePlan::Ignored {
                    shard,
                    reason: GetShardHomeIgnoreReason::HomeRegionTerminating { region },
                });
            }
            return Ok(GetShardHomePlan::Reply { shard, region });
        }

        let active_allocations = self.active_allocations();
        if active_allocations.region_count() == 0 {
            return Ok(GetShardHomePlan::Ignored {
                shard,
                reason: GetShardHomeIgnoreReason::NoActiveRegions,
            });
        }

        let region = strategy.allocate_shard(&requester, &shard, &active_allocations)?;
        if !active_allocations.contains_region(&region) {
            return Ok(GetShardHomePlan::Ignored {
                shard,
                reason: GetShardHomeIgnoreReason::AllocatedRegionNoLongerActive { region },
            });
        }

        let event = CoordinatorEvent::ShardHomeAllocated {
            shard: shard.clone(),
            region: region.clone(),
        };
        self.state.apply(event.clone())?;
        Ok(GetShardHomePlan::Allocated {
            event,
            host_region: region,
            host_shard: HostShard { shard_id: shard },
        })
    }

    fn defer_request(&mut self, shard: ShardId, requester: RegionId) {
        self.rebalance_in_progress
            .entry(shard)
            .or_default()
            .insert(requester);
    }

    fn active_allocations(&self) -> ShardAllocations {
        let mut active = self.state.allocations().clone();
        for region in self
            .graceful_shutdown_regions
            .iter()
            .chain(self.terminating_regions.iter())
        {
            active.remove_region(region);
        }
        active
    }
}
