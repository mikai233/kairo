use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BeginHandOff, CoordinatorEvent, CoordinatorState, GetShardHome, HostShard, RegionId,
    ShardAllocationStrategy, ShardAllocations, ShardId, ShardingError,
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
pub enum RebalancePlan {
    Started { shards: Vec<ShardRebalancePlan> },
    Skipped { reason: RebalanceSkipReason },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionShutdownPlan {
    Started {
        region: RegionId,
        shards: Vec<ShardRebalancePlan>,
    },
    AlreadyInProgress {
        region: RegionId,
    },
    UnknownRegion {
        region: RegionId,
    },
    NoShards {
        region: RegionId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRebalancePlan {
    pub shard: ShardId,
    pub from_region: RegionId,
    pub participants: BTreeSet<RegionId>,
    pub begin_handoff: BeginHandOff,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceSkipReason {
    PreparingForShutdown,
    RegionsUnavailable { regions: BTreeSet<RegionId> },
    NoShardRegions,
    StrategySelectedNoShards,
    SelectedShardsMissingHomes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceCompletionPlan {
    Deallocated {
        shard: ShardId,
        event: CoordinatorEvent,
        pending_requesters: Vec<RegionId>,
        retry_get_shard_home: GetShardHome,
    },
    Cleared {
        shard: ShardId,
        pending_requesters: Vec<RegionId>,
    },
    TimedOut {
        shard: ShardId,
        pending_requesters: Vec<RegionId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorRuntime {
    state: CoordinatorState,
    all_regions_registered: bool,
    graceful_shutdown_regions: BTreeSet<RegionId>,
    terminating_regions: BTreeSet<RegionId>,
    unavailable_regions: BTreeSet<RegionId>,
    rebalance_in_progress: BTreeMap<ShardId, BTreeSet<RegionId>>,
    preparing_for_shutdown: bool,
}

impl CoordinatorRuntime {
    pub fn new(state: CoordinatorState) -> Self {
        Self {
            state,
            all_regions_registered: true,
            graceful_shutdown_regions: BTreeSet::new(),
            terminating_regions: BTreeSet::new(),
            unavailable_regions: BTreeSet::new(),
            rebalance_in_progress: BTreeMap::new(),
            preparing_for_shutdown: false,
        }
    }

    pub fn state(&self) -> &CoordinatorState {
        &self.state
    }

    pub fn apply_event(&mut self, event: CoordinatorEvent) -> Result<(), ShardingError> {
        self.state.apply(event)
    }

    pub fn merge_remembered_shards(
        &mut self,
        shards: impl IntoIterator<Item = ShardId>,
    ) -> Vec<ShardId> {
        self.state.merge_remembered_shards(shards)
    }

    pub fn remembered_shard_home_requests(&self) -> Vec<GetShardHome> {
        if !self.state.remember_entities() {
            return Vec::new();
        }
        self.state
            .unallocated_shards()
            .iter()
            .cloned()
            .map(|shard_id| GetShardHome { shard_id })
            .collect()
    }

    pub fn allocate_remembered_shard_homes<S>(
        &mut self,
        requester: impl Into<RegionId>,
        strategy: &S,
    ) -> Result<Vec<GetShardHomePlan>, ShardingError>
    where
        S: ShardAllocationStrategy + ?Sized,
    {
        let requester = requester.into();
        let requests = self.remembered_shard_home_requests();
        let mut plans = Vec::with_capacity(requests.len());
        for request in requests {
            plans.push(self.request_shard_home(requester.clone(), request.shard_id, strategy)?);
        }
        Ok(plans)
    }

    pub fn into_state(self) -> CoordinatorState {
        self.state
    }

    pub fn set_all_regions_registered(&mut self, all_regions_registered: bool) {
        self.all_regions_registered = all_regions_registered;
    }

    pub fn set_preparing_for_shutdown(&mut self, preparing_for_shutdown: bool) {
        self.preparing_for_shutdown = preparing_for_shutdown;
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

    pub fn mark_region_unavailable(&mut self, region: impl Into<RegionId>) {
        self.unavailable_regions.insert(region.into());
    }

    pub fn unmark_region_unavailable(&mut self, region: &RegionId) {
        self.unavailable_regions.remove(region);
    }

    pub fn unavailable_regions(&self) -> &BTreeSet<RegionId> {
        &self.unavailable_regions
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

    pub fn rebalance_in_progress(&self) -> &BTreeMap<ShardId, BTreeSet<RegionId>> {
        &self.rebalance_in_progress
    }

    pub fn plan_rebalance<S>(&mut self, strategy: &S) -> Result<RebalancePlan, ShardingError>
    where
        S: ShardAllocationStrategy + ?Sized,
    {
        if self.preparing_for_shutdown {
            return Ok(RebalancePlan::Skipped {
                reason: RebalanceSkipReason::PreparingForShutdown,
            });
        }

        if !self.unavailable_regions.is_empty() {
            return Ok(RebalancePlan::Skipped {
                reason: RebalanceSkipReason::RegionsUnavailable {
                    regions: self.unavailable_regions.clone(),
                },
            });
        }

        let active_allocations = self.active_allocations();
        if active_allocations.region_count() == 0 {
            return Ok(RebalancePlan::Skipped {
                reason: RebalanceSkipReason::NoShardRegions,
            });
        }

        let in_progress = self
            .rebalance_in_progress
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let selected = strategy.rebalance(&active_allocations, &in_progress)?;
        if selected.is_empty() {
            return Ok(RebalancePlan::Skipped {
                reason: RebalanceSkipReason::StrategySelectedNoShards,
            });
        }

        let participants = self.rebalance_participants();
        let mut plans = Vec::new();
        for shard in selected {
            if self.rebalance_in_progress.contains_key(&shard) {
                continue;
            }
            if let Some(from_region) = self.state.shard_home(&shard).cloned() {
                self.begin_rebalance(shard.clone());
                plans.push(ShardRebalancePlan {
                    begin_handoff: BeginHandOff {
                        shard_id: shard.clone(),
                    },
                    participants: participants.clone(),
                    shard,
                    from_region,
                });
            }
        }

        if plans.is_empty() {
            Ok(RebalancePlan::Skipped {
                reason: RebalanceSkipReason::SelectedShardsMissingHomes,
            })
        } else {
            Ok(RebalancePlan::Started { shards: plans })
        }
    }

    pub fn plan_region_shutdown(&mut self, region: impl Into<RegionId>) -> RegionShutdownPlan {
        let region = region.into();
        if self.graceful_shutdown_regions.contains(&region) {
            return RegionShutdownPlan::AlreadyInProgress { region };
        }
        let Some(shards) = self
            .state
            .allocations()
            .shards_for(&region)
            .map(|shards| shards.to_vec())
        else {
            return RegionShutdownPlan::UnknownRegion { region };
        };

        self.mark_graceful_shutdown(region.clone());
        let participants = self.rebalance_participants();
        let mut plans = Vec::new();
        for shard in shards {
            if self.rebalance_in_progress.contains_key(&shard) {
                continue;
            }
            self.begin_rebalance(shard.clone());
            plans.push(ShardRebalancePlan {
                begin_handoff: BeginHandOff {
                    shard_id: shard.clone(),
                },
                participants: participants.clone(),
                from_region: region.clone(),
                shard,
            });
        }

        if plans.is_empty() {
            RegionShutdownPlan::NoShards { region }
        } else {
            RegionShutdownPlan::Started {
                region,
                shards: plans,
            }
        }
    }

    pub fn complete_rebalance(
        &mut self,
        shard: impl Into<ShardId>,
        ok: bool,
    ) -> Result<RebalanceCompletionPlan, ShardingError> {
        let shard = shard.into();
        if !self.rebalance_in_progress.contains_key(&shard) {
            return Ok(RebalanceCompletionPlan::Cleared {
                shard,
                pending_requesters: Vec::new(),
            });
        }

        if !ok {
            let pending_requesters = self.clear_rebalance(&shard);
            return Ok(RebalanceCompletionPlan::TimedOut {
                shard,
                pending_requesters,
            });
        }

        if self.state.shard_home(&shard).is_some() {
            let event = CoordinatorEvent::ShardHomeDeallocated {
                shard: shard.clone(),
            };
            self.state.apply(event.clone())?;
            let pending_requesters = self.clear_rebalance(&shard);
            Ok(RebalanceCompletionPlan::Deallocated {
                retry_get_shard_home: GetShardHome {
                    shard_id: shard.clone(),
                },
                event,
                pending_requesters,
                shard,
            })
        } else {
            let pending_requesters = self.clear_rebalance(&shard);
            Ok(RebalanceCompletionPlan::Cleared {
                shard,
                pending_requesters,
            })
        }
    }

    pub fn request_shard_home<S>(
        &mut self,
        requester: impl Into<RegionId>,
        shard: impl Into<ShardId>,
        strategy: &S,
    ) -> Result<GetShardHomePlan, ShardingError>
    where
        S: ShardAllocationStrategy + ?Sized,
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

    fn rebalance_participants(&self) -> BTreeSet<RegionId> {
        self.state
            .allocations()
            .regions()
            .chain(self.state.proxies().iter())
            .cloned()
            .collect()
    }
}
