#![deny(missing_docs)]
//! Deterministic coordinator decisions for shard allocation, rebalance, and shutdown.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BeginHandOff, CoordinatorEvent, CoordinatorState, GetShardHome, HostShard, RegionId,
    ShardAllocationStrategy, ShardAllocations, ShardId, ShardingError,
};

/// Decision produced for one shard-home request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetShardHomePlan {
    /// Return an already known shard owner.
    Reply {
        /// Requested shard.
        shard: ShardId,
        /// Current owner region.
        region: RegionId,
    },
    /// Persist a new owner and ask that region to host the shard.
    Allocated {
        /// Allocation event already applied to coordinator state.
        event: CoordinatorEvent,
        /// Region that must receive the host command.
        host_region: RegionId,
        /// Host command for the selected region.
        host_shard: HostShard,
    },
    /// Wait until the shard's in-progress rebalance completes.
    Deferred {
        /// Shard currently rebalancing.
        shard: ShardId,
        /// Region whose request must be retried afterward.
        requester: RegionId,
    },
    /// Do not answer or allocate under the current coordinator state.
    Ignored {
        /// Requested shard.
        shard: ShardId,
        /// State condition preventing a response.
        reason: GetShardHomeIgnoreReason,
    },
}

/// Reason a shard-home request cannot currently be served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetShardHomeIgnoreReason {
    /// Startup has not yet observed the required region registrations.
    NotAllRegionsRegistered,
    /// Every registered region is shutting down or terminating.
    NoActiveRegions,
    /// The known owner is terminating, so its stale home must not be returned.
    HomeRegionTerminating {
        /// Terminating owner.
        region: RegionId,
    },
    /// The allocation strategy selected a region absent from the validated active snapshot.
    AllocatedRegionNoLongerActive {
        /// Rejected strategy result.
        region: RegionId,
    },
}

/// Result of asking the allocation strategy to begin periodic rebalancing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalancePlan {
    /// Start one worker for each selected, currently owned shard.
    Started {
        /// Per-shard plans already marked in progress.
        shards: Vec<ShardRebalancePlan>,
    },
    /// No worker should start under the current state.
    Skipped {
        /// Condition that prevented or emptied the rebalance.
        reason: RebalanceSkipReason,
    },
}

/// Coordinator decision for a region's graceful-shutdown request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionShutdownPlan {
    /// Mark the region as shutting down and hand off its eligible shards.
    Started {
        /// Region beginning graceful shutdown.
        region: RegionId,
        /// Per-shard plans already marked in progress.
        shards: Vec<ShardRebalancePlan>,
    },
    /// Ignore a repeated request while this region is still marked as shutting down.
    AlreadyInProgress {
        /// Region with an existing graceful-shutdown attempt.
        region: RegionId,
    },
    /// Ignore a request from an unregistered region.
    UnknownRegion {
        /// Unknown region identity.
        region: RegionId,
    },
    /// The known region has no shard for which a new worker can start.
    ///
    /// This includes an empty region and a region whose shards are all already
    /// rebalancing. The region remains marked as gracefully shutting down.
    NoShards {
        /// Region with no newly startable shard handoffs.
        region: RegionId,
    },
}

/// Inputs required to run the two-phase handoff for one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRebalancePlan {
    /// Shard being moved or stopped.
    pub shard: ShardId,
    /// Region that currently owns and must stop the shard.
    pub from_region: RegionId,
    /// Snapshot of all regions and proxies that must invalidate the shard home.
    pub participants: BTreeSet<RegionId>,
    /// Stable begin-handoff command for the shard.
    pub begin_handoff: BeginHandOff,
}

/// Reason periodic rebalancing did not start any worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceSkipReason {
    /// Coordinator termination preparation disables new periodic work.
    PreparingForShutdown,
    /// At least one region is currently unavailable, making a safe balance snapshot impossible.
    RegionsUnavailable {
        /// Regions observed as unavailable.
        regions: BTreeSet<RegionId>,
    },
    /// No active shard-hosting region remains.
    NoShardRegions,
    /// The allocation strategy selected no shard.
    StrategySelectedNoShards,
    /// Every selected shard was already rebalancing or had no current owner.
    SelectedShardsMissingHomes,
}

/// State transition produced when one rebalance worker finishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceCompletionPlan {
    /// A successful handoff removed the old shard home.
    Deallocated {
        /// Shard whose old home was removed.
        shard: ShardId,
        /// Deallocation event already applied to coordinator state.
        event: CoordinatorEvent,
        /// Regions whose deferred home requests must be retried.
        pending_requesters: Vec<RegionId>,
        /// Coordinator-owned retry used to allocate remembered or otherwise unclaimed shards.
        retry_get_shard_home: GetShardHome,
    },
    /// Tracking was cleared without another deallocation event.
    ///
    /// This is returned for a late completion with no active worker or when the
    /// shard home disappeared through region termination during handoff.
    Cleared {
        /// Shard whose tracking was cleared or already absent.
        shard: ShardId,
        /// Regions whose deferred home requests must be retried.
        pending_requesters: Vec<RegionId>,
    },
    /// Handoff failed or timed out, leaving the old shard home allocated.
    TimedOut {
        /// Shard whose attempt failed.
        shard: ShardId,
        /// Regions whose deferred home requests must be retried.
        pending_requesters: Vec<RegionId>,
    },
}

/// Synchronous coordinator state machine layered over durable [`CoordinatorState`].
///
/// Durable registration and allocation facts live in the wrapped state.
/// Startup gates, shutdown observations, availability, and in-progress
/// rebalances are incarnation-local runtime state.
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
    /// Creates a runtime that initially considers all supplied regions registered.
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

    /// Returns the durable coordinator state.
    pub fn state(&self) -> &CoordinatorState {
        &self.state
    }

    /// Applies one durable registration or allocation event.
    ///
    /// The caller remains responsible for corresponding transient lifecycle
    /// markers such as terminating or unavailable regions.
    pub fn apply_event(&mut self, event: CoordinatorEvent) -> Result<(), ShardingError> {
        self.state.apply(event)
    }

    /// Adds remembered shard IDs that do not already have an allocation.
    ///
    /// Returns the newly added unallocated shard IDs in deterministic order.
    pub fn merge_remembered_shards(
        &mut self,
        shards: impl IntoIterator<Item = ShardId>,
    ) -> Vec<ShardId> {
        self.state.merge_remembered_shards(shards)
    }

    /// Builds home requests for every currently unallocated remembered shard.
    ///
    /// Returns an empty vector when remember-entities is disabled.
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

    /// Runs each unallocated remembered shard request through normal allocation.
    ///
    /// Plans are produced in deterministic shard order and each successful
    /// allocation is applied before the next strategy call.
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

    /// Consumes the runtime and discards its incarnation-local tracking state.
    pub fn into_state(self) -> CoordinatorState {
        self.state
    }

    /// Opens or closes the startup gate for shard-home requests.
    pub fn set_all_regions_registered(&mut self, all_regions_registered: bool) {
        self.all_regions_registered = all_regions_registered;
    }

    /// Returns whether the startup registration gate is open.
    pub fn all_regions_registered(&self) -> bool {
        self.all_regions_registered
    }

    /// Enables or disables coordinator shutdown preparation.
    ///
    /// Preparation suppresses periodic rebalance planning but does not rewrite
    /// durable coordinator state.
    pub fn set_preparing_for_shutdown(&mut self, preparing_for_shutdown: bool) {
        self.preparing_for_shutdown = preparing_for_shutdown;
    }

    /// Excludes a region from new allocations as part of graceful shutdown.
    pub fn mark_graceful_shutdown(&mut self, region: impl Into<RegionId>) {
        self.graceful_shutdown_regions.insert(region.into());
    }

    /// Makes a gracefully shutting-down region eligible for allocation again.
    pub fn unmark_graceful_shutdown(&mut self, region: &RegionId) {
        self.graceful_shutdown_regions.remove(region);
    }

    /// Marks a region termination as in progress and excludes it from allocation.
    pub fn mark_region_terminating(&mut self, region: impl Into<RegionId>) {
        self.terminating_regions.insert(region.into());
    }

    /// Clears a region's termination-in-progress marker.
    pub fn unmark_region_terminating(&mut self, region: &RegionId) {
        self.terminating_regions.remove(region);
    }

    /// Records a local availability observation that suppresses periodic rebalancing.
    pub fn mark_region_unavailable(&mut self, region: impl Into<RegionId>) {
        self.unavailable_regions.insert(region.into());
    }

    /// Clears a local availability observation after the region heals or departs.
    pub fn unmark_region_unavailable(&mut self, region: &RegionId) {
        self.unavailable_regions.remove(region);
    }

    /// Returns regions currently suppressing periodic rebalancing.
    pub fn unavailable_regions(&self) -> &BTreeSet<RegionId> {
        &self.unavailable_regions
    }

    /// Marks a shard as rebalancing.
    ///
    /// Returns `true` only for a new entry. A duplicate call is a no-op and
    /// preserves any requesters already deferred behind the active rebalance.
    pub fn begin_rebalance(&mut self, shard: impl Into<ShardId>) -> bool {
        let shard = shard.into();
        match self.rebalance_in_progress.entry(shard) {
            Entry::Vacant(entry) => {
                entry.insert(BTreeSet::new());
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Clears a shard's rebalance entry and returns deferred requesters in sorted order.
    pub fn clear_rebalance(&mut self, shard: &ShardId) -> Vec<RegionId> {
        self.rebalance_in_progress
            .remove(shard)
            .map(|requesters| requesters.into_iter().collect())
            .unwrap_or_default()
    }

    /// Returns requesters waiting for one shard's rebalance, or `None` when inactive.
    pub fn pending_rebalance_requesters(&self, shard: &ShardId) -> Option<&BTreeSet<RegionId>> {
        self.rebalance_in_progress.get(shard)
    }

    /// Returns all active rebalances and their deferred requesters.
    pub fn rebalance_in_progress(&self) -> &BTreeMap<ShardId, BTreeSet<RegionId>> {
        &self.rebalance_in_progress
    }

    /// Selects owned shards through the allocation strategy and marks workers in progress.
    ///
    /// Periodic rebalancing is suppressed during coordinator shutdown preparation
    /// or while any region is unavailable. Participants include every registered
    /// region and proxy, matching the shard-home invalidation fan-out.
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

    /// Plans handoff of every eligible shard owned by a gracefully shutting-down region.
    ///
    /// The region is excluded from new allocation before plans are returned.
    /// Unknown and repeated requests do not mutate handoff tracking.
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

    /// Applies one worker result and releases deferred shard-home requesters.
    ///
    /// Success deallocates a still-owned shard. Failure retains the old home and
    /// clears its graceful-shutdown marker so the region can retry, matching
    /// Pekko's handoff-timeout behavior.
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
            if let Some(region) = self.state.shard_home(&shard) {
                self.graceful_shutdown_regions.remove(region);
            }
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

    /// Resolves, defers, allocates, or ignores one shard-home request.
    ///
    /// Known homes are returned without invoking the strategy unless their owner
    /// is terminating. New allocations exclude graceful-shutdown and terminating
    /// regions, validate the strategy result against that active snapshot, and
    /// apply the allocation event before returning the host command.
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
