#![deny(missing_docs)]

use std::collections::BTreeSet;

use crate::{RegionId, ShardAllocations, ShardId, ShardingError};

#[derive(Debug, Clone, PartialEq, Eq)]
/// State transition understood by the shard coordinator model.
pub enum CoordinatorEvent {
    /// Adds a region eligible to host shards.
    ShardRegionRegistered {
        /// Stable logical region identifier.
        region: RegionId,
    },
    /// Adds a proxy that routes messages but cannot host shards.
    ShardRegionProxyRegistered {
        /// Stable logical proxy identifier.
        proxy: RegionId,
    },
    /// Removes a region and releases all shards it owned.
    ShardRegionTerminated {
        /// Stable logical region identifier.
        region: RegionId,
    },
    /// Removes a registered proxy.
    ShardRegionProxyTerminated {
        /// Stable logical proxy identifier.
        proxy: RegionId,
    },
    /// Assigns one previously unallocated shard to a region.
    ShardHomeAllocated {
        /// Stable logical shard identifier.
        shard: ShardId,
        /// Registered region that becomes the owner.
        region: RegionId,
    },
    /// Releases a shard from its current region.
    ShardHomeDeallocated {
        /// Stable logical shard identifier.
        shard: ShardId,
    },
    /// Marks completion of coordinator state initialization without mutating assignments.
    ShardCoordinatorInitialized,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// Pure coordinator ownership state for regions, proxies, and remembered shards.
///
/// When remember-entities is enabled, deallocated shards remain in the
/// unallocated set so recovery can assign them again after region or
/// coordinator failover.
pub struct CoordinatorState {
    allocations: ShardAllocations,
    proxies: BTreeSet<RegionId>,
    unallocated_shards: BTreeSet<ShardId>,
    remember_entities: bool,
}

impl CoordinatorState {
    /// Creates empty state with remember-entities disabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables or disables remembered unallocated shard tracking.
    ///
    /// Disabling the feature immediately clears the unallocated set while
    /// leaving live allocations unchanged.
    pub fn with_remember_entities(mut self, enabled: bool) -> Self {
        self.remember_entities = enabled;
        if !enabled {
            self.unallocated_shards.clear();
        }
        self
    }

    /// Returns whether unallocated shards are retained for recovery.
    pub fn remember_entities(&self) -> bool {
        self.remember_entities
    }

    /// Returns whether no region or proxy is registered.
    pub fn is_empty(&self) -> bool {
        self.allocations.is_empty() && self.proxies.is_empty()
    }

    /// Returns the current exclusive shard-to-region assignments.
    pub fn allocations(&self) -> &ShardAllocations {
        &self.allocations
    }

    /// Returns registered proxy identifiers in deterministic order.
    pub fn proxies(&self) -> &BTreeSet<RegionId> {
        &self.proxies
    }

    /// Returns remembered shards currently waiting for allocation.
    pub fn unallocated_shards(&self) -> &BTreeSet<ShardId> {
        &self.unallocated_shards
    }

    /// Returns the union of allocated and remembered-unallocated shard ids.
    pub fn all_shards(&self) -> BTreeSet<ShardId> {
        self.allocations
            .shards()
            .cloned()
            .chain(self.unallocated_shards.iter().cloned())
            .collect()
    }

    /// Merges shards loaded from a remember store into the unallocated set.
    ///
    /// Already allocated or previously remembered shards are ignored. The
    /// returned vector contains only newly added ids in input order.
    pub fn merge_remembered_shards(
        &mut self,
        shards: impl IntoIterator<Item = ShardId>,
    ) -> Vec<ShardId> {
        if !self.remember_entities {
            return Vec::new();
        }

        let mut added = Vec::new();
        for shard in shards {
            if self.allocations.region_for_shard(&shard).is_none()
                && self.unallocated_shards.insert(shard.clone())
            {
                added.push(shard);
            }
        }
        added
    }

    /// Returns the region currently hosting `shard`.
    pub fn shard_home(&self, shard: &ShardId) -> Option<&RegionId> {
        self.allocations.region_for_shard(shard)
    }

    /// Validates and applies one coordinator transition.
    ///
    /// Duplicate registrations, unknown removals, and duplicate allocations are
    /// rejected without discarding existing assignments.
    pub fn apply(&mut self, event: CoordinatorEvent) -> Result<(), ShardingError> {
        match event {
            CoordinatorEvent::ShardRegionRegistered { region } => self.register_region(region),
            CoordinatorEvent::ShardRegionProxyRegistered { proxy } => self.register_proxy(proxy),
            CoordinatorEvent::ShardRegionTerminated { region } => self.terminate_region(&region),
            CoordinatorEvent::ShardRegionProxyTerminated { proxy } => self.terminate_proxy(&proxy),
            CoordinatorEvent::ShardHomeAllocated { shard, region } => {
                self.allocate_shard(shard, &region)
            }
            CoordinatorEvent::ShardHomeDeallocated { shard } => self.deallocate_shard(&shard),
            CoordinatorEvent::ShardCoordinatorInitialized => Ok(()),
        }
    }

    fn register_region(&mut self, region: RegionId) -> Result<(), ShardingError> {
        if !self.allocations.insert_region(region.clone()) {
            return Err(ShardingError::RegionAlreadyRegistered(region));
        }
        Ok(())
    }

    fn register_proxy(&mut self, proxy: RegionId) -> Result<(), ShardingError> {
        if !self.proxies.insert(proxy.clone()) {
            return Err(ShardingError::RegionProxyAlreadyRegistered(proxy));
        }
        Ok(())
    }

    fn terminate_region(&mut self, region: &RegionId) -> Result<(), ShardingError> {
        let Some(shards) = self.allocations.remove_region(region) else {
            return Err(ShardingError::UnknownRegion(region.clone()));
        };
        if self.remember_entities {
            self.unallocated_shards.extend(shards);
        }
        Ok(())
    }

    fn terminate_proxy(&mut self, proxy: &RegionId) -> Result<(), ShardingError> {
        if self.proxies.remove(proxy) {
            Ok(())
        } else {
            Err(ShardingError::UnknownRegionProxy(proxy.clone()))
        }
    }

    fn allocate_shard(&mut self, shard: ShardId, region: &RegionId) -> Result<(), ShardingError> {
        if self.allocations.region_for_shard(&shard).is_some() {
            return Err(ShardingError::ShardAlreadyAllocated(shard));
        }
        self.allocations.allocate_shard(region, shard.clone())?;
        if self.remember_entities {
            self.unallocated_shards.remove(&shard);
        }
        Ok(())
    }

    fn deallocate_shard(&mut self, shard: &ShardId) -> Result<(), ShardingError> {
        if self.allocations.deallocate_shard(shard).is_none() {
            return Err(ShardingError::UnknownShard(shard.clone()));
        }
        if self.remember_entities {
            self.unallocated_shards.insert(shard.clone());
        }
        Ok(())
    }
}
