use std::collections::BTreeSet;

use crate::{RegionId, ShardAllocations, ShardId, ShardingError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorEvent {
    ShardRegionRegistered { region: RegionId },
    ShardRegionProxyRegistered { proxy: RegionId },
    ShardRegionTerminated { region: RegionId },
    ShardRegionProxyTerminated { proxy: RegionId },
    ShardHomeAllocated { shard: ShardId, region: RegionId },
    ShardHomeDeallocated { shard: ShardId },
    ShardCoordinatorInitialized,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CoordinatorState {
    allocations: ShardAllocations,
    proxies: BTreeSet<RegionId>,
    unallocated_shards: BTreeSet<ShardId>,
    remember_entities: bool,
}

impl CoordinatorState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_remember_entities(mut self, enabled: bool) -> Self {
        self.remember_entities = enabled;
        if !enabled {
            self.unallocated_shards.clear();
        }
        self
    }

    pub fn remember_entities(&self) -> bool {
        self.remember_entities
    }

    pub fn is_empty(&self) -> bool {
        self.allocations.is_empty() && self.proxies.is_empty()
    }

    pub fn allocations(&self) -> &ShardAllocations {
        &self.allocations
    }

    pub fn proxies(&self) -> &BTreeSet<RegionId> {
        &self.proxies
    }

    pub fn unallocated_shards(&self) -> &BTreeSet<ShardId> {
        &self.unallocated_shards
    }

    pub fn all_shards(&self) -> BTreeSet<ShardId> {
        self.allocations
            .shards()
            .cloned()
            .chain(self.unallocated_shards.iter().cloned())
            .collect()
    }

    pub fn shard_home(&self, shard: &ShardId) -> Option<&RegionId> {
        self.allocations.region_for_shard(shard)
    }

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
