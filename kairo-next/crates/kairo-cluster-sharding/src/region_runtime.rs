use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::{
    BeginHandOffAck, GetShardHome, HandOff, HostShard, RegionId, ShardId, ShardStopped,
    ShardingEnvelope, ShardingError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionRoutePlan<M> {
    DeliverLocal {
        shard: ShardId,
        message: ShardingEnvelope<M>,
    },
    Forward {
        shard: ShardId,
        region: RegionId,
        message: ShardingEnvelope<M>,
    },
    Buffered {
        shard: ShardId,
        request: Option<GetShardHome>,
    },
    Dropped {
        shard: Option<ShardId>,
        reason: RegionDropReason,
        message: ShardingEnvelope<M>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionDropReason {
    EmptyShardId,
    BufferFull,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostShardPlan<M> {
    StartLocalShard {
        shard: ShardId,
        command: HostShard,
    },
    AlreadyStarted {
        shard: ShardId,
        started: crate::ShardStarted,
        buffered: Vec<ShardingEnvelope<M>>,
    },
    IgnoredGracefulShutdown {
        shard: ShardId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardHomePlan<M> {
    StartLocalShard {
        shard: ShardId,
        command: HostShard,
    },
    DeliverLocal {
        shard: ShardId,
        buffered: Vec<ShardingEnvelope<M>>,
    },
    Forward {
        shard: ShardId,
        region: RegionId,
        buffered: Vec<ShardingEnvelope<M>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardStartedPlan<M> {
    pub started: crate::ShardStarted,
    pub buffered: Vec<ShardingEnvelope<M>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginHandOffPlan {
    Ack {
        shard: ShardId,
        ack: BeginHandOffAck,
    },
    IgnoredPreparingForShutdown {
        shard: ShardId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandOffPlan {
    ForwardToLocalShard {
        shard: ShardId,
        command: HandOff,
        dropped_buffered: usize,
    },
    ReplyShardStopped {
        shard: ShardId,
        stopped: ShardStopped,
        dropped_buffered: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRegionRuntime<M> {
    self_region: RegionId,
    buffer_capacity: usize,
    region_by_shard: BTreeMap<ShardId, RegionId>,
    shards_by_region: BTreeMap<RegionId, BTreeSet<ShardId>>,
    local_shards: BTreeSet<ShardId>,
    starting_shards: BTreeSet<ShardId>,
    handing_off_shards: BTreeSet<ShardId>,
    shard_buffers: BTreeMap<ShardId, VecDeque<ShardingEnvelope<M>>>,
    graceful_shutdown_in_progress: bool,
    preparing_for_shutdown: bool,
}

impl<M> ShardRegionRuntime<M> {
    pub fn new(self_region: impl Into<RegionId>, buffer_capacity: usize) -> Self {
        Self {
            self_region: self_region.into(),
            buffer_capacity,
            region_by_shard: BTreeMap::new(),
            shards_by_region: BTreeMap::new(),
            local_shards: BTreeSet::new(),
            starting_shards: BTreeSet::new(),
            handing_off_shards: BTreeSet::new(),
            shard_buffers: BTreeMap::new(),
            graceful_shutdown_in_progress: false,
            preparing_for_shutdown: false,
        }
    }

    pub fn self_region(&self) -> &RegionId {
        &self.self_region
    }

    pub fn region_for_shard(&self, shard: &ShardId) -> Option<&RegionId> {
        self.region_by_shard.get(shard)
    }

    pub fn local_shards(&self) -> &BTreeSet<ShardId> {
        &self.local_shards
    }

    pub fn starting_shards(&self) -> &BTreeSet<ShardId> {
        &self.starting_shards
    }

    pub fn handing_off_shards(&self) -> &BTreeSet<ShardId> {
        &self.handing_off_shards
    }

    pub fn buffered_count(&self, shard: &ShardId) -> usize {
        self.shard_buffers.get(shard).map_or(0, VecDeque::len)
    }

    pub fn total_buffered_count(&self) -> usize {
        self.shard_buffers.values().map(VecDeque::len).sum()
    }

    pub fn set_graceful_shutdown_in_progress(&mut self, in_progress: bool) {
        self.graceful_shutdown_in_progress = in_progress;
    }

    pub fn set_preparing_for_shutdown(&mut self, preparing: bool) {
        self.preparing_for_shutdown = preparing;
    }

    pub fn preparing_for_shutdown(&self) -> bool {
        self.preparing_for_shutdown
    }

    pub fn graceful_shutdown_complete(&self) -> bool {
        self.graceful_shutdown_in_progress
            && self.local_shards.is_empty()
            && self.total_buffered_count() == 0
    }

    pub fn graceful_shutdown_in_progress(&self) -> bool {
        self.graceful_shutdown_in_progress
    }

    pub fn route(
        &mut self,
        shard: impl Into<ShardId>,
        message: ShardingEnvelope<M>,
    ) -> RegionRoutePlan<M> {
        let shard = shard.into();
        if shard.is_empty() {
            return RegionRoutePlan::Dropped {
                shard: None,
                reason: RegionDropReason::EmptyShardId,
                message,
            };
        }

        match self.region_by_shard.get(&shard).cloned() {
            Some(region) if region == self.self_region => {
                if self.local_shards.contains(&shard) && !self.shard_buffers.contains_key(&shard) {
                    RegionRoutePlan::DeliverLocal { shard, message }
                } else {
                    match self.buffer_message(shard.clone(), message) {
                        Ok(()) => RegionRoutePlan::Buffered {
                            shard,
                            request: None,
                        },
                        Err(message) => RegionRoutePlan::Dropped {
                            shard: Some(shard),
                            reason: RegionDropReason::BufferFull,
                            message,
                        },
                    }
                }
            }
            Some(region) => RegionRoutePlan::Forward {
                shard,
                region,
                message,
            },
            None => {
                let should_request = !self.shard_buffers.contains_key(&shard);
                match self.buffer_message(shard.clone(), message) {
                    Ok(()) => RegionRoutePlan::Buffered {
                        request: should_request.then(|| GetShardHome {
                            shard_id: shard.clone(),
                        }),
                        shard,
                    },
                    Err(message) => RegionRoutePlan::Dropped {
                        shard: Some(shard),
                        reason: RegionDropReason::BufferFull,
                        message,
                    },
                }
            }
        }
    }

    pub fn host_shard(&mut self, shard: impl Into<ShardId>) -> HostShardPlan<M> {
        let shard = shard.into();
        if self.graceful_shutdown_in_progress {
            return HostShardPlan::IgnoredGracefulShutdown { shard };
        }

        self.assign_shard(shard.clone(), self.self_region.clone());
        if self.local_shards.contains(&shard) {
            let buffered = self.drain_buffer(&shard);
            HostShardPlan::AlreadyStarted {
                started: crate::ShardStarted {
                    shard_id: shard.clone(),
                },
                buffered,
                shard,
            }
        } else {
            self.starting_shards.insert(shard.clone());
            HostShardPlan::StartLocalShard {
                command: HostShard {
                    shard_id: shard.clone(),
                },
                shard,
            }
        }
    }

    pub fn record_shard_home(
        &mut self,
        shard: impl Into<ShardId>,
        region: impl Into<RegionId>,
    ) -> Result<ShardHomePlan<M>, ShardingError> {
        let shard = shard.into();
        let region = region.into();
        if self
            .region_by_shard
            .get(&shard)
            .is_some_and(|current| current == &self.self_region && region != self.self_region)
        {
            return Err(ShardingError::InconsistentShardHome {
                shard,
                current_region: self.self_region.clone(),
                new_region: region,
            });
        }

        self.assign_shard(shard.clone(), region.clone());

        if region == self.self_region {
            if self.local_shards.contains(&shard) {
                let buffered = self.drain_buffer(&shard);
                Ok(ShardHomePlan::DeliverLocal { shard, buffered })
            } else {
                self.starting_shards.insert(shard.clone());
                Ok(ShardHomePlan::StartLocalShard {
                    command: HostShard {
                        shard_id: shard.clone(),
                    },
                    shard,
                })
            }
        } else {
            let buffered = self.drain_buffer(&shard);
            Ok(ShardHomePlan::Forward {
                shard,
                region,
                buffered,
            })
        }
    }

    pub fn mark_shard_started(&mut self, shard: impl Into<ShardId>) -> ShardStartedPlan<M> {
        let shard = shard.into();
        self.starting_shards.remove(&shard);
        self.local_shards.insert(shard.clone());
        self.assign_shard(shard.clone(), self.self_region.clone());
        let buffered = self.drain_buffer(&shard);
        ShardStartedPlan {
            started: crate::ShardStarted {
                shard_id: shard.clone(),
            },
            buffered,
        }
    }

    pub fn begin_handoff(&mut self, shard: impl Into<ShardId>) -> BeginHandOffPlan {
        let shard = shard.into();
        if self.preparing_for_shutdown {
            return BeginHandOffPlan::IgnoredPreparingForShutdown { shard };
        }

        self.unassign_shard(&shard);
        BeginHandOffPlan::Ack {
            ack: BeginHandOffAck {
                shard_id: shard.clone(),
            },
            shard,
        }
    }

    pub fn handoff(&mut self, shard: impl Into<ShardId>) -> HandOffPlan {
        let shard = shard.into();
        let dropped_buffered = self.drop_buffer(&shard);
        self.unassign_shard(&shard);

        if self.local_shards.contains(&shard) {
            self.handing_off_shards.insert(shard.clone());
            HandOffPlan::ForwardToLocalShard {
                command: HandOff {
                    shard_id: shard.clone(),
                },
                dropped_buffered,
                shard,
            }
        } else {
            HandOffPlan::ReplyShardStopped {
                stopped: ShardStopped {
                    shard_id: shard.clone(),
                },
                dropped_buffered,
                shard,
            }
        }
    }

    pub fn mark_shard_stopped(&mut self, shard: &ShardId) {
        self.local_shards.remove(shard);
        self.starting_shards.remove(shard);
        self.handing_off_shards.remove(shard);
        self.unassign_shard(shard);
    }

    fn buffer_message(
        &mut self,
        shard: ShardId,
        message: ShardingEnvelope<M>,
    ) -> Result<(), ShardingEnvelope<M>> {
        if self.total_buffered_count() >= self.buffer_capacity {
            return Err(message);
        }
        self.shard_buffers
            .entry(shard)
            .or_default()
            .push_back(message);
        Ok(())
    }

    fn drain_buffer(&mut self, shard: &ShardId) -> Vec<ShardingEnvelope<M>> {
        self.shard_buffers
            .remove(shard)
            .map(VecDeque::into_iter)
            .map(Iterator::collect)
            .unwrap_or_default()
    }

    fn drop_buffer(&mut self, shard: &ShardId) -> usize {
        self.shard_buffers
            .remove(shard)
            .map_or(0, |buffer| buffer.len())
    }

    fn assign_shard(&mut self, shard: ShardId, region: RegionId) {
        self.unassign_shard(&shard);
        self.region_by_shard.insert(shard.clone(), region.clone());
        self.shards_by_region
            .entry(region)
            .or_default()
            .insert(shard);
    }

    fn unassign_shard(&mut self, shard: &ShardId) {
        if let Some(region) = self.region_by_shard.remove(shard)
            && let Some(shards) = self.shards_by_region.get_mut(&region)
        {
            shards.remove(shard);
            if shards.is_empty() {
                self.shards_by_region.remove(&region);
            }
        }
    }
}
