#![deny(missing_docs)]
//! Deterministic shard-region routing, buffering, and handoff decisions.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::{
    BeginHandOffAck, GetShardHome, HandOff, HostShard, RegionId, ShardId, ShardStopped,
    ShardingEnvelope, ShardingError,
};

/// Routing decision for one sharding envelope entering a region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionRoutePlan<M> {
    /// Deliver directly to an already started local shard.
    DeliverLocal {
        /// Target shard.
        shard: ShardId,
        /// Business envelope to deliver.
        message: ShardingEnvelope<M>,
    },
    /// Forward directly to a known remote shard region.
    Forward {
        /// Target shard.
        shard: ShardId,
        /// Region currently owning the shard.
        region: RegionId,
        /// Business envelope to forward.
        message: ShardingEnvelope<M>,
    },
    /// Retain the envelope until a shard home or local shard is ready.
    Buffered {
        /// Target shard.
        shard: ShardId,
        /// Home request emitted only when this is the shard's first buffered envelope.
        request: Option<GetShardHome>,
    },
    /// Reject the envelope while returning ownership to the caller.
    Dropped {
        /// Derived shard, or `None` when the shard ID was empty.
        shard: Option<ShardId>,
        /// Capacity or identity failure that caused the drop.
        reason: RegionDropReason,
        /// Undelivered business envelope.
        message: ShardingEnvelope<M>,
    },
}

/// Reason a region drops a routed envelope before delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionDropReason {
    /// The shard derivation result was empty.
    EmptyShardId,
    /// The region-wide buffer reached its configured capacity.
    BufferFull,
}

/// Region decision for a coordinator's host-shard command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostShardPlan<M> {
    /// Create the local shard before acknowledging the coordinator.
    StartLocalShard {
        /// Shard to create.
        shard: ShardId,
        /// Stable host command being fulfilled.
        command: HostShard,
    },
    /// A local shard already exists, so acknowledge and replay queued envelopes.
    AlreadyStarted {
        /// Existing local shard.
        shard: ShardId,
        /// Stable acknowledgement for the coordinator.
        started: crate::ShardStarted,
        /// Buffered envelopes drained in arrival order.
        buffered: Vec<ShardingEnvelope<M>>,
    },
    /// Reject new ownership while this region is gracefully shutting down.
    IgnoredGracefulShutdown {
        /// Shard the coordinator attempted to host.
        shard: ShardId,
    },
}

/// Region decision after learning a shard's current home.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardHomePlan<M> {
    /// The home is this region and its local shard must be created.
    StartLocalShard {
        /// Shard to create.
        shard: ShardId,
        /// Local host command.
        command: HostShard,
    },
    /// The home is an existing local shard; replay its queue.
    DeliverLocal {
        /// Existing local shard.
        shard: ShardId,
        /// Buffered envelopes drained in arrival order.
        buffered: Vec<ShardingEnvelope<M>>,
    },
    /// The home is remote; forward the complete local queue to that region.
    Forward {
        /// Resolved shard.
        shard: ShardId,
        /// Remote owner region.
        region: RegionId,
        /// Buffered envelopes drained in arrival order.
        buffered: Vec<ShardingEnvelope<M>>,
    },
}

/// Result of recording that a local shard finished starting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardStartedPlan<M> {
    /// Stable acknowledgement for the coordinator.
    pub started: crate::ShardStarted,
    /// Buffered envelopes drained in arrival order for local replay.
    pub buffered: Vec<ShardingEnvelope<M>>,
}

/// Region response to the first phase of shard handoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginHandOffPlan {
    /// Invalidate the cached home and acknowledge the coordinator.
    Ack {
        /// Shard whose home was invalidated.
        shard: ShardId,
        /// Stable begin-handoff acknowledgement.
        ack: BeginHandOffAck,
    },
    /// Ignore handoff because the cluster is already preparing to terminate.
    IgnoredPreparingForShutdown {
        /// Shard whose command was ignored.
        shard: ShardId,
    },
}

/// Region response to the owner-stop phase of shard handoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandOffPlan {
    /// Forward handoff to the existing local shard.
    ForwardToLocalShard {
        /// Local shard being stopped.
        shard: ShardId,
        /// Stable handoff command for the shard actor.
        command: HandOff,
        /// Envelopes discarded to prevent ordering inversion across the move.
        dropped_buffered: usize,
    },
    /// Reply immediately because no local shard needs stopping.
    ReplyShardStopped {
        /// Shard already absent from this region.
        shard: ShardId,
        /// Stable completion acknowledgement.
        stopped: ShardStopped,
        /// Envelopes discarded to prevent ordering inversion across the move.
        dropped_buffered: usize,
    },
}

/// Synchronous state machine for one shard region's routing view.
///
/// The runtime maintains bidirectional shard-home indexes, local shard
/// lifecycle sets, and a region-wide bounded buffer with FIFO ordering inside
/// each shard. It produces plans; actor spawning, transport sends, replies,
/// and dead-letter publication remain the owning actor's responsibility.
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
    /// Creates an empty region runtime with a global envelope buffer capacity.
    ///
    /// A capacity of zero makes every envelope requiring buffering immediately
    /// produce [`RegionDropReason::BufferFull`].
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

    /// Returns this region's stable coordinator identity.
    pub fn self_region(&self) -> &RegionId {
        &self.self_region
    }

    /// Returns the cached owner for a shard.
    pub fn region_for_shard(&self, shard: &ShardId) -> Option<&RegionId> {
        self.region_by_shard.get(shard)
    }

    /// Returns shards whose local actors have completed startup.
    pub fn local_shards(&self) -> &BTreeSet<ShardId> {
        &self.local_shards
    }

    /// Returns shards awaiting completion of local actor startup.
    pub fn starting_shards(&self) -> &BTreeSet<ShardId> {
        &self.starting_shards
    }

    /// Returns local shards currently executing owner handoff.
    pub fn handing_off_shards(&self) -> &BTreeSet<ShardId> {
        &self.handing_off_shards
    }

    /// Returns the number of envelopes buffered for one shard.
    pub fn buffered_count(&self, shard: &ShardId) -> usize {
        self.shard_buffers.get(shard).map_or(0, VecDeque::len)
    }

    /// Returns the total number of envelopes buffered across all shards.
    pub fn total_buffered_count(&self) -> usize {
        self.shard_buffers.values().map(VecDeque::len).sum()
    }

    /// Starts or clears graceful region shutdown.
    ///
    /// While set, new host-shard commands are ignored.
    pub fn set_graceful_shutdown_in_progress(&mut self, in_progress: bool) {
        self.graceful_shutdown_in_progress = in_progress;
    }

    /// Records whether cluster-wide shutdown preparation has begun.
    ///
    /// During preparation, begin-handoff commands are ignored because the
    /// region will terminate with the cluster instead of rebalancing.
    pub fn set_preparing_for_shutdown(&mut self, preparing: bool) {
        self.preparing_for_shutdown = preparing;
    }

    /// Returns whether cluster-wide shutdown preparation is active.
    pub fn preparing_for_shutdown(&self) -> bool {
        self.preparing_for_shutdown
    }

    /// Returns whether graceful shutdown can stop the region now.
    ///
    /// Completion requires shutdown to be active, no started local shards, and
    /// no buffered envelopes.
    pub fn graceful_shutdown_complete(&self) -> bool {
        self.graceful_shutdown_in_progress
            && self.local_shards.is_empty()
            && self.total_buffered_count() == 0
    }

    /// Returns whether graceful region shutdown is active.
    pub fn graceful_shutdown_in_progress(&self) -> bool {
        self.graceful_shutdown_in_progress
    }

    /// Plans delivery, forwarding, buffering, or dropping for one envelope.
    ///
    /// If a local shard has an existing queue, new envelopes join that queue
    /// instead of bypassing it. An unknown shard requests its home only when
    /// creating the first buffered entry; retry scheduling belongs to the actor.
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

    /// Records coordinator ownership by this region and plans local shard startup.
    ///
    /// An already started local shard drains any buffered envelopes. Graceful
    /// shutdown rejects the command without changing the cached ownership view.
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

    /// Records a coordinator shard-home reply and drains its local queue.
    ///
    /// A reply is rejected while local owner handoff is active. Moving a shard
    /// directly from an already known local home to a remote region is also an
    /// inconsistency; the coordinator must complete handoff first.
    pub fn record_shard_home(
        &mut self,
        shard: impl Into<ShardId>,
        region: impl Into<RegionId>,
    ) -> Result<ShardHomePlan<M>, ShardingError> {
        let shard = shard.into();
        let region = region.into();
        if self.handing_off_shards.contains(&shard) {
            return Err(ShardingError::ShardHomeDuringHandOff { shard, region });
        }
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

    /// Marks local shard startup complete and drains its buffered envelopes.
    ///
    /// The caller must invoke this only for a shard actor it has successfully
    /// started. The resulting cache assignment is always this region.
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

    /// Invalidates a shard-home cache entry and plans begin-handoff acknowledgement.
    ///
    /// Envelopes arriving afterward are buffered under an unknown home until
    /// the owner-stop phase drops them to avoid cross-region reordering.
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

    /// Plans owner shard shutdown and drops envelopes buffered during handoff.
    ///
    /// Dropping the buffer matches Pekko's ordering rule: messages forwarded
    /// from another region between begin-handoff and handoff must not overtake
    /// messages delivered through the new owner after reallocation.
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

    /// Removes all local lifecycle and cached-home state for a stopped shard.
    pub fn mark_shard_stopped(&mut self, shard: &ShardId) {
        self.local_shards.remove(shard);
        self.starting_shards.remove(shard);
        self.handing_off_shards.remove(shard);
        self.unassign_shard(shard);
    }

    /// Removes every cached shard home associated with a stopped remote region.
    ///
    /// Returned shard IDs are sorted. Existing buffers are retained so the
    /// actor can request new homes without losing accepted envelopes.
    pub fn mark_region_stopped(&mut self, region: &RegionId) -> Vec<ShardId> {
        let Some(shards) = self.shards_by_region.remove(region) else {
            return Vec::new();
        };
        for shard in &shards {
            self.region_by_shard.remove(shard);
        }
        shards.into_iter().collect()
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
