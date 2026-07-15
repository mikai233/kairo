#![deny(missing_docs)]

//! Construction helper for a coordinator state and its matching local handoff
//! transport.
//!
//! This module supports explicit local coordinator assembly in tests and
//! low-level examples. Cluster-integrated applications normally initialize
//! entity kinds through [`crate::ClusterSharding`].

use crate::{
    CoordinatorEvent, CoordinatorState, HandoffRegionTarget, HandoffTransport, RegionId,
    ShardingError,
};

#[derive(Clone)]
/// Matched coordinator state and local region targets for handoff orchestration.
///
/// Construction applies the normal region-registration state transition for
/// every target, ensuring the state and transport contain the same region set.
pub struct ShardCoordinatorBootstrap<M>
where
    M: Send + 'static,
{
    state: CoordinatorState,
    handoff_transport: HandoffTransport<M>,
}

impl<M> ShardCoordinatorBootstrap<M>
where
    M: Send + 'static,
{
    /// Builds coordinator state and handoff transport from local region targets.
    ///
    /// Returns [`ShardingError::RegionAlreadyRegistered`] when two targets use
    /// the same region identifier. An empty iterator creates empty coordinator
    /// state and transport.
    pub fn local_regions(
        regions: impl IntoIterator<Item = HandoffRegionTarget<M>>,
    ) -> Result<Self, ShardingError> {
        let mut state = CoordinatorState::new();
        let mut handoff_transport = HandoffTransport::new();
        for region in regions {
            state.apply(CoordinatorEvent::ShardRegionRegistered {
                region: region.region().clone(),
            })?;
            handoff_transport.insert_target(region);
        }
        Ok(Self {
            state,
            handoff_transport,
        })
    }

    /// Returns the initialized coordinator state.
    pub fn state(&self) -> &CoordinatorState {
        &self.state
    }

    /// Returns the local handoff transport containing the registered targets.
    pub fn handoff_transport(&self) -> &HandoffTransport<M> {
        &self.handoff_transport
    }

    /// Consumes the helper and returns the matched state and handoff transport.
    pub fn into_parts(self) -> (CoordinatorState, HandoffTransport<M>) {
        (self.state, self.handoff_transport)
    }

    /// Iterates over the region identifiers registered in coordinator state.
    pub fn region_ids(&self) -> impl Iterator<Item = &RegionId> {
        self.state.allocations().regions()
    }
}
