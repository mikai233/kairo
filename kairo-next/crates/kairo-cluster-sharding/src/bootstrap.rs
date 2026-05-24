use crate::{
    CoordinatorEvent, CoordinatorState, HandoffRegionTarget, HandoffTransport, RegionId,
    ShardingError,
};

#[derive(Clone)]
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

    pub fn state(&self) -> &CoordinatorState {
        &self.state
    }

    pub fn handoff_transport(&self) -> &HandoffTransport<M> {
        &self.handoff_transport
    }

    pub fn into_parts(self) -> (CoordinatorState, HandoffTransport<M>) {
        (self.state, self.handoff_transport)
    }

    pub fn region_ids(&self) -> impl Iterator<Item = &RegionId> {
        self.state.allocations().regions()
    }
}
