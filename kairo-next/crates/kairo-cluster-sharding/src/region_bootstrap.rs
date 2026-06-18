use kairo_actor::{ActorError, ActorRef, ActorSystem};
use kairo_cluster::Cluster;

use crate::{
    RegionCoordinatorDiscoveryConfig, RegionId, ShardRegionActor, ShardRegionDiscoverySubscriber,
    ShardRegionDiscoverySubscriberMsg, ShardRegionMsg,
};

/// Configuration for spawning a shard region with cluster-driven coordinator discovery.
#[derive(Clone)]
pub struct ShardRegionBootstrapConfig<M>
where
    M: Send + 'static,
{
    region_name: String,
    discovery_subscriber_name: String,
    cluster: Cluster,
    self_region: RegionId,
    region_buffer_capacity: usize,
    shard_buffer_capacity: usize,
    discovery: RegionCoordinatorDiscoveryConfig<M>,
}

impl<M> ShardRegionBootstrapConfig<M>
where
    M: Send + 'static,
{
    /// Builds an explicit local-shard region bootstrap configuration.
    ///
    /// The helper owns only actor construction. Cluster membership remains
    /// gossip-driven through the supplied [`Cluster`] facade, and coordinator
    /// target selection remains in [`RegionCoordinatorDiscoveryConfig`].
    pub fn new(
        region_name: impl Into<String>,
        discovery_subscriber_name: impl Into<String>,
        cluster: Cluster,
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        discovery: RegionCoordinatorDiscoveryConfig<M>,
    ) -> Self {
        Self {
            region_name: region_name.into(),
            discovery_subscriber_name: discovery_subscriber_name.into(),
            cluster,
            self_region: self_region.into(),
            region_buffer_capacity,
            shard_buffer_capacity,
            discovery,
        }
    }
}

/// Actor refs created by a shard-region bootstrap.
///
/// The region receives normal [`ShardRegionMsg`] commands. The discovery
/// subscriber owns the cluster subscription that feeds coordinator discovery
/// snapshots and events into that region.
#[derive(Clone)]
pub struct ShardRegionBootstrap<M>
where
    M: Send + 'static,
{
    region: ActorRef<ShardRegionMsg<M>>,
    discovery_subscriber: ActorRef<ShardRegionDiscoverySubscriberMsg>,
}

impl<M> ShardRegionBootstrap<M>
where
    M: Send + 'static,
{
    /// Spawns a local-shard region and the discovery subscriber that drives it.
    ///
    /// If the subscriber cannot be spawned after the region was created, the
    /// helper requests region stop before returning the spawn error.
    pub fn spawn_local_shards_with_coordinator_discovery(
        system: &ActorSystem,
        config: ShardRegionBootstrapConfig<M>,
    ) -> Result<Self, ActorError> {
        let region = system.spawn(
            config.region_name,
            ShardRegionActor::props_with_local_shards_and_coordinator_discovery(
                config.self_region,
                config.region_buffer_capacity,
                config.shard_buffer_capacity,
                config.discovery,
            ),
        )?;
        let discovery_subscriber = match system.spawn(
            config.discovery_subscriber_name,
            ShardRegionDiscoverySubscriber::props(config.cluster, region.clone()),
        ) {
            Ok(subscriber) => subscriber,
            Err(error) => {
                system.stop(&region);
                return Err(error);
            }
        };

        Ok(Self {
            region,
            discovery_subscriber,
        })
    }

    /// Returns the spawned shard region actor ref.
    pub fn region(&self) -> ActorRef<ShardRegionMsg<M>> {
        self.region.clone()
    }

    /// Returns the spawned discovery-subscriber actor ref.
    pub fn discovery_subscriber(&self) -> ActorRef<ShardRegionDiscoverySubscriberMsg> {
        self.discovery_subscriber.clone()
    }

    /// Splits the bootstrap handle into its region and subscriber refs.
    pub fn into_parts(
        self,
    ) -> (
        ActorRef<ShardRegionMsg<M>>,
        ActorRef<ShardRegionDiscoverySubscriberMsg>,
    ) {
        (self.region, self.discovery_subscriber)
    }
}
