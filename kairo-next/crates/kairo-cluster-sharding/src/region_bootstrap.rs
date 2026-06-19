use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, ActorSystem, Props};
use kairo_cluster::Cluster;

use crate::{
    EntityId, RegionCoordinatorDiscoveryConfig, RegionId, RememberShardStoreMsg, ShardId,
    ShardRegionActor, ShardRegionDiscoverySubscriber, ShardRegionDiscoverySubscriberMsg,
    ShardRegionMsg,
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

/// Configuration for spawning a discovery-driven shard region with local
/// remember-entity stores owned by the region's shard children.
#[derive(Clone)]
pub struct ShardRegionLocalRememberStoreBootstrapConfig<M>
where
    M: Send + 'static,
{
    region_name: String,
    discovery_subscriber_name: String,
    cluster: Cluster,
    self_region: RegionId,
    type_name: String,
    region_buffer_capacity: usize,
    shard_buffer_capacity: usize,
    remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
    timeout: Duration,
    discovery: RegionCoordinatorDiscoveryConfig<M>,
}

impl<M> ShardRegionLocalRememberStoreBootstrapConfig<M>
where
    M: Send + 'static,
{
    /// Builds a local remember-store shard-region bootstrap configuration.
    pub fn new(
        base: ShardRegionBootstrapConfig<M>,
        type_name: impl Into<String>,
        remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
        timeout: Duration,
    ) -> Self {
        Self {
            region_name: base.region_name,
            discovery_subscriber_name: base.discovery_subscriber_name,
            cluster: base.cluster,
            self_region: base.self_region,
            type_name: type_name.into(),
            region_buffer_capacity: base.region_buffer_capacity,
            shard_buffer_capacity: base.shard_buffer_capacity,
            remembered_entities_by_shard,
            timeout,
            discovery: base.discovery,
        }
    }
}

/// Configuration for spawning a discovery-driven shard region backed by
/// externally supplied remember-entity store actors.
#[derive(Clone)]
pub struct ShardRegionRememberStoreBootstrapConfig<M>
where
    M: Send + 'static,
{
    region_name: String,
    discovery_subscriber_name: String,
    cluster: Cluster,
    self_region: RegionId,
    region_buffer_capacity: usize,
    shard_buffer_capacity: usize,
    remember_stores_by_shard: BTreeMap<ShardId, ActorRef<RememberShardStoreMsg>>,
    timeout: Duration,
    discovery: RegionCoordinatorDiscoveryConfig<M>,
}

impl<M> ShardRegionRememberStoreBootstrapConfig<M>
where
    M: Send + 'static,
{
    /// Builds a shared remember-store shard-region bootstrap configuration.
    pub fn new(
        base: ShardRegionBootstrapConfig<M>,
        remember_stores_by_shard: BTreeMap<ShardId, ActorRef<RememberShardStoreMsg>>,
        timeout: Duration,
    ) -> Self {
        Self {
            region_name: base.region_name,
            discovery_subscriber_name: base.discovery_subscriber_name,
            cluster: base.cluster,
            self_region: base.self_region,
            region_buffer_capacity: base.region_buffer_capacity,
            shard_buffer_capacity: base.shard_buffer_capacity,
            remember_stores_by_shard,
            timeout,
            discovery: base.discovery,
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

    /// Spawns a discovery-driven local-shard region whose shards own local
    /// remember-entity stores.
    ///
    /// If the subscriber cannot be spawned after the region was created, the
    /// helper requests region stop before returning the spawn error.
    pub fn spawn_local_remember_store_shards_with_coordinator_discovery(
        system: &ActorSystem,
        config: ShardRegionLocalRememberStoreBootstrapConfig<M>,
    ) -> Result<Self, ActorError> {
        let ShardRegionLocalRememberStoreBootstrapConfig {
            region_name,
            discovery_subscriber_name,
            cluster,
            self_region,
            type_name,
            region_buffer_capacity,
            shard_buffer_capacity,
            remembered_entities_by_shard,
            timeout,
            discovery,
        } = config;
        let region = system.spawn(region_name, {
            Props::new(move || {
                ShardRegionActor::new_with_local_remember_store_shards(
                    self_region.clone(),
                    type_name.clone(),
                    region_buffer_capacity,
                    shard_buffer_capacity,
                    remembered_entities_by_shard.clone(),
                    timeout,
                )
                .with_coordinator_discovery(discovery.clone())
            })
        })?;
        let discovery_subscriber = match system.spawn(
            discovery_subscriber_name,
            ShardRegionDiscoverySubscriber::props(cluster, region.clone()),
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

    /// Spawns a discovery-driven local-shard region backed by externally
    /// supplied remember-entity store actors.
    ///
    /// If the subscriber cannot be spawned after the region was created, the
    /// helper requests region stop before returning the spawn error.
    pub fn spawn_remember_store_shards_with_coordinator_discovery(
        system: &ActorSystem,
        config: ShardRegionRememberStoreBootstrapConfig<M>,
    ) -> Result<Self, ActorError> {
        let ShardRegionRememberStoreBootstrapConfig {
            region_name,
            discovery_subscriber_name,
            cluster,
            self_region,
            region_buffer_capacity,
            shard_buffer_capacity,
            remember_stores_by_shard,
            timeout,
            discovery,
        } = config;
        let region = system.spawn(region_name, {
            Props::new(move || {
                ShardRegionActor::new_with_remember_store_shards(
                    self_region.clone(),
                    region_buffer_capacity,
                    shard_buffer_capacity,
                    remember_stores_by_shard.clone(),
                    timeout,
                )
                .with_coordinator_discovery(discovery.clone())
            })
        })?;
        let discovery_subscriber = match system.spawn(
            discovery_subscriber_name,
            ShardRegionDiscoverySubscriber::props(cluster, region.clone()),
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
