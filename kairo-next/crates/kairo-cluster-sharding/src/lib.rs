//! Cluster sharding API surface and protocols.
//!
//! Sharding routes by entity id while keeping business messages typed and
//! free of embedded routing metadata. A caller can send directly through a
//! region `ActorRef<ShardingEnvelope<M>>`, or bind one entity id into an
//! [`EntityRef<M>`]. Sending through an entity ref wraps the business `M` in a
//! [`ShardingEnvelope<M>`] before it reaches the region. The entity actor
//! itself receives `M`, not the envelope.
//!
//! This follows Pekko's observable typed-sharding model without making
//! [`EntityRef`] a normal watchable actor ref. Entity lifetime is owned by the
//! shard and region; lifecycle observation belongs at those runtime
//! boundaries, not on the logical entity handle.
//!
//! Shard ids are computed from the entity id with a documented fixed 64-bit
//! FNV-1a hash through [`stable_hash_entity_id`] and [`shard_id_for`]. The
//! default shard count is [`DEFAULT_SHARD_COUNT`]. The hash deliberately avoids
//! Rust's `DefaultHasher`, type names, enum discriminants, and memory layout so
//! independently started nodes assign the same entity id to the same shard.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use kairo_actor::{Actor, ActorResult, ActorSystem, Context, Props};
//! use kairo_cluster_sharding::{
//!     EntityRef, ShardingEnvelope, default_shard_id_for, shard_id_for,
//! };
//!
//! struct RegionProbe;
//!
//! impl Actor for RegionProbe {
//!     type Msg = ShardingEnvelope<String>;
//!
//!     fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
//!         let (entity_id, business_message) = msg.into_parts();
//!         assert_eq!(entity_id, "account-7");
//!         assert_eq!(business_message, "credit".to_string());
//!         Ok(())
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let system = ActorSystem::builder("sharding-docs").build()?;
//! let region = system.spawn("region", Props::new(|| RegionProbe))?;
//! let account = EntityRef::new("account-7", region);
//!
//! account.tell("credit".to_string())?;
//! assert_eq!(account.entity_id(), "account-7");
//!
//! assert_eq!(shard_id_for("account-7", 100)?, default_shard_id_for("account-7"));
//! system.terminate(Duration::from_secs(1))?;
//! # Ok(())
//! # }
//! ```

mod allocation;
mod bootstrap;
mod codec;
mod coordinator;
mod coordinator_actor;
mod coordinator_discovery;
mod coordinator_handoff;
mod coordinator_remote_home;
mod coordinator_remote_regions;
mod coordinator_remote_registration;
mod coordinator_remote_reply;
mod coordinator_remote_target;
mod coordinator_runtime;
mod coordinator_store;
mod coordinator_system_inbound;
mod entity_factory;
mod entity_ref;
mod entity_router;
mod entity_shard_actor;
mod entity_type;
mod envelope;
mod errors;
mod handoff_transport;
mod handoff_worker;
mod hashing;
mod protocol;
mod region_actor;
mod region_coordinator_discovery;
mod region_discovery_subscriber;
mod region_home_requests;
mod region_protocol;
mod region_registration;
mod region_remote;
mod region_remote_coordinator;
mod region_remote_coordinator_transport;
mod region_runtime;
mod region_shards;
mod region_system_inbound;
mod region_transport;
mod remember;
mod remember_actor;
mod remember_ddata;
mod remember_shard_ddata;
mod shard_actor;
mod shard_loading;
mod shard_remember;
mod shard_runtime;
mod shard_store;

pub use allocation::{LeastShardAllocationStrategy, ShardAllocationStrategy, ShardAllocations};
pub use bootstrap::ShardCoordinatorBootstrap;
pub use codec::{
    BEGIN_HANDOFF_ACK_SERIALIZER_ID, BEGIN_HANDOFF_SERIALIZER_ID, BeginHandOffAckCodec,
    BeginHandOffCodec, GET_SHARD_HOME_SERIALIZER_ID, GetShardHomeCodec, HANDOFF_SERIALIZER_ID,
    HOST_SHARD_SERIALIZER_ID, HandOffCodec, HostShardCodec, REGISTER_ACK_SERIALIZER_ID,
    REGISTER_SERIALIZER_ID, ROUTED_SHARD_ENVELOPE_SERIALIZER_ID, RegisterAckCodec, RegisterCodec,
    RoutedShardEnvelopeCodec, SHARD_HOME_SERIALIZER_ID, SHARD_STARTED_SERIALIZER_ID,
    SHARD_STOPPED_SERIALIZER_ID, ShardHomeCodec, ShardStartedCodec, ShardStoppedCodec,
    register_sharding_protocol_codecs,
};
pub use coordinator::{CoordinatorEvent, CoordinatorState};
pub use coordinator_actor::{CoordinatorStateSnapshot, ShardCoordinatorActor, ShardCoordinatorMsg};
pub use coordinator_discovery::{
    CoordinatorDiscoveryChange, CoordinatorDiscoverySettings, CoordinatorDiscoveryState,
};
pub use coordinator_handoff::CoordinatorHandoff;
pub use coordinator_remote_home::{
    ShardCoordinatorRemoteHome, ShardCoordinatorRemoteHomeError, ShardCoordinatorRemoteHomeInbound,
    ShardCoordinatorRemoteHomeOutbound,
};
pub use coordinator_remote_regions::{CoordinatorRemoteRegions, remote_region_id};
pub use coordinator_remote_registration::{
    ShardCoordinatorRemoteRegistrationAck, ShardCoordinatorRemoteRegistrationError,
    ShardCoordinatorRemoteRegistrationInbound, ShardCoordinatorRemoteRegistrationOutbound,
};
pub use coordinator_remote_reply::{CoordinatorRemoteReplyError, CoordinatorRemoteReplyTarget};
pub use coordinator_remote_target::{
    DEFAULT_SHARD_COORDINATOR_REMOTE_PATH, ShardCoordinatorRemoteTarget,
    ShardCoordinatorRemoteTargetError, coordinator_recipient_for_node,
};
pub use coordinator_runtime::{
    CoordinatorRuntime, GetShardHomeIgnoreReason, GetShardHomePlan, RebalanceCompletionPlan,
    RebalancePlan, RebalanceSkipReason, ShardRebalancePlan,
};
pub use coordinator_system_inbound::{
    ShardCoordinatorSystemInbound, ShardCoordinatorSystemInboundError,
    is_shard_coordinator_system_manifest,
};
pub use entity_factory::EntityActorFactory;
pub use entity_ref::EntityRef;
pub use entity_router::ShardingEnvelopeRouter;
pub use entity_shard_actor::EntityShardActor;
pub use entity_type::EntityTypeKey;
pub use envelope::ShardingEnvelope;
pub use errors::ShardingError;
pub use handoff_transport::{
    HandoffDeliveryFailure, HandoffDeliveryReport, HandoffDeliveryTarget, HandoffRegionTarget,
    HandoffTransport,
};
pub use handoff_worker::{
    HandoffWorkerActor, HandoffWorkerDone, HandoffWorkerMsg, HandoffWorkerPhase,
    HandoffWorkerSnapshot,
};
pub use hashing::{DEFAULT_SHARD_COUNT, default_shard_id_for, shard_id_for, stable_hash_entity_id};
pub use protocol::{
    BeginHandOff, BeginHandOffAck, GetShardHome, HandOff, HostShard, Register, RegisterAck,
    RoutedShardEnvelope, ShardHome, ShardStarted, ShardStopped,
};
pub use region_actor::ShardRegionActor;
pub use region_coordinator_discovery::{
    RegionCoordinatorDiscovery, RegionCoordinatorDiscoveryConfig, RegionCoordinatorDiscoveryPlan,
};
pub use region_discovery_subscriber::{
    ShardRegionDiscoverySubscriber, ShardRegionDiscoverySubscriberMsg,
    ShardRegionDiscoverySubscriberSnapshot,
};
pub use region_protocol::{
    RegionBufferedReplayPlan, RegionLocalHandOffCompletionFailure,
    RegionLocalHandOffCompletionPlan, RegionLocalHandOffPlan, RegionLocalRoutePlan, ShardRegionMsg,
    ShardRegionSnapshot,
};
pub use region_registration::{RegionRegistrationConfig, RegionRegistrationStatus};
pub use region_remote::{
    DEFAULT_SHARD_REGION_REMOTE_PATH, ShardRegionRemoteControlOutbound, ShardRegionRemoteError,
    ShardRegionRemoteInbound, ShardRegionRemoteOutbound,
};
pub use region_remote_coordinator::{
    RegionRemoteCoordinator, RegionRemoteRegistrationPlan, RegionRemoteShardHomePlan,
    region_id_from_wire_ref, shard_home_plan_from_remote,
};
pub use region_remote_coordinator_transport::{
    RegionRemoteCoordinatorTransport, RegionRemoteCoordinatorTransportError,
};
pub use region_runtime::{
    BeginHandOffPlan, HandOffPlan, HostShardPlan, RegionDropReason, RegionRoutePlan, ShardHomePlan,
    ShardRegionRuntime, ShardStartedPlan,
};
pub use region_system_inbound::{
    ShardRegionSystemInbound, ShardRegionSystemInboundError, is_shard_region_system_manifest,
};
pub use region_transport::{RegionRouteDelivery, RegionRouteTarget, RegionRouteTransport};
pub use remember::{
    REMEMBER_ENTITY_SHARD_KEY_COUNT, RememberCoordinatorStoreState, RememberCoordinatorUpdateDone,
    RememberShardStoreState, RememberShardUpdate, RememberShardUpdateDone, RememberedShards,
    remember_entity_key_index, remember_entity_key_index_for, remember_entity_shard_key,
};
pub use remember_actor::{
    RememberCoordinatorStoreActor, RememberCoordinatorStoreMsg, RememberCoordinatorStoreSnapshot,
    RememberShardStoreActor, RememberShardStoreMsg, RememberShardStoreSnapshot, RememberedEntities,
};
pub use remember_ddata::{
    RememberCoordinatorDDataStoreActor, RememberCoordinatorDDataStoreMsg,
    RememberCoordinatorDDataStoreSnapshot, remember_coordinator_shards_key,
};
pub use remember_shard_ddata::{
    RememberShardDDataStoreActor, RememberShardDDataStoreMsg, RememberShardDDataStoreSnapshot,
    remember_entity_shard_replicator_key,
};
pub use shard_actor::{ShardActor, ShardMsg, ShardSnapshot};
pub use shard_runtime::{
    EntityDelivery, EntityTerminatedPlan, PassivateIgnoreReason, PassivatePlan,
    RememberUpdateDonePlan, RememberedEntitiesPlan, ShardDeliverPlan, ShardDropReason,
    ShardEntityState, ShardHandOffPlan, ShardRuntime,
};

pub type EntityId = String;
pub type RegionId = String;
pub type ShardId = String;

#[cfg(test)]
mod tests;
