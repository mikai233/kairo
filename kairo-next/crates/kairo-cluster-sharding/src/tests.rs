use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, Address, Context, Props, Recipient,
};
use kairo_cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, CurrentClusterState, Gossip, Member,
    MemberStatus, UniqueAddress,
};
use kairo_distributed_data::{GSet, ORSet, ReplicaId, ReplicatorActor};
use kairo_serialization::{
    ActorRefWireData, MessageCodec, Registry, RemoteEnvelope, RemoteMessage, SerializationRegistry,
    WireReader, WireWriter,
};

use crate::{
    BeginHandOffPlan, CoordinatorDiscoverySettings, CoordinatorEvent, CoordinatorRuntime,
    CoordinatorState, CoordinatorStateSnapshot, DEFAULT_SHARD_COUNT, EntityActorFactory,
    EntityDelivery, EntityRef, EntityShardActor, GetShardHome, GetShardHomeIgnoreReason,
    GetShardHomePlan, GracefulShutdownReq, HandOff, HandOffPlan, HandoffDeliveryFailure,
    HandoffDeliveryTarget, HandoffRegionTarget, HandoffTransport, HandoffWorkerActor,
    HandoffWorkerDone, HandoffWorkerMsg, HostShard, HostShardPlan, LeastShardAllocationStrategy,
    PassivatePlan, RebalanceCompletionPlan, RebalancePlan, RebalanceSkipReason,
    RegionBufferedReplayPlan, RegionCoordinatorDiscoveryConfig, RegionDropReason,
    RegionLocalHandOffCompletionPlan, RegionLocalHandOffPlan, RegionLocalRoutePlan,
    RegionRegistrationConfig, RegionRegistrationStatus, RegionRemoteCoordinatorTransport,
    RegionRouteDelivery, RegionRoutePlan, RegionRouteTarget, RegionRouteTransport,
    RegionShutdownPlan, RegionStopped, Register, RememberCoordinatorDDataStoreActor,
    RememberCoordinatorDDataStoreMsg, RememberCoordinatorDDataStoreSnapshot,
    RememberCoordinatorStoreActor, RememberCoordinatorStoreMsg, RememberCoordinatorStoreSnapshot,
    RememberCoordinatorStoreState, RememberShardDDataStoreActor, RememberShardDDataStoreMsg,
    RememberShardDDataStoreSnapshot, RememberShardStoreActor, RememberShardStoreMsg,
    RememberShardStoreSnapshot, RememberShardStoreState, RememberShardUpdate,
    RememberUpdateDonePlan, RememberedEntities, RememberedEntitiesPlan, ShardActor,
    ShardAllocationStrategy, ShardAllocations, ShardCoordinatorActor, ShardCoordinatorBootstrap,
    ShardCoordinatorMsg, ShardCoordinatorRemoteHome, ShardCoordinatorRemoteRegistrationAck,
    ShardCoordinatorRemoteTarget, ShardDeliverPlan, ShardDropReason, ShardEntityState,
    ShardHandOffPlan, ShardHomePlan, ShardMsg, ShardRebalancePlan, ShardRegionActor,
    ShardRegionDiscoverySubscriber, ShardRegionDiscoverySubscriberMsg,
    ShardRegionDiscoverySubscriberSnapshot, ShardRegionMsg, ShardRegionRemoteInbound,
    ShardRegionRemoteOutbound, ShardRegionRuntime, ShardRegionSnapshot, ShardRuntime,
    ShardSnapshot, ShardStarted, ShardStartedPlan, ShardStopped, ShardingEnvelope,
    ShardingEnvelopeRouter, ShardingError, default_shard_id_for, register_sharding_protocol_codecs,
    remember_coordinator_shards_key, remember_entity_key_index, remember_entity_key_index_for,
    remember_entity_shard_key, remember_entity_shard_replicator_key, shard_id_for,
    stable_hash_entity_id,
};

mod allocation;
mod coordinator_actor;
mod coordinator_runtime;
mod coordinator_state;
mod entity_routing;
mod entity_shard_actor;
mod handoff_orchestration;
mod handoff_transport;
mod region_actor_handoff;
mod region_actor_local;
mod region_discovery_subscriber;
mod region_registration;
mod region_remote_coordinator_actor;
mod region_route_resolution;
mod region_runtime;
mod remember;
mod shard_actor;
mod shard_remember_runtime;
mod shard_runtime;

fn coordinator_runtime_with_regions<const N: usize>(regions: [&str; N]) -> CoordinatorRuntime {
    let mut state = CoordinatorState::new();
    for region in regions {
        state
            .apply(CoordinatorEvent::ShardRegionRegistered {
                region: region.to_string(),
            })
            .unwrap();
    }
    CoordinatorRuntime::new(state)
}

struct FixedRebalanceStrategy {
    shards: BTreeSet<String>,
}

impl FixedRebalanceStrategy {
    fn new<const N: usize>(shards: [&str; N]) -> Self {
        Self {
            shards: shards.into_iter().map(str::to_string).collect(),
        }
    }
}

impl ShardAllocationStrategy for FixedRebalanceStrategy {
    fn allocate_shard(
        &self,
        _requester: &String,
        _shard: &String,
        _current: &ShardAllocations,
    ) -> Result<String, ShardingError> {
        Err(ShardingError::NoShardRegions)
    }

    fn rebalance(
        &self,
        _current: &ShardAllocations,
        _in_progress: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>, ShardingError> {
        Ok(self.shards.clone())
    }
}

struct RebalanceThenAllocateStrategy {
    rebalance_shards: BTreeSet<String>,
    allocation_region: String,
}

impl RebalanceThenAllocateStrategy {
    fn new<const N: usize>(rebalance_shards: [&str; N], allocation_region: &str) -> Self {
        Self {
            rebalance_shards: rebalance_shards.into_iter().map(str::to_string).collect(),
            allocation_region: allocation_region.to_string(),
        }
    }
}

impl ShardAllocationStrategy for RebalanceThenAllocateStrategy {
    fn allocate_shard(
        &self,
        _requester: &String,
        _shard: &String,
        _current: &ShardAllocations,
    ) -> Result<String, ShardingError> {
        Ok(self.allocation_region.clone())
    }

    fn rebalance(
        &self,
        _current: &ShardAllocations,
        _in_progress: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>, ShardingError> {
        Ok(self.rebalance_shards.clone())
    }
}

struct RegionProbe {
    observed: mpsc::Sender<(String, &'static str)>,
}

struct RecordingEntity {
    entity_id: String,
    observed: mpsc::Sender<(String, String)>,
}

impl Actor for RecordingEntity {
    type Msg = String;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.observed
            .send((self.entity_id.clone(), msg.clone()))
            .map_err(|error| ActorError::Message(error.to_string()))?;
        if msg == "stop" {
            ctx.stop(ctx.myself())?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteRouteMessage(String);

impl RemoteMessage for RemoteRouteMessage {
    const MANIFEST: &'static str = "kairo.sharding.test.remote-route";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy)]
struct RemoteRouteMessageCodec;

impl MessageCodec<RemoteRouteMessage> for RemoteRouteMessageCodec {
    fn serializer_id(&self) -> u32 {
        49_001
    }

    fn encode(&self, message: &RemoteRouteMessage) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.0)?;
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<RemoteRouteMessage> {
        assert_eq!(version, RemoteRouteMessage::VERSION);
        let mut reader = WireReader::new(&payload);
        Ok(RemoteRouteMessage(reader.read_string()?))
    }
}

struct RecordingRemoteEntity {
    entity_id: String,
    observed: mpsc::Sender<(String, String)>,
}

impl Actor for RecordingRemoteEntity {
    type Msg = RemoteRouteMessage;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.observed
            .send((self.entity_id.clone(), msg.0))
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

fn remote_node(system: &str, host: &str, port: u16) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", system, Some(host.to_string()), Some(port)),
        1,
    )
}

fn remote_unique_node(system: &str, host: &str, port: u16, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", system, Some(host.to_string()), Some(port)),
        uid,
    )
}

fn cluster_member(
    unique_address: UniqueAddress,
    status: MemberStatus,
    roles: impl IntoIterator<Item = &'static str>,
    up_number: u64,
) -> Member {
    Member::new(
        unique_address,
        roles.into_iter().map(ToString::to_string).collect(),
    )
    .with_status(status)
    .with_up_number(up_number)
}

fn cluster_state(members: Vec<Member>) -> CurrentClusterState {
    CurrentClusterState {
        members,
        unreachable: Vec::new(),
        seen_by: HashSet::new(),
        leader: None,
        role_leaders: HashMap::new(),
        member_tombstones: HashSet::new(),
    }
}

fn wait_for_local_shard(
    kit: &kairo_testkit::ActorSystemTestKit,
    region: &ActorRef<ShardRegionMsg<String>>,
    shard: &str,
) -> ActorRef<ShardMsg<String>> {
    let reply = kit
        .create_probe::<Option<ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    for _ in 0..20 {
        region
            .tell(ShardRegionMsg::GetLocalShard {
                shard: shard.to_string(),
                reply_to: reply.actor_ref(),
            })
            .unwrap();
        if let Some(shard_ref) = reply.expect_msg(Duration::from_millis(500)).unwrap() {
            return shard_ref;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for local shard `{shard}`");
}

impl Actor for RegionProbe {
    type Msg = ShardingEnvelope<&'static str>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        let (entity_id, message) = msg.into_parts();
        self.observed
            .send((entity_id, message))
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}
