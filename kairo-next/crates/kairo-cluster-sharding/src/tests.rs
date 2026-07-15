use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, Address, Context, Props, Recipient,
};
use kairo_cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, CurrentClusterState, Gossip, Member,
    MemberStatus, UniqueAddress,
};
use kairo_distributed_data::{
    DeltaPropagationLoop, DeltaPropagationTarget, DeltaPropagationTickReport,
    DeltaPropagationTransport, DeltaReplicatedData, GSet, GSetStringDeltaCodec, ORSet,
    ORSetStringDeltaCodec, ReplicaId, ReplicatedDelta, ReplicatorActor, ReplicatorDeltaPropagation,
};
use kairo_serialization::{
    ActorRefWireData, MessageCodec, Registry, RemoteEnvelope, RemoteMessage, SerializationRegistry,
    WireReader, WireWriter,
};

use crate::{
    BeginHandOff, BeginHandOffAck, BeginHandOffPlan, CoordinatorDiscoverySettings,
    CoordinatorEvent, CoordinatorRemoteReplyTarget, CoordinatorRuntime, CoordinatorState,
    CoordinatorStateSnapshot, DEFAULT_SHARD_COUNT, EntityActorFactory, EntityDelivery,
    EntityMessageExtractor, EntityMessageExtractorRouter, EntityRef, EntityShardActor,
    ExtractedEntityMessage, GetShardHome, GetShardHomeIgnoreReason, GetShardHomePlan,
    GracefulShutdownReq, HandOff, HandOffPlan, HandoffDeliveryFailure, HandoffDeliveryTarget,
    HandoffRegionTarget, HandoffTransport, HandoffWorkerActor, HandoffWorkerDone, HandoffWorkerMsg,
    HostShard, HostShardPlan, LeastShardAllocationStrategy, MovedRememberedEntitiesPlan,
    PassivatePlan, RebalanceCompletionPlan, RebalancePlan, RebalanceSkipReason,
    RegionBufferedReplayPlan, RegionCoordinatorDiscoveryConfig, RegionDropReason,
    RegionLocalHandOffCompletionFailure, RegionLocalHandOffCompletionPlan, RegionLocalHandOffPlan,
    RegionLocalRoutePlan, RegionRegistrationConfig, RegionRegistrationStatus, RegionRouteDelivery,
    RegionRoutePlan, RegionRouteTarget, RegionRouteTransport, RegionShutdownPlan, RegionStopped,
    Register, RegisterAck, RememberCoordinatorDDataStoreActor, RememberCoordinatorDDataStoreMsg,
    RememberCoordinatorDDataStoreSnapshot, RememberCoordinatorORSetDDataStoreActor,
    RememberCoordinatorStoreActor, RememberCoordinatorStoreMsg, RememberCoordinatorStoreSnapshot,
    RememberCoordinatorStoreState, RememberShardDDataStoreActor, RememberShardDDataStoreMsg,
    RememberShardDDataStoreSnapshot, RememberShardStoreActor, RememberShardStoreMsg,
    RememberShardStoreSnapshot, RememberShardStoreState, RememberShardUpdate,
    RememberShardUpdateDone, RememberUpdateDonePlan, RememberedEntities, RememberedEntitiesPlan,
    ShardActor, ShardAllocationStrategy, ShardAllocations, ShardCoordinatorActor,
    ShardCoordinatorBootstrap, ShardCoordinatorMsg, ShardCoordinatorRemoteHome,
    ShardCoordinatorRemoteRegistrationAck, ShardCoordinatorRemoteTarget,
    ShardCoordinatorSystemInbound, ShardDeliverPlan, ShardDropReason, ShardEntityState,
    ShardHandOffPlan, ShardHomePlan, ShardMsg, ShardRebalancePlan, ShardRegionActor,
    ShardRegionBootstrap, ShardRegionBootstrapConfig, ShardRegionDiscoverySubscriber,
    ShardRegionDiscoverySubscriberMsg, ShardRegionDiscoverySubscriberSnapshot,
    ShardRegionLocalRememberStoreBootstrapConfig, ShardRegionMsg,
    ShardRegionRememberStoreBootstrapConfig, ShardRegionRemoteInbound, ShardRegionRemoteOutbound,
    ShardRegionRuntime, ShardRegionSnapshot, ShardRuntime, ShardSnapshot, ShardStarted,
    ShardStartedPlan, ShardStopped, ShardingEnvelope, ShardingEnvelopeRouter, ShardingError,
    default_shard_id_for, register_sharding_protocol_codecs, remember_coordinator_shards_key,
    remember_entity_key_index, remember_entity_key_index_for, remember_entity_shard_key,
    remember_entity_shard_replicator_key, remote_region_id, shard_id_for, stable_hash_entity_id,
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

#[test]
fn sharding_sources_do_not_use_rust_default_hasher() -> Result<(), Box<dyn std::error::Error>> {
    let crate_src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let forbidden_terms = [
        "DefaultHasher",
        "std::collections::hash_map::DefaultHasher",
        "collections::hash_map::DefaultHasher",
    ];

    let mut files = Vec::new();
    collect_active_rs_files(&crate_src, &mut files)?;

    for file in files {
        let source = std::fs::read_to_string(&file)?.replace("\r\n", "\n");
        for (line_index, line) in source.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }

            for term in forbidden_terms {
                assert!(
                    !line.contains(term),
                    "{}:{} must not use `{term}` for shard routing; use stable_hash_entity_id/shard_id_for instead",
                    file.display(),
                    line_index + 1
                );
            }
        }
    }

    Ok(())
}

fn collect_active_rs_files(
    directory: &std::path::Path,
    files: &mut Vec<std::path::PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = path.file_name().and_then(|name| name.to_str());
        if path.is_dir() {
            if file_name == Some("tests") {
                continue;
            }
            collect_active_rs_files(&path, files)?;
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs")
            && file_name != Some("tests.rs")
        {
            files.push(path);
        }
    }

    Ok(())
}

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
    kairo_testkit::await_assert(
        Duration::from_millis(10_200),
        Duration::from_millis(10),
        || -> Result<ActorRef<ShardMsg<String>>, String> {
            region
                .tell(ShardRegionMsg::GetLocalShard {
                    shard: shard.to_string(),
                    reply_to: reply.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            match reply.expect_msg(Duration::from_millis(500)) {
                Ok(Some(shard_ref)) => Ok(shard_ref),
                Ok(None) => Err(format!("local shard `{shard}` is not available yet")),
                Err(error) => Err(format!(
                    "timed out waiting for local shard `{shard}` response: {error}"
                )),
            }
        },
    )
    .unwrap()
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
