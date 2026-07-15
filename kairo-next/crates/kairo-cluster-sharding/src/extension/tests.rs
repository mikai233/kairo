use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use kairo_cluster::{
    ClusterDaemonBootstrapSettings, ClusterGossipProcessSettings, ClusterMembershipMsg,
    DeadlineFailureDetectorSettings, Gossip, HeartbeatSenderSettings, MemberStatus,
    register_cluster_daemon, register_cluster_protocol_codecs,
};
use kairo_cluster_tools::{ClusterSingletonSettings, register_cluster_tools_protocol_codecs};
use kairo_distributed_data::{
    DistributedDataHandle, DistributedDataSettings, GetResponse, ORSet, ORSetStringCodec,
    ORSetStringDeltaCodec, ReadConsistency, ReplicatorActorMsg, ReplicatorWireCodecs,
    register_ddata_protocol_codecs, register_distributed_data,
};
use kairo_remote::{RemoteSettings, TcpRemoteReconnectSettings, register_remote_protocol_codecs};
use kairo_serialization::{MessageCodec, SerializationError, SerializationRegistry};
use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

use super::*;
use crate::{CoordinatorStateSnapshot, ShardCoordinatorMsg, register_sharding_protocol_codecs};

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestMessage(String);

impl RemoteMessage for TestMessage {
    const MANIFEST: &'static str = "kairo.sharding.test.ExtensionMessage";
    const VERSION: u16 = 1;
}

struct TestMessageCodec;

impl MessageCodec<TestMessage> for TestMessageCodec {
    fn serializer_id(&self) -> u32 {
        19_001
    }

    fn encode(&self, message: &TestMessage) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::copy_from_slice(message.0.as_bytes()))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<TestMessage> {
        String::from_utf8(payload.to_vec())
            .map(TestMessage)
            .map_err(|error| SerializationError::Message(error.to_string()))
    }
}

struct RecordingEntity {
    entity_id: String,
    received: Arc<Mutex<Vec<(String, String)>>>,
    started: Arc<Mutex<Vec<String>>>,
}

struct SharedCoordinatorRememberStore {
    shards: Arc<Mutex<BTreeSet<String>>>,
}

impl Actor for SharedCoordinatorRememberStore {
    type Msg = crate::RememberCoordinatorStoreMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            crate::RememberCoordinatorStoreMsg::AddShard { shard, reply_to } => {
                self.shards.lock().unwrap().insert(shard.clone());
                let _ = reply_to.tell(crate::RememberCoordinatorUpdateDone { shard });
            }
            crate::RememberCoordinatorStoreMsg::GetShards { reply_to } => {
                let _ = reply_to.tell(crate::RememberedShards {
                    shards: self.shards.lock().unwrap().clone(),
                });
            }
            crate::RememberCoordinatorStoreMsg::GetState { reply_to } => {
                let _ = reply_to.tell(crate::RememberCoordinatorStoreSnapshot {
                    shards: self.shards.lock().unwrap().clone(),
                });
            }
        }
        Ok(())
    }
}

impl Actor for RecordingEntity {
    type Msg = TestMessage;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.started.lock().unwrap().push(self.entity_id.clone());
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        let should_stop = msg.0 == "stop";
        self.received
            .lock()
            .expect("recording entity log poisoned")
            .push((self.entity_id.clone(), msg.0));
        if should_stop {
            ctx.stop(ctx.myself())?;
        }
        Ok(())
    }
}

struct ComposedShardingNode {
    kit: ActorSystemTestKit,
    runtime: TcpRemoteActorRuntime,
    cluster: kairo_cluster::ClusterDaemonHandle,
    sharding: Arc<ClusterSharding>,
    coordinator: ActorRef<ShardCoordinatorMsg<TestMessage>>,
    region: ActorRef<ShardRegionMsg<TestMessage>>,
    coordinator_probe: TestProbe<CoordinatorStateSnapshot>,
    region_probe: TestProbe<crate::ShardRegionSnapshot>,
    gossip_probe: TestProbe<Gossip>,
    received: Arc<Mutex<Vec<(String, String)>>>,
    started: Arc<Mutex<Vec<String>>>,
    ddata: Option<DistributedDataHandle<ORSet<String>>>,
    ddata_probe: TestProbe<GetResponse<ORSet<String>>>,
}

impl ComposedShardingNode {
    fn start(
        system: &str,
        node_uid: u64,
        remote_uid: u64,
        seed_nodes: Vec<kairo_actor::Address>,
        registry: Arc<Registry>,
        type_key: EntityTypeKey<TestMessage>,
    ) -> Self {
        Self::start_with_options(
            system,
            node_uid,
            remote_uid,
            seed_nodes,
            Vec::new(),
            registry,
            type_key,
            None,
            None,
            None,
            false,
            true,
        )
    }

    fn start_direct(
        system: &str,
        node_uid: u64,
        remote_uid: u64,
        seed_nodes: Vec<kairo_actor::Address>,
        registry: Arc<Registry>,
        type_key: EntityTypeKey<TestMessage>,
    ) -> Self {
        Self::start_with_options(
            system,
            node_uid,
            remote_uid,
            seed_nodes,
            Vec::new(),
            registry,
            type_key,
            None,
            None,
            None,
            false,
            false,
        )
    }

    fn start_role_scoped(
        system: &str,
        node_uid: u64,
        remote_uid: u64,
        seed_nodes: Vec<kairo_actor::Address>,
        roles: Vec<String>,
        registry: Arc<Registry>,
        type_key: EntityTypeKey<TestMessage>,
    ) -> Self {
        Self::start_with_options(
            system,
            node_uid,
            remote_uid,
            seed_nodes,
            roles,
            registry,
            type_key,
            Some("backend".to_string()),
            None,
            None,
            false,
            true,
        )
    }

    fn start_with_shared_coordinator_store(
        system: &str,
        node_uid: u64,
        remote_uid: u64,
        seed_nodes: Vec<kairo_actor::Address>,
        registry: Arc<Registry>,
        type_key: EntityTypeKey<TestMessage>,
        store: Arc<Mutex<BTreeSet<String>>>,
    ) -> Self {
        Self::start_with_options(
            system,
            node_uid,
            remote_uid,
            seed_nodes,
            Vec::new(),
            registry,
            type_key,
            None,
            Some(store),
            None,
            false,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_with_transport_ddata_remember_entities(
        system: &str,
        node_uid: u64,
        remote_uid: u64,
        seed_nodes: Vec<kairo_actor::Address>,
        registry: Arc<Registry>,
        type_key: EntityTypeKey<TestMessage>,
    ) -> Self {
        Self::start_with_options(
            system,
            node_uid,
            remote_uid,
            seed_nodes,
            Vec::new(),
            registry,
            type_key,
            None,
            None,
            None,
            true,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_with_options(
        system: &str,
        node_uid: u64,
        remote_uid: u64,
        seed_nodes: Vec<kairo_actor::Address>,
        roles: Vec<String>,
        registry: Arc<Registry>,
        type_key: EntityTypeKey<TestMessage>,
        coordinator_role: Option<String>,
        coordinator_store: Option<Arc<Mutex<BTreeSet<String>>>>,
        ddata_remember_entities: Option<DDataRememberEntitiesSettings>,
        transport_ddata_remember_entities: bool,
        use_singleton: bool,
    ) -> Self {
        let kit = ActorSystemTestKit::new(system).unwrap();
        let mut builder = TcpRemoteActorRuntime::builder(
            kit.system().clone(),
            registry,
            RemoteSettings::new("127.0.0.1", 0),
            remote_uid,
        )
        .with_reconnect_settings(
            TcpRemoteReconnectSettings::new(Duration::from_millis(100), Duration::from_millis(300))
                .unwrap(),
        );
        let cluster_registration = register_cluster_daemon(
            &mut builder,
            ClusterDaemonBootstrapSettings::new(node_uid)
                .with_seed_nodes(seed_nodes)
                .with_roles(roles)
                .with_config_digest(Some(Bytes::from_static(b"sharding-extension")))
                .with_gossip_process_settings(
                    ClusterGossipProcessSettings::new(Duration::from_millis(15)).unwrap(),
                )
                .with_heartbeat_sender_settings(
                    HeartbeatSenderSettings::new(
                        5,
                        DeadlineFailureDetectorSettings::new(
                            Duration::from_millis(15),
                            Duration::from_millis(100),
                        )
                        .unwrap(),
                    )
                    .with_heartbeat_expected_response_after(Duration::from_millis(10)),
                ),
        )
        .unwrap();
        let ddata_registration = transport_ddata_remember_entities.then(|| {
            register_distributed_data(
                &mut builder,
                cluster_registration.clone(),
                DistributedDataSettings::new(ReplicatorWireCodecs::<ORSet<String>>::new(
                    Arc::new(ORSetStringDeltaCodec),
                    Arc::new(ORSetStringCodec),
                ))
                .with_gossip_interval(Duration::from_millis(20))
                .with_delta_propagation_interval(Duration::from_millis(10)),
            )
            .unwrap()
        });
        let sharding_settings = ClusterShardingSettings::default()
            .with_registration_retry_interval(Duration::from_millis(20));
        let sharding_registration = if use_singleton {
            register_cluster_sharding_with_singleton(
                &mut builder,
                cluster_registration.clone(),
                sharding_settings,
                ClusterSingletonSettings::default()
                    .with_route_refresh_interval(Duration::from_millis(10)),
            )
        } else {
            register_cluster_sharding(
                &mut builder,
                cluster_registration.clone(),
                sharding_settings,
            )
        }
        .unwrap();
        let runtime = builder.bind().unwrap();
        let cluster = cluster_registration.activate(&runtime).unwrap();
        let transport_ddata = ddata_registration.map(|registration| {
            registration
                .activate(&runtime)
                .expect("transport ddata should activate")
        });
        let sharding = sharding_registration.activate(&runtime).unwrap();
        let received = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Mutex::new(Vec::new()));
        let entity_received = Arc::clone(&received);
        let entity_started = Arc::clone(&started);
        let mut entity = Entity::of(type_key.clone(), move |entity_id| RecordingEntity {
            entity_id,
            received: Arc::clone(&entity_received),
            started: Arc::clone(&entity_started),
        })
        .with_stop_message(TestMessage("stop".to_string()));
        if let Some(role) = coordinator_role {
            entity = entity.with_coordinator_role(role);
        }
        if let Some(shards) = coordinator_store {
            let store = kit
                .system()
                .spawn_system(
                    "shared-coordinator-remember-store",
                    Props::new(move || SharedCoordinatorRememberStore {
                        shards: Arc::clone(&shards),
                    }),
                )
                .unwrap();
            entity = entity.with_coordinator_remember_store(store, Duration::from_secs(1));
        }
        if let Some(settings) = ddata_remember_entities {
            entity = entity.with_ddata_remember_entities(settings);
        }
        if let Some(ddata) = transport_ddata.as_ref() {
            let replica = ddata.self_replica();
            let store = kit
                .system()
                .spawn_system(
                    "ddata-coordinator-remember-store",
                    crate::RememberCoordinatorORSetDDataStoreActor::props(
                        type_key.name(),
                        replica.clone(),
                        ddata.replicator().clone(),
                    ),
                )
                .unwrap();
            entity = entity.with_ddata_remember_entities(DDataRememberEntitiesSettings::new(
                store,
                replica,
                ddata.replicator().clone(),
                Duration::from_secs(1),
            ));
        }
        sharding.init(entity).unwrap();
        let (coordinator, region) = {
            let entities = sharding.entities.lock().unwrap();
            let initialized = entities
                .get(type_key.name())
                .unwrap()
                .downcast_ref::<InitializedEntity<TestMessage>>()
                .unwrap();
            (
                initialized._coordinator.clone(),
                initialized._region.clone(),
            )
        };
        let ddata_probe = kit.create_probe("ddata-read").unwrap();
        Self {
            coordinator_probe: kit.create_probe("coordinator-state").unwrap(),
            region_probe: kit.create_probe("region-state").unwrap(),
            gossip_probe: kit.create_probe("cluster-gossip").unwrap(),
            kit,
            runtime,
            cluster,
            sharding,
            coordinator,
            region,
            received,
            started,
            ddata: transport_ddata,
            ddata_probe,
        }
    }

    fn gossip(&self) -> Gossip {
        self.cluster
            .membership()
            .tell(ClusterMembershipMsg::SendCurrentGossip {
                reply_to: self.gossip_probe.actor_ref(),
            })
            .unwrap();
        self.gossip_probe
            .expect_msg(Duration::from_secs(1))
            .unwrap()
    }

    fn coordinator_state(&self) -> CoordinatorStateSnapshot {
        self.try_coordinator_state(Duration::from_secs(1)).unwrap()
    }

    fn try_coordinator_state(&self, timeout: Duration) -> Result<CoordinatorStateSnapshot, String> {
        self.coordinator
            .tell(ShardCoordinatorMsg::GetState {
                reply_to: self.coordinator_probe.actor_ref(),
            })
            .map_err(|error| error.reason().to_string())?;
        self.coordinator_probe
            .expect_msg(timeout)
            .map_err(|error| error.to_string())
    }

    fn init_additional_type(&self, type_key: EntityTypeKey<TestMessage>) {
        let received = Arc::clone(&self.received);
        self.sharding
            .init(
                Entity::new(type_key, move |entity_id| RecordingEntity {
                    entity_id,
                    received: Arc::clone(&received),
                    started: Arc::new(Mutex::new(Vec::new())),
                })
                .with_stop_message(TestMessage("stop".to_string())),
            )
            .unwrap();
    }

    fn region_state(&self) -> crate::ShardRegionSnapshot {
        self.region
            .tell(ShardRegionMsg::GetState {
                reply_to: self.region_probe.actor_ref(),
            })
            .unwrap();
        self.region_probe
            .expect_msg(Duration::from_secs(1))
            .unwrap()
    }

    fn ddata_contains_remembered_entity(
        &self,
        type_name: &str,
        shard: &str,
        entity_id: &str,
    ) -> Result<(), String> {
        let ddata = self
            .ddata
            .as_ref()
            .ok_or_else(|| "distributed data is not enabled".to_string())?;
        let index = crate::remember_entity_key_index(entity_id);
        let key = crate::remember_entity_shard_replicator_key(type_name, shard, index)
            .map_err(|error| error.to_string())?;
        ddata
            .replicator()
            .tell(ReplicatorActorMsg::Get {
                key,
                consistency: ReadConsistency::local(),
                reply_to: self.ddata_probe.actor_ref(),
            })
            .map_err(|error| error.reason().to_string())?;
        match self
            .ddata_probe
            .expect_msg(Duration::from_millis(250))
            .map_err(|error| error.to_string())?
        {
            GetResponse::Success { data, .. } if data.contains(&entity_id.to_string()) => Ok(()),
            response => Err(format!(
                "successor ddata has not converged entity {entity_id}: {response:?}"
            )),
        }
    }

    fn ddata_contains_remembered_shard(&self, type_name: &str, shard: &str) -> Result<(), String> {
        let ddata = self
            .ddata
            .as_ref()
            .ok_or_else(|| "distributed data is not enabled".to_string())?;
        ddata
            .replicator()
            .tell(ReplicatorActorMsg::Get {
                key: crate::remember_coordinator_shards_key(type_name),
                consistency: ReadConsistency::local(),
                reply_to: self.ddata_probe.actor_ref(),
            })
            .map_err(|error| error.reason().to_string())?;
        match self
            .ddata_probe
            .expect_msg(Duration::from_millis(250))
            .map_err(|error| error.to_string())?
        {
            GetResponse::Success { data, .. } if data.contains(&shard.to_string()) => Ok(()),
            response => Err(format!(
                "successor ddata has not converged shard {shard}: {response:?}"
            )),
        }
    }

    fn shutdown(self) {
        self.kit.system().stop(self.cluster.root());
        self.runtime.shutdown().unwrap();
        self.kit.shutdown(Duration::from_secs(1)).unwrap();
    }
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_remote_protocol_codecs(&mut registry).unwrap();
    register_cluster_protocol_codecs(&mut registry).unwrap();
    register_cluster_tools_protocol_codecs(&mut registry).unwrap();
    register_ddata_protocol_codecs(&mut registry).unwrap();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    registry
        .register::<TestMessage, _>(TestMessageCodec)
        .unwrap();
    Arc::new(registry)
}

#[test]
fn settings_reject_zero_capacities_and_intervals() {
    assert!(
        ClusterShardingSettings::default()
            .with_region_buffer_capacity(0)
            .validate()
            .is_err()
    );
    assert!(
        ClusterShardingSettings::default()
            .with_shard_buffer_capacity(0)
            .validate()
            .is_err()
    );
    assert!(
        ClusterShardingSettings::default()
            .with_registration_retry_interval(Duration::ZERO)
            .validate()
            .is_err()
    );
    assert!(
        ClusterShardingSettings::default()
            .with_handoff_timeout(Duration::ZERO)
            .validate()
            .is_err()
    );
    assert!(
        ClusterShardingSettings::default()
            .with_shutdown_timeout(Duration::ZERO)
            .validate()
            .is_err()
    );
}

#[test]
fn direct_registration_retains_single_node_coordinator_compatibility() {
    let type_key = EntityTypeKey::new("direct-account");
    let node = ComposedShardingNode::start_direct(
        "sharding-direct",
        10,
        110,
        Vec::new(),
        registry(),
        type_key.clone(),
    );
    await_assert(Duration::from_secs(2), Duration::from_millis(10), || {
        let state = node.coordinator_state();
        (!state.allocations.is_empty())
            .then_some(())
            .ok_or_else(|| format!("direct coordinator has not registered its region: {state:?}"))
    })
    .unwrap();

    node.sharding
        .entity_ref_for(type_key, "direct-1")
        .unwrap()
        .tell(TestMessage("local".to_string()))
        .unwrap();
    await_assert(Duration::from_secs(2), Duration::from_millis(10), || {
        let received = node.received.lock().unwrap().clone();
        received
            .contains(&("direct-1".to_string(), "local".to_string()))
            .then_some(())
            .ok_or_else(|| format!("direct coordinator route has not delivered: {received:?}"))
    })
    .unwrap();
    node.shutdown();
}

#[test]
fn role_scoped_coordinator_runs_only_on_eligible_node() {
    let registry = registry();
    let type_key = EntityTypeKey::new("role-account");
    let frontend = ComposedShardingNode::start_role_scoped(
        "sharding-role-frontend",
        20,
        120,
        Vec::new(),
        vec!["frontend".to_string()],
        registry.clone(),
        type_key.clone(),
    );
    await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
        (frontend
            .gossip()
            .member(frontend.cluster.self_node())
            .map(|member| member.status)
            == Some(MemberStatus::Up))
        .then_some(())
        .ok_or_else(|| "frontend seed has not formed".to_string())
    })
    .unwrap();
    frontend
        .coordinator
        .tell(ShardCoordinatorMsg::GetState {
            reply_to: frontend.coordinator_probe.actor_ref(),
        })
        .unwrap();
    assert!(
        frontend
            .coordinator_probe
            .expect_msg(Duration::from_millis(100))
            .is_err(),
        "role-ineligible frontend unexpectedly hosted the coordinator"
    );

    let backend = ComposedShardingNode::start_role_scoped(
        "sharding-role-backend",
        21,
        121,
        vec![frontend.cluster.self_node().address.clone()],
        vec!["backend".to_string()],
        registry,
        type_key.clone(),
    );
    await_assert(Duration::from_secs(4), Duration::from_millis(10), || {
        let state = backend.coordinator_state();
        (state.allocations.len() == 2).then_some(()).ok_or_else(|| {
            format!("backend coordinator has not registered both regions: {state:?}")
        })
    })
    .unwrap();

    let entity_id = "role-account-1";
    let shard = crate::default_shard_id_for(entity_id);
    frontend
        .sharding
        .entity_ref_for(type_key, entity_id)
        .unwrap()
        .tell(TestMessage("role-route".to_string()))
        .unwrap();
    await_assert(Duration::from_secs(8), Duration::from_millis(10), || {
        let expected = (entity_id.to_string(), "role-route".to_string());
        let frontend_received = frontend.received.lock().unwrap().clone();
        let backend_received = backend.received.lock().unwrap().clone();
        (frontend_received.contains(&expected) || backend_received.contains(&expected))
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "role-scoped route has not delivered: frontend={frontend_received:?}, backend={backend_received:?}"
                )
            })
    })
    .unwrap();
    let state = backend.coordinator_state();
    assert!(
        state
            .allocations
            .values()
            .any(|shards| shards.contains(&shard)),
        "backend coordinator did not allocate routed shard {shard}: {state:?}"
    );

    backend.shutdown();
    frontend.shutdown();
}

#[test]
fn singleton_successor_recovers_remembered_shard_allocation() {
    let registry = registry();
    let type_key = EntityTypeKey::new("remembered-account");
    let store = Arc::new(Mutex::new(BTreeSet::new()));
    let seed = ComposedShardingNode::start_with_shared_coordinator_store(
        "sharding-remembered-seed",
        30,
        130,
        Vec::new(),
        registry.clone(),
        type_key.clone(),
        Arc::clone(&store),
    );
    await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
        (seed
            .gossip()
            .member(seed.cluster.self_node())
            .map(|member| member.status)
            == Some(MemberStatus::Up))
        .then_some(())
        .ok_or_else(|| "remember-store seed has not formed".to_string())
    })
    .unwrap();
    let peer = ComposedShardingNode::start_with_shared_coordinator_store(
        "sharding-remembered-peer",
        31,
        131,
        vec![seed.cluster.self_node().address.clone()],
        registry,
        type_key.clone(),
        Arc::clone(&store),
    );
    await_assert(Duration::from_secs(4), Duration::from_millis(10), || {
        let state = seed.coordinator_state();
        (state.allocations.len() == 2)
            .then_some(())
            .ok_or_else(|| format!("remembering coordinator is not ready: {state:?}"))
    })
    .unwrap();

    let entity_id = "remembered-account-1";
    let shard = crate::default_shard_id_for(entity_id);
    peer.sharding
        .entity_ref_for(type_key, entity_id)
        .unwrap()
        .tell(TestMessage("before-failover".to_string()))
        .unwrap();
    await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
        store
            .lock()
            .unwrap()
            .contains(&shard)
            .then_some(())
            .ok_or_else(|| format!("coordinator store has not remembered shard {shard}"))
    })
    .unwrap();

    seed.cluster.cluster().leave_self().unwrap();
    await_assert(Duration::from_secs(4), Duration::from_millis(25), || {
        let state = peer.coordinator_state();
        state
            .allocations
            .values()
            .any(|shards| shards.contains(&shard))
            .then_some(())
            .ok_or_else(|| {
                format!("successor has not recovered remembered shard {shard}: {state:?}")
            })
    })
    .unwrap();

    peer.shutdown();
    seed.shutdown();
}

#[test]
fn singleton_successor_recovers_shard_from_transport_ddata() {
    let registry = registry();
    let type_key = EntityTypeKey::new("ddata-coordinator-account");
    let seed = ComposedShardingNode::start_with_transport_ddata_remember_entities(
        "sharding-ddata-coordinator-seed",
        35,
        135,
        Vec::new(),
        registry.clone(),
        type_key.clone(),
    );
    await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
        (seed
            .gossip()
            .member(seed.cluster.self_node())
            .map(|member| member.status)
            == Some(MemberStatus::Up))
        .then_some(())
        .ok_or_else(|| "ddata coordinator seed has not formed".to_string())
    })
    .unwrap();
    let peer = ComposedShardingNode::start_with_transport_ddata_remember_entities(
        "sharding-ddata-coordinator-peer",
        36,
        136,
        vec![seed.cluster.self_node().address.clone()],
        registry,
        type_key.clone(),
    );
    await_assert(Duration::from_secs(4), Duration::from_millis(10), || {
        let state = seed.try_coordinator_state(Duration::from_millis(250))?;
        (state.allocations.len() == 2)
            .then_some(())
            .ok_or_else(|| format!("ddata coordinator is not ready: {state:?}"))
    })
    .unwrap();

    let entity_id = "ddata-coordinator-account-1";
    let shard = crate::default_shard_id_for(entity_id);
    peer.sharding
        .entity_ref_for(type_key.clone(), entity_id)
        .unwrap()
        .tell(TestMessage("persist-shard".to_string()))
        .unwrap();
    await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
        let delivered = seed
            .received
            .lock()
            .unwrap()
            .contains(&(entity_id.to_string(), "persist-shard".to_string()))
            || peer
                .received
                .lock()
                .unwrap()
                .contains(&(entity_id.to_string(), "persist-shard".to_string()));
        delivered
            .then_some(())
            .ok_or_else(|| "initial shard request has not delivered".to_string())
    })
    .unwrap();
    await_assert(Duration::from_secs(4), Duration::from_millis(20), || {
        peer.ddata_contains_remembered_shard(type_key.name(), &shard)
    })
    .unwrap();

    seed.cluster.cluster().leave_self().unwrap();
    await_assert(Duration::from_secs(4), Duration::from_millis(20), || {
        let state = peer.try_coordinator_state(Duration::from_millis(250))?;
        state
            .allocations
            .values()
            .any(|shards| shards.contains(&shard))
            .then_some(())
            .ok_or_else(|| format!("successor has not restored ddata shard {shard}: {state:?}"))
    })
    .unwrap();

    peer.shutdown();
    seed.shutdown();
}

#[test]
fn public_ddata_remember_entities_recovers_entity_after_region_failover() {
    let registry = registry();
    let type_key = EntityTypeKey::new("ddata-account");
    let seed = ComposedShardingNode::start_with_transport_ddata_remember_entities(
        "sharding-ddata-seed",
        40,
        140,
        Vec::new(),
        registry.clone(),
        type_key.clone(),
    );
    await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
        (seed
            .gossip()
            .member(seed.cluster.self_node())
            .map(|member| member.status)
            == Some(MemberStatus::Up))
        .then_some(())
        .ok_or_else(|| "ddata remember seed has not formed".to_string())
    })
    .unwrap();
    let peer = ComposedShardingNode::start_with_transport_ddata_remember_entities(
        "sharding-ddata-peer",
        41,
        141,
        vec![seed.cluster.self_node().address.clone()],
        registry,
        type_key.clone(),
    );
    await_assert(Duration::from_secs(4), Duration::from_millis(10), || {
        let state = seed.try_coordinator_state(Duration::from_millis(250))?;
        (state.allocations.len() == 2)
            .then_some(())
            .ok_or_else(|| format!("ddata remember coordinator is not ready: {state:?}"))
    })
    .unwrap();

    let entity_id = "ddata-account-1";
    let shard = crate::default_shard_id_for(entity_id);
    peer.sharding
        .entity_ref_for(type_key.clone(), entity_id)
        .unwrap()
        .tell(TestMessage("before-failover".to_string()))
        .unwrap();
    let initial_owner_is_seed =
        match await_assert(Duration::from_secs(8), Duration::from_millis(10), || {
            if seed
                .received
                .lock()
                .unwrap()
                .contains(&(entity_id.to_string(), "before-failover".to_string()))
            {
                Ok(true)
            } else if peer
                .received
                .lock()
                .unwrap()
                .contains(&(entity_id.to_string(), "before-failover".to_string()))
            {
                Ok(false)
            } else {
                Err("initial remembered entity delivery has not completed".to_string())
            }
        }) {
            Ok(owner) => owner,
            Err(error) => panic!(
                "{error}; seed coordinator={:?}; seed region={:?}; peer region={:?}; \
             seed started={:?}; peer started={:?}",
                seed.try_coordinator_state(Duration::from_secs(1)),
                seed.region_state(),
                peer.region_state(),
                seed.started.lock().unwrap(),
                peer.started.lock().unwrap(),
            ),
        };

    let (owner, successor) = if initial_owner_is_seed {
        (&seed, &peer)
    } else {
        (&peer, &seed)
    };
    await_assert(Duration::from_secs(4), Duration::from_millis(20), || {
        successor.ddata_contains_remembered_entity(type_key.name(), &shard, entity_id)
    })
    .unwrap();
    let owner_region = await_assert(Duration::from_secs(4), Duration::from_millis(20), || {
        seed.try_coordinator_state(Duration::from_millis(250))?
            .allocations
            .into_iter()
            .find_map(|(region, shards)| shards.contains(&shard).then_some(region))
            .ok_or_else(|| "routed shard should have an owner before failover".to_string())
    })
    .unwrap();
    owner.kit.system().stop(&owner.region);
    assert!(owner.region.wait_for_stop(Duration::from_secs(1)));
    seed.coordinator
        .tell(ShardCoordinatorMsg::RegionStopped {
            region: owner_region,
        })
        .unwrap();
    let recovery = await_assert(Duration::from_secs(4), Duration::from_millis(10), || {
        let recovered = successor
            .started
            .lock()
            .unwrap()
            .contains(&entity_id.to_string());
        if recovered {
            Ok(())
        } else {
            Err(format!(
                "successor has not recovered entity {entity_id}; coordinator={:?}; \
                     successor region={:?}; seed started={:?}; peer started={:?}",
                seed.coordinator_state(),
                successor.region_state(),
                seed.started.lock().unwrap(),
                peer.started.lock().unwrap(),
            ))
        }
    });
    if let Err(error) = recovery {
        peer.shutdown();
        seed.shutdown();
        panic!("{error}");
    }
    let state = seed.coordinator_state();
    assert!(
        state
            .allocations
            .values()
            .any(|shards| shards.contains(&shard)),
        "remembered shard should remain allocated after failover: {state:?}"
    );

    successor
        .sharding
        .entity_ref_for(type_key, entity_id)
        .unwrap()
        .tell(TestMessage("after-failover".to_string()))
        .unwrap();
    await_assert(Duration::from_secs(2), Duration::from_millis(10), || {
        successor
            .received
            .lock()
            .unwrap()
            .contains(&(entity_id.to_string(), "after-failover".to_string()))
            .then_some(())
            .ok_or_else(|| "recovered entity has not received post-failover message".to_string())
    })
    .unwrap();

    peer.shutdown();
    seed.shutdown();
}

#[test]
fn composed_extension_routes_remote_entities_and_recovers_after_coordinator_handover() {
    let registry = registry();
    let type_key = EntityTypeKey::new("account");
    let seed = ComposedShardingNode::start(
        "sharding-a-seed",
        1,
        101,
        Vec::new(),
        registry.clone(),
        type_key.clone(),
    );
    await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
        (seed
            .gossip()
            .member(seed.cluster.self_node())
            .map(|member| member.status)
            == Some(MemberStatus::Up))
        .then_some(())
        .ok_or_else(|| "sharding seed has not formed".to_string())
    })
    .unwrap();
    let peer = ComposedShardingNode::start(
        "sharding-z-peer",
        2,
        102,
        vec![seed.cluster.self_node().address.clone()],
        registry,
        type_key.clone(),
    );
    let audit_key = EntityTypeKey::new("audit");
    seed.init_additional_type(audit_key.clone());
    peer.init_additional_type(audit_key.clone());
    await_assert(Duration::from_secs(4), Duration::from_millis(10), || {
        let state = seed.coordinator_state();
        (state.allocations.len() == 2)
            .then_some(())
            .ok_or_else(|| format!("oldest coordinator has not registered both regions: {state:?}"))
    })
    .unwrap();

    peer.sharding
        .entity_ref_for(type_key.clone(), "account-7")
        .unwrap()
        .tell(TestMessage("credit".to_string()))
        .unwrap();
    peer.sharding
        .entity_ref_for(audit_key, "audit-3")
        .unwrap()
        .tell(TestMessage("record".to_string()))
        .unwrap();

    let delivery = await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
        let received = seed.received.lock().unwrap().clone();
        (received.len() == 2
            && received.contains(&("account-7".to_string(), "credit".to_string()))
            && received.contains(&("audit-3".to_string(), "record".to_string())))
        .then_some(())
        .ok_or_else(|| format!("remote entity has not received message: {received:?}"))
    });
    if let Err(error) = delivery {
        panic!(
            "{error}; seed coordinator={:?}; seed region={:?}; peer region={:?}; peer received={:?}",
            seed.coordinator_state(),
            seed.region_state(),
            peer.region_state(),
            peer.received.lock().unwrap()
        );
    }
    assert!(peer.received.lock().unwrap().is_empty());

    seed.cluster.cluster().leave_self().unwrap();
    await_assert(Duration::from_secs(4), Duration::from_millis(25), || {
        let state = peer.coordinator_state();
        (!state.allocations.is_empty())
            .then_some(())
            .ok_or_else(|| {
                format!("successor coordinator has not registered its region: {state:?}")
            })
    })
    .unwrap();

    let previous_shard = crate::default_shard_id_for("account-7");
    let successor_entity = (0_u32..)
        .map(|index| format!("account-after-handover-{index}"))
        .find(|entity_id| crate::default_shard_id_for(entity_id) != previous_shard)
        .unwrap();
    let successor_shard = crate::default_shard_id_for(&successor_entity);
    peer.sharding
        .entity_ref_for(type_key, successor_entity.clone())
        .unwrap()
        .tell(TestMessage("after-handover".to_string()))
        .unwrap();
    let successor_delivery = await_assert(
        Duration::from_secs(3),
        Duration::from_millis(10),
        || {
            let expected = (successor_entity.clone(), "after-handover".to_string());
            let seed_received = seed.received.lock().unwrap().clone();
            let peer_received = peer.received.lock().unwrap().clone();
            (seed_received.contains(&expected) || peer_received.contains(&expected))
                .then_some(())
                .ok_or_else(|| {
                    format!(
                        "no region has received the post-handover message: seed={seed_received:?}, peer={peer_received:?}"
                    )
                })
        },
    );
    if let Err(error) = successor_delivery {
        panic!(
            "{error}; successor coordinator={:?}; successor region={:?}; seed received={:?}",
            peer.coordinator_state(),
            peer.region_state(),
            seed.received.lock().unwrap()
        );
    }
    await_assert(Duration::from_secs(2), Duration::from_millis(10), || {
        let state = peer.coordinator_state();
        state
            .allocations
            .values()
            .any(|shards| shards.contains(&successor_shard))
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "successor coordinator has not allocated shard {successor_shard}: {state:?}"
                )
            })
    })
    .unwrap();

    peer.shutdown();
    seed.shutdown();
}
