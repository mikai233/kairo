use std::collections::BTreeSet;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use kairo::actor::{Actor, ActorResult, ActorSystem, Context};
use kairo::cluster::{
    ClusterDaemonBootstrapSettings, ClusterDaemonHandle, ClusterGossipProcessSettings,
    ClusterMembershipMsg, Convergence, DeadlineFailureDetectorSettings, Gossip,
    HeartbeatSenderSettings, MemberStatus, register_cluster_daemon,
    register_cluster_protocol_codecs,
};
use kairo::cluster_sharding::{
    ClusterSharding, ClusterShardingSettings, DDataRememberEntitiesSettings, Entity, EntityTypeKey,
    RememberCoordinatorORSetDDataStoreActor, default_shard_id_for,
    register_cluster_sharding_with_singleton, register_sharding_protocol_codecs,
    remember_entity_key_index, remember_entity_shard_replicator_key,
};
use kairo::cluster_tools::{ClusterSingletonSettings, register_cluster_tools_protocol_codecs};
use kairo::distributed_data::{
    DistributedDataHandle, DistributedDataSettings, GetResponse, ORSet, ORSetStringCodec,
    ORSetStringDeltaCodec, ReadConsistency, ReplicatorActorMsg, ReplicatorWireCodecs,
    register_ddata_protocol_codecs, register_distributed_data,
};
use kairo::remote::{
    RemoteSettings, TcpRemoteActorRuntime, TcpRemoteReconnectSettings,
    register_remote_protocol_codecs,
};
use kairo::serialization::{
    MessageCodec, Registry, RemoteMessage, SerializationError, SerializationRegistry,
};

use crate::reply::spawn_one_shot_reply;

static REPLY_ID: AtomicU64 = AtomicU64::new(0);

type DemoResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardingCounterCommand {
    Add(u64),
    Stop,
}

impl RemoteMessage for ShardingCounterCommand {
    const MANIFEST: &'static str = "kairo.example.sharding-counter-command";
    const VERSION: u16 = 1;
}

struct ShardingCounterCommandCodec;

impl MessageCodec<ShardingCounterCommand> for ShardingCounterCommandCodec {
    fn serializer_id(&self) -> u32 {
        45_001
    }

    fn encode(&self, message: &ShardingCounterCommand) -> kairo::serialization::Result<Bytes> {
        match message {
            ShardingCounterCommand::Add(amount) => {
                let mut payload = Vec::with_capacity(9);
                payload.push(0);
                payload.extend_from_slice(&amount.to_be_bytes());
                Ok(Bytes::from(payload))
            }
            ShardingCounterCommand::Stop => Ok(Bytes::from_static(&[1])),
        }
    }

    fn decode(
        &self,
        payload: Bytes,
        _version: u16,
    ) -> kairo::serialization::Result<ShardingCounterCommand> {
        match payload.first().copied() {
            Some(0) if payload.len() == 9 => {
                let mut amount = [0; 8];
                amount.copy_from_slice(&payload[1..]);
                Ok(ShardingCounterCommand::Add(u64::from_be_bytes(amount)))
            }
            Some(1) if payload.len() == 1 => Ok(ShardingCounterCommand::Stop),
            _ => Err(SerializationError::Message(
                "invalid sharding counter command payload".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardingNodeObservation {
    pub started_entities: Vec<String>,
    pub stopped_entities: Vec<String>,
    pub deliveries: Vec<(String, u64)>,
}

#[derive(Default)]
struct ShardingNodeRecorder {
    observation: Mutex<ShardingNodeObservation>,
}

impl ShardingNodeRecorder {
    fn record_start(&self, entity_id: String) {
        self.observation
            .lock()
            .expect("sharding observation poisoned")
            .started_entities
            .push(entity_id);
    }

    fn record_delivery(&self, entity_id: String, amount: u64) {
        self.observation
            .lock()
            .expect("sharding observation poisoned")
            .deliveries
            .push((entity_id, amount));
    }

    fn record_stop(&self, entity_id: String) {
        self.observation
            .lock()
            .expect("sharding observation poisoned")
            .stopped_entities
            .push(entity_id);
    }

    fn snapshot(&self) -> ShardingNodeObservation {
        self.observation
            .lock()
            .expect("sharding observation poisoned")
            .clone()
    }
}

struct ShardingCounter {
    entity_id: String,
    recorder: Arc<ShardingNodeRecorder>,
}

impl Actor for ShardingCounter {
    type Msg = ShardingCounterCommand;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.recorder.record_start(self.entity_id.clone());
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, message: Self::Msg) -> ActorResult {
        match message {
            ShardingCounterCommand::Add(amount) => {
                self.recorder
                    .record_delivery(self.entity_id.clone(), amount);
                Ok(())
            }
            ShardingCounterCommand::Stop => {
                self.recorder.record_stop(self.entity_id.clone());
                ctx.stop(ctx.myself())
            }
        }
    }
}

struct ShardingTcpDemoNode {
    system: ActorSystem,
    _runtime: TcpRemoteActorRuntime,
    cluster: ClusterDaemonHandle,
    sharding: Arc<ClusterSharding>,
    ddata: DistributedDataHandle<ORSet<String>>,
    type_key: EntityTypeKey<ShardingCounterCommand>,
    recorder: Arc<ShardingNodeRecorder>,
}

impl ShardingTcpDemoNode {
    fn start(
        system_name: &str,
        node_uid: u64,
        remote_uid: u64,
        seed_nodes: Vec<kairo::actor::Address>,
        type_key: EntityTypeKey<ShardingCounterCommand>,
    ) -> DemoResult<Self> {
        let system = ActorSystem::builder(system_name).build()?;
        let registry = sharding_registry()?;
        let mut builder = TcpRemoteActorRuntime::builder(
            system.clone(),
            registry,
            RemoteSettings::new("127.0.0.1", 0),
            remote_uid,
        )
        .with_reconnect_settings(TcpRemoteReconnectSettings::new(
            Duration::from_millis(100),
            Duration::from_millis(300),
        )?);
        let cluster_registration = register_cluster_daemon(
            &mut builder,
            ClusterDaemonBootstrapSettings::new(node_uid)
                .with_seed_nodes(seed_nodes)
                .with_config_digest(Some(Bytes::from_static(b"sharding-tcp-acceptance")))
                .with_gossip_process_settings(ClusterGossipProcessSettings::new(
                    Duration::from_millis(15),
                )?)
                .with_heartbeat_sender_settings(
                    HeartbeatSenderSettings::new(
                        5,
                        DeadlineFailureDetectorSettings::new(
                            Duration::from_millis(100),
                            Duration::from_secs(2),
                        )
                        .map_err(|error| format!("invalid failure detector settings: {error:?}"))?,
                    )
                    .with_heartbeat_expected_response_after(Duration::from_millis(500)),
                ),
        )?;
        let ddata_registration = register_distributed_data(
            &mut builder,
            cluster_registration.clone(),
            DistributedDataSettings::new(ReplicatorWireCodecs::<ORSet<String>>::new(
                Arc::new(ORSetStringDeltaCodec),
                Arc::new(ORSetStringCodec),
            ))
            .with_gossip_interval(Duration::from_millis(20))
            .with_delta_propagation_interval(Duration::from_millis(10)),
        )?;
        let sharding_registration = register_cluster_sharding_with_singleton(
            &mut builder,
            cluster_registration.clone(),
            ClusterShardingSettings::default()
                .with_registration_retry_interval(Duration::from_millis(20))
                .with_rebalance_interval(Duration::from_secs(5))
                .with_handoff_timeout(Duration::from_secs(2))
                .with_shutdown_timeout(Duration::from_secs(8)),
            ClusterSingletonSettings::default()
                .with_route_refresh_interval(Duration::from_millis(10)),
        )?;

        let runtime = builder.bind()?;
        let cluster = cluster_registration.activate(&runtime)?;
        let ddata = ddata_registration.activate(&runtime)?;
        let sharding = sharding_registration.activate(&runtime)?;
        let coordinator_store = system.spawn_system(
            "sharding-demo-coordinator-store",
            RememberCoordinatorORSetDDataStoreActor::props(
                type_key.name(),
                ddata.self_replica(),
                ddata.replicator().clone(),
            ),
        )?;
        let recorder = Arc::new(ShardingNodeRecorder::default());
        let entity_recorder = Arc::clone(&recorder);
        sharding.init(
            Entity::of(type_key.clone(), move |entity_id| ShardingCounter {
                entity_id,
                recorder: Arc::clone(&entity_recorder),
            })
            .with_stop_message(ShardingCounterCommand::Stop)
            .with_ddata_remember_entities(DDataRememberEntitiesSettings::new(
                coordinator_store,
                ddata.self_replica(),
                ddata.replicator().clone(),
                Duration::from_secs(1),
            )),
        )?;

        Ok(Self {
            system,
            _runtime: runtime,
            cluster,
            sharding,
            ddata,
            type_key,
            recorder,
        })
    }

    fn seed_address(&self) -> kairo::actor::Address {
        self.cluster.self_node().address.clone()
    }

    fn send(&self, entity_id: &str, message: ShardingCounterCommand) -> DemoResult<()> {
        self.sharding
            .entity_ref_for(self.type_key.clone(), entity_id)?
            .tell(message)?;
        Ok(())
    }

    fn observation(&self) -> ShardingNodeObservation {
        self.recorder.snapshot()
    }

    fn gossip(&self, timeout: Duration) -> DemoResult<Gossip> {
        let id = REPLY_ID.fetch_add(1, Ordering::Relaxed);
        let (reply_to, replies) =
            spawn_one_shot_reply(&self.system, format!("sharding-demo-gossip-{id}"))?;
        self.cluster
            .membership()
            .tell(ClusterMembershipMsg::SendCurrentGossip { reply_to })?;
        Ok(replies.recv_timeout(timeout)?)
    }

    fn wait_for_up_members(&self, count: usize, timeout: Duration) -> DemoResult<()> {
        wait_until(
            timeout,
            || {
                let gossip = self.gossip(Duration::from_millis(250))?;
                let up = gossip
                    .members()
                    .iter()
                    .filter(|member| member.status == MemberStatus::Up)
                    .count();
                Ok((up == count).then_some(()))
            },
            format!("{count} Up cluster members"),
        )
    }

    fn wait_for_convergence(&self, timeout: Duration) -> DemoResult<()> {
        wait_until(
            timeout,
            || {
                let gossip = self.gossip(Duration::from_millis(250))?;
                Ok(Convergence::check(&gossip, self.cluster.self_node())
                    .is_converged()
                    .then_some(()))
            },
            "cluster gossip convergence",
        )
    }

    fn contains_remembered_entity(&self, entity_id: &str) -> DemoResult<bool> {
        let shard = default_shard_id_for(entity_id);
        let index = remember_entity_key_index(entity_id);
        let key = remember_entity_shard_replicator_key(self.type_key.name(), &shard, index)?;
        let id = REPLY_ID.fetch_add(1, Ordering::Relaxed);
        let (reply_to, replies) =
            spawn_one_shot_reply(&self.system, format!("sharding-demo-ddata-{id}"))?;
        self.ddata.replicator().tell(ReplicatorActorMsg::Get {
            key,
            consistency: ReadConsistency::local(),
            reply_to,
        })?;
        Ok(matches!(
            replies.recv_timeout(Duration::from_millis(250))?,
            GetResponse::Success { data, .. } if data.contains(&entity_id.to_string())
        ))
    }

    fn wait_for_remembered_entities(
        &self,
        entity_ids: &[String],
        timeout: Duration,
    ) -> DemoResult<()> {
        wait_until(
            timeout,
            || {
                for entity_id in entity_ids {
                    if !self.contains_remembered_entity(entity_id)? {
                        return Ok(None);
                    }
                }
                Ok(Some(()))
            },
            "remembered entities in the local ddata replica",
        )
    }

    fn shutdown(&self, timeout: Duration) -> DemoResult<()> {
        self.system
            .run_coordinated_shutdown("three-node sharding demo complete", timeout)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreeNodeShardingObservation {
    pub initial_entities: Vec<String>,
    pub rebalanced_entity: String,
    pub recovered_after_leave: String,
    pub remaining_member_count: usize,
    pub delivered_after_recovery: bool,
}

pub fn run_three_node_sharding_acceptance() -> DemoResult<ThreeNodeShardingObservation> {
    let type_key = EntityTypeKey::new("three-node-counter");
    let node_a =
        ShardingTcpDemoNode::start("sharding-demo-a", 1, 101, Vec::new(), type_key.clone())?;
    node_a.wait_for_up_members(1, Duration::from_secs(3))?;

    let entity_ids = distinct_shard_entity_ids(6);
    for entity_id in &entity_ids {
        node_a.send(entity_id, ShardingCounterCommand::Add(1))?;
    }
    wait_until(
        Duration::from_secs(4),
        || {
            let observation = node_a.observation();
            Ok((observation.deliveries.len() == entity_ids.len()).then_some(()))
        },
        "initial entities on the seed node",
    )?;

    let seed = node_a.seed_address();
    let node_b = ShardingTcpDemoNode::start(
        "sharding-demo-b",
        2,
        102,
        vec![seed.clone()],
        type_key.clone(),
    )?;
    let node_c = ShardingTcpDemoNode::start("sharding-demo-c", 3, 103, vec![seed], type_key)?;

    let scenario = (|| -> DemoResult<ThreeNodeShardingObservation> {
        for node in [&node_a, &node_b, &node_c] {
            node.wait_for_up_members(3, Duration::from_secs(5))?;
        }
        node_b.wait_for_remembered_entities(&entity_ids, Duration::from_secs(4))?;
        node_c.wait_for_remembered_entities(&entity_ids, Duration::from_secs(4))?;

        let rebalanced_entity = wait_until(
            Duration::from_secs(8),
            || {
                let moved = node_b
                    .observation()
                    .started_entities
                    .into_iter()
                    .chain(node_c.observation().started_entities)
                    .find(|entity_id| entity_ids.contains(entity_id));
                Ok(moved)
            },
            "an existing shard to rebalance through handoff",
        )
        .map_err(|error| {
            format!(
                "{error}; seed={:?}; peer-b={:?}; peer-c={:?}",
                node_a.observation(),
                node_b.observation(),
                node_c.observation(),
            )
        })?;
        let moved_before_leave = node_b
            .observation()
            .started_entities
            .into_iter()
            .chain(node_c.observation().started_entities)
            .collect::<BTreeSet<_>>();
        let recovered_after_leave = entity_ids
            .iter()
            .find(|entity_id| !moved_before_leave.contains(*entity_id))
            .cloned()
            .ok_or("periodic rebalance moved every entity before oldest-node leave")?;

        node_a
            .wait_for_convergence(Duration::from_secs(5))
            .map_err(|error| {
                format!(
                    "{error}; node-a={:?}; node-b={:?}; node-c={:?}",
                    node_a.gossip(Duration::from_secs(1)),
                    node_b.gossip(Duration::from_secs(1)),
                    node_c.gossip(Duration::from_secs(1)),
                )
            })?;
        if let Err(error) = node_a.shutdown(Duration::from_secs(12)) {
            return Err(format!(
                "oldest node shutdown failed: {error}; node-a={:?}; node-b={:?}; node-c={:?}",
                node_a.gossip(Duration::from_secs(1)),
                node_b.gossip(Duration::from_secs(1)),
                node_c.gossip(Duration::from_secs(1)),
            )
            .into());
        }
        node_b.wait_for_up_members(2, Duration::from_secs(5))?;
        node_c.wait_for_up_members(2, Duration::from_secs(5))?;
        wait_until(
            Duration::from_secs(6),
            || {
                let restarted = node_b
                    .observation()
                    .started_entities
                    .iter()
                    .chain(node_c.observation().started_entities.iter())
                    .any(|entity_id| entity_id == &recovered_after_leave);
                Ok(restarted.then_some(()))
            },
            "a remembered entity to restart after oldest-node leave",
        )?;

        node_b.send(&recovered_after_leave, ShardingCounterCommand::Add(2))?;
        wait_until(
            Duration::from_secs(4),
            || {
                let delivered = node_b
                    .observation()
                    .deliveries
                    .iter()
                    .chain(node_c.observation().deliveries.iter())
                    .any(|delivery| delivery == &(recovered_after_leave.clone(), 2));
                Ok(delivered.then_some(()))
            },
            "post-recovery entity delivery",
        )?;

        Ok(ThreeNodeShardingObservation {
            initial_entities: entity_ids.clone(),
            rebalanced_entity,
            recovered_after_leave,
            remaining_member_count: 2,
            delivered_after_recovery: true,
        })
    })();

    let shutdown_a = node_a.shutdown(Duration::from_secs(12));
    let shutdown_b = node_b.shutdown(Duration::from_secs(12));
    let shutdown_c = node_c.shutdown(Duration::from_secs(12));

    let observation = scenario?;
    shutdown_a
        .map_err(|error| -> Box<dyn Error> { format!("node-a shutdown failed: {error}").into() })?;
    shutdown_b
        .map_err(|error| -> Box<dyn Error> { format!("node-b shutdown failed: {error}").into() })?;
    shutdown_c
        .map_err(|error| -> Box<dyn Error> { format!("node-c shutdown failed: {error}").into() })?;
    Ok(observation)
}

fn distinct_shard_entity_ids(count: usize) -> Vec<String> {
    let mut shards = BTreeSet::new();
    let mut entities = Vec::with_capacity(count);
    for index in 0.. {
        let entity_id = format!("counter-{index}");
        if shards.insert(default_shard_id_for(&entity_id)) {
            entities.push(entity_id);
            if entities.len() == count {
                return entities;
            }
        }
    }
    unreachable!("the stable shard function should produce enough distinct shards")
}

fn sharding_registry() -> DemoResult<Arc<Registry>> {
    let mut registry = Registry::new();
    register_remote_protocol_codecs(&mut registry)?;
    register_cluster_protocol_codecs(&mut registry)?;
    register_cluster_tools_protocol_codecs(&mut registry)?;
    register_ddata_protocol_codecs(&mut registry)?;
    register_sharding_protocol_codecs(&mut registry)?;
    registry.register::<ShardingCounterCommand, _>(ShardingCounterCommandCodec)?;
    Ok(Arc::new(registry))
}

fn wait_until<T, F>(timeout: Duration, mut check: F, description: impl AsRef<str>) -> DemoResult<T>
where
    F: FnMut() -> DemoResult<Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = check()? {
            return Ok(value);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!("timed out waiting for {}", description.as_ref()).into());
        }
        thread::sleep(Duration::from_millis(10).min(remaining));
    }
}
