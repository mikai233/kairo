use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use kairo::actor::{ActorRef, ActorSystem};
use kairo::cluster_sharding::{
    CoordinatorState, DEFAULT_SHARD_COUNT, EntityRef, HandoffTransport,
    LeastShardAllocationStrategy, ShardCoordinatorActor, ShardMsg, ShardRegionActor,
    ShardRegionMsg, ShardSnapshot, ShardingEnvelopeRouter, default_shard_id_for,
};

use crate::reply::spawn_one_shot_reply;

static REPLY_ID: AtomicU64 = AtomicU64::new(0);

pub struct LocalShardingExample {
    system: ActorSystem,
    region: ActorRef<ShardRegionMsg<String>>,
    router: ActorRef<kairo::cluster_sharding::ShardingEnvelope<String>>,
}

impl LocalShardingExample {
    pub fn start(system_name: &str) -> Result<Self, Box<dyn Error>> {
        let system = ActorSystem::builder(system_name).build()?;
        let coordinator = system.spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_handoff(
                CoordinatorState::new(),
                LeastShardAllocationStrategy::default(),
                "stop".to_string(),
                Duration::from_millis(500),
                HandoffTransport::new(),
            ),
        )?;
        let region = system.spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_local_shards_and_registration(
                "region-a",
                32,
                32,
                coordinator,
                Duration::from_millis(20),
            ),
        )?;
        let router = system.spawn(
            "entity-router",
            ShardingEnvelopeRouter::props(region.clone(), DEFAULT_SHARD_COUNT),
        )?;

        Ok(Self {
            system,
            region,
            router,
        })
    }

    pub fn entity_ref(&self, entity_id: impl Into<String>) -> EntityRef<String> {
        EntityRef::new(entity_id, self.router.clone())
    }

    pub fn wait_for_active_entity(
        &self,
        entity_id: &str,
        timeout: Duration,
    ) -> Result<ShardSnapshot, Box<dyn Error>> {
        let shard = default_shard_id_for(entity_id);
        let shard_ref = self.wait_for_local_shard(&shard, timeout)?;
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = self.shard_snapshot(&shard_ref, Duration::from_millis(100))?;
            if snapshot
                .active_entities
                .iter()
                .any(|active| active == entity_id)
            {
                return Ok(snapshot);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for entity `{entity_id}` in shard `{shard}`"
                )
                .into());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), kairo::actor::ActorError> {
        self.system
            .run_coordinated_shutdown("local sharding example complete", timeout)
    }

    fn wait_for_local_shard(
        &self,
        shard: &str,
        timeout: Duration,
    ) -> Result<ActorRef<ShardMsg<String>>, Box<dyn Error>> {
        let deadline = Instant::now() + timeout;
        loop {
            let id = REPLY_ID.fetch_add(1, Ordering::Relaxed);
            let (reply_to, replies) =
                spawn_one_shot_reply(&self.system, format!("local-shard-{id}"))?;
            self.region.tell(ShardRegionMsg::GetLocalShard {
                shard: shard.to_string(),
                reply_to,
            })?;
            if let Some(shard_ref) = replies.recv_timeout(Duration::from_millis(100))? {
                return Ok(shard_ref);
            }
            if Instant::now() >= deadline {
                return Err(format!("timed out waiting for local shard `{shard}`").into());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn shard_snapshot(
        &self,
        shard: &ActorRef<ShardMsg<String>>,
        timeout: Duration,
    ) -> Result<ShardSnapshot, Box<dyn Error>> {
        let id = REPLY_ID.fetch_add(1, Ordering::Relaxed);
        let (reply_to, replies) =
            spawn_one_shot_reply(&self.system, format!("shard-snapshot-{id}"))?;
        shard.tell(ShardMsg::GetState { reply_to })?;
        Ok(replies.recv_timeout(timeout)?)
    }
}
