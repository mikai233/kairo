use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::thread;
use std::time::{Duration, Instant};

use kairo::actor::{ActorRef, ActorSystem};
use kairo::cluster_sharding::{
    CoordinatorEvent, CoordinatorStateSnapshot, HandoffRegionTarget, HostShardPlan,
    LeastShardAllocationStrategy, RegionShutdownPlan, ShardCoordinatorActor,
    ShardCoordinatorBootstrap, ShardCoordinatorMsg, ShardRegionActor, ShardRegionMsg,
    ShardRegionSnapshot,
};

use crate::reply::spawn_one_shot_reply;

use super::next_reply_id;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GracefulRegionShutdownObservation {
    pub shard: String,
    pub from_region: String,
    pub to_region: String,
    pub shutdown_started: bool,
    pub old_owner_has_shard: bool,
    pub new_owner_has_shard: bool,
}

pub fn run_local_graceful_region_shutdown(
    system_name: &str,
) -> Result<GracefulRegionShutdownObservation, Box<dyn Error>> {
    let system = ActorSystem::builder(system_name).build()?;
    let shard = "shard-1".to_string();
    let from_region = "region-a".to_string();
    let to_region = "region-b".to_string();
    let region_a = system.spawn(
        "region-a",
        ShardRegionActor::<String>::props_with_local_remember_store_shards(
            from_region.clone(),
            "orders",
            10,
            10,
            BTreeMap::from([(shard.clone(), BTreeSet::from(["entity-1".to_string()]))]),
            Duration::from_millis(500),
        ),
    )?;
    let region_b = system.spawn(
        "region-b",
        ShardRegionActor::<String>::props_with_local_remember_store_shards(
            to_region.clone(),
            "orders",
            10,
            10,
            BTreeMap::new(),
            Duration::from_millis(500),
        ),
    )?;

    let id = next_reply_id();
    let (host_reply_to, host_replies) =
        spawn_one_shot_reply::<HostShardPlan<String>>(&system, format!("host-shard-{id}"))?;
    region_a.tell(ShardRegionMsg::HostShard {
        shard: shard.clone(),
        reply_to: host_reply_to,
    })?;
    let _ = host_replies.recv_timeout(Duration::from_secs(2))?;

    let bootstrap = ShardCoordinatorBootstrap::local_regions([
        HandoffRegionTarget::new(from_region.clone(), region_a.clone()),
        HandoffRegionTarget::new(to_region.clone(), region_b.clone()),
    ])?;
    let (mut state, transport) = bootstrap.into_parts();
    state.apply(CoordinatorEvent::ShardHomeAllocated {
        shard: shard.clone(),
        region: from_region.clone(),
    })?;
    let coordinator = system.spawn(
        "coordinator",
        ShardCoordinatorActor::props_with_handoff(
            state,
            LeastShardAllocationStrategy::default(),
            "stop".to_string(),
            Duration::from_millis(500),
            transport,
        ),
    )?;

    let id = next_reply_id();
    let (shutdown_reply_to, shutdown_replies) =
        spawn_one_shot_reply::<RegionShutdownPlan>(&system, format!("shutdown-region-{id}"))?;
    coordinator.tell(ShardCoordinatorMsg::GracefulShutdownReq {
        region: from_region.clone(),
        reply_to: Some(shutdown_reply_to),
    })?;
    let shutdown_started = matches!(
        shutdown_replies.recv_timeout(Duration::from_secs(2))?,
        RegionShutdownPlan::Started { .. }
    );

    let coordinator_state = wait_for_coordinator_shard_owner(
        &system,
        &coordinator,
        &shard,
        &to_region,
        Duration::from_secs(2),
    )?;
    let old_owner_has_shard = coordinator_state
        .allocations
        .get(&from_region)
        .is_some_and(|shards| shards.contains(&shard));
    let new_owner_has_shard =
        wait_for_region_shard(&system, &region_b, &shard, Duration::from_secs(2))?;
    let observation = GracefulRegionShutdownObservation {
        shard,
        from_region,
        to_region,
        shutdown_started,
        old_owner_has_shard,
        new_owner_has_shard,
    };

    system.terminate(Duration::from_secs(1))?;
    Ok(observation)
}

fn wait_for_coordinator_shard_owner(
    system: &ActorSystem,
    coordinator: &ActorRef<ShardCoordinatorMsg<String>>,
    shard: &str,
    expected_region: &str,
    timeout: Duration,
) -> Result<CoordinatorStateSnapshot, Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        let id = next_reply_id();
        let (reply_to, replies) = spawn_one_shot_reply(system, format!("coordinator-state-{id}"))?;
        coordinator.tell(ShardCoordinatorMsg::GetState { reply_to })?;
        let snapshot = replies.recv_timeout(Duration::from_millis(100))?;
        if snapshot
            .allocations
            .get(expected_region)
            .is_some_and(|shards| shards.contains(&shard.to_string()))
        {
            return Ok(snapshot);
        }
        if Instant::now() >= deadline {
            return Err(
                format!("timed out waiting for shard `{shard}` owner `{expected_region}`").into(),
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_region_shard(
    system: &ActorSystem,
    region: &ActorRef<ShardRegionMsg<String>>,
    shard: &str,
    timeout: Duration,
) -> Result<bool, Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        let id = next_reply_id();
        let (reply_to, replies) = spawn_one_shot_reply(system, format!("region-state-{id}"))?;
        region.tell(ShardRegionMsg::GetState { reply_to })?;
        let snapshot: ShardRegionSnapshot = replies.recv_timeout(Duration::from_millis(100))?;
        if snapshot.local_shards.contains(shard) {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(10));
    }
}
