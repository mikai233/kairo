use std::time::Duration;

use kairo::cluster_sharding::PassivatePlan;
use kairo_examples::sharding_local::{LocalShardingExample, run_local_graceful_region_shutdown};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sharding = LocalShardingExample::start("local-sharding")?;
    let counter = sharding.entity_ref("counter-1");

    counter.tell("increment".to_string())?;
    counter.tell("increment".to_string())?;

    let observed = sharding.wait_for_entity_value("counter-1", 2, Duration::from_secs(2))?;
    let snapshot = sharding.wait_for_active_entity("counter-1", Duration::from_secs(2))?;
    println!(
        "entity {} reached {} in shard {} with {} active entity(ies)",
        observed.entity_id, observed.value, snapshot.shard_id, snapshot.entity_count
    );

    let passivated = sharding.entity_ref("counter-passivate");
    passivated.tell("increment".to_string())?;
    let before_passivation =
        sharding.wait_for_entity_value("counter-passivate", 1, Duration::from_secs(2))?;
    let passivation = sharding.passivate_entity("counter-passivate", Duration::from_secs(2))?;
    let stopped = sharding.wait_for_inactive_entity("counter-passivate", Duration::from_secs(2))?;
    passivated.tell("increment".to_string())?;
    let restarted =
        sharding.wait_for_entity_value("counter-passivate", 1, Duration::from_secs(2))?;
    match passivation {
        PassivatePlan::SendStop { entity_id, .. } => println!(
            "entity {} passivated from value {} in shard {} and restarted at {}",
            entity_id, before_passivation.value, stopped.shard_id, restarted.value
        ),
        PassivatePlan::Ignored { entity_id, reason } => {
            println!("entity {entity_id} passivation ignored: {reason:?}")
        }
    }

    sharding.shutdown(Duration::from_secs(1))?;

    let graceful = run_local_graceful_region_shutdown("local-sharding-graceful-shutdown")?;
    println!(
        "graceful shutdown moved {} from {} to {} and recovered {:?}",
        graceful.shard, graceful.from_region, graceful.to_region, graceful.recovered_entities
    );
    Ok(())
}
