use std::time::Duration;

use kairo_examples::sharding_local::LocalShardingExample;

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

    sharding.shutdown(Duration::from_secs(1))?;
    Ok(())
}
