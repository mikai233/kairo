use std::sync::mpsc;
use std::time::Duration;

use kairo::cluster_sharding::PassivatePlan;
use kairo::prelude::*;
use kairo_examples::configured_counter::run_configured_counter;
use kairo_examples::counter::{CounterCmd, spawn_counter};
use kairo_examples::patterns::{PatternObservation, run_ask_pipe_to_self};
use kairo_examples::sharding_local::{LocalShardingExample, run_local_graceful_region_shutdown};

#[test]
fn local_counter_example_smoke() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder("example-smoke-counter").build()?;
    let counter = spawn_counter(&system, "counter", 1)?;
    let (reply_to, replies) = mpsc::channel();

    counter.tell(CounterCmd::Increment)?;
    counter.tell(CounterCmd::Get { reply_to })?;

    assert_eq!(replies.recv_timeout(Duration::from_secs(1))?, 2);

    counter.tell(CounterCmd::Stop)?;
    assert!(counter.wait_for_stop(Duration::from_secs(1)));
    system.terminate(Duration::from_secs(1))?;
    Ok(())
}

#[test]
fn configured_counter_example_smoke() -> Result<(), Box<dyn std::error::Error>> {
    let observation = run_configured_counter(
        "example-smoke-configured-counter",
        example_config_path(),
        10,
        Duration::from_secs(1),
    )?;

    assert_eq!(observation.value, 11);
    assert_eq!(observation.dispatcher_throughput, 2);
    Ok(())
}

#[test]
fn ask_pipe_to_self_example_smoke() -> Result<(), Box<dyn std::error::Error>> {
    let observations = run_ask_pipe_to_self("example-smoke-patterns", 7)?;

    assert!(observations.contains(&PatternObservation::AskCompleted {
        input: 7,
        output: 14,
    }));
    assert!(observations.contains(&PatternObservation::PipeCompleted {
        input: 7,
        output: 10,
    }));
    Ok(())
}

#[test]
fn cluster_sharding_local_example_smoke() -> Result<(), Box<dyn std::error::Error>> {
    let sharding = LocalShardingExample::start("example-smoke-sharding")?;
    let entity = sharding.entity_ref("counter-smoke");

    entity.tell("increment".to_string())?;
    entity.tell("increment".to_string())?;

    let observation = sharding.wait_for_entity_value("counter-smoke", 2, Duration::from_secs(2))?;
    assert_eq!(observation.value, 2);

    let snapshot = sharding.wait_for_active_entity("counter-smoke", Duration::from_secs(2))?;
    assert_eq!(snapshot.entity_count, 1);
    assert_eq!(snapshot.active_entities, vec!["counter-smoke".to_string()]);

    sharding.shutdown(Duration::from_secs(1))?;
    Ok(())
}

#[test]
fn cluster_sharding_local_example_passivates_and_restarts_entity()
-> Result<(), Box<dyn std::error::Error>> {
    let sharding = LocalShardingExample::start("example-smoke-sharding-passivation")?;
    let entity = sharding.entity_ref("counter-passivate");

    entity.tell("increment".to_string())?;
    let first = sharding.wait_for_entity_value("counter-passivate", 1, Duration::from_secs(2))?;
    assert_eq!(first.value, 1);

    let passivation = sharding.passivate_entity("counter-passivate", Duration::from_secs(2))?;
    assert!(matches!(
        passivation,
        PassivatePlan::SendStop { entity_id, .. } if entity_id == "counter-passivate"
    ));

    let stopped = sharding.wait_for_inactive_entity("counter-passivate", Duration::from_secs(2))?;
    assert_eq!(stopped.entity_count, 0);
    assert!(stopped.active_entities.is_empty());

    entity.tell("increment".to_string())?;
    let restarted =
        sharding.wait_for_entity_value("counter-passivate", 1, Duration::from_secs(2))?;
    assert_eq!(restarted.value, 1);

    let snapshot = sharding.wait_for_active_entity("counter-passivate", Duration::from_secs(2))?;
    assert_eq!(snapshot.entity_count, 1);

    sharding.shutdown(Duration::from_secs(1))?;
    Ok(())
}

#[test]
fn cluster_sharding_local_example_gracefully_moves_region_shard()
-> Result<(), Box<dyn std::error::Error>> {
    let observation =
        run_local_graceful_region_shutdown("example-smoke-sharding-graceful-shutdown")?;

    assert_eq!(observation.shard, "shard-1");
    assert_eq!(observation.from_region, "region-a");
    assert_eq!(observation.to_region, "region-b");
    assert!(observation.shutdown_started);
    assert!(!observation.old_owner_has_shard);
    assert!(observation.new_owner_has_shard);
    Ok(())
}

fn example_config_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/kairo.local.toml")
}
