use std::sync::mpsc;
use std::time::Duration;

use kairo::prelude::*;
use kairo_examples::configured_counter::run_configured_counter;
use kairo_examples::counter::{CounterCmd, spawn_counter};
use kairo_examples::patterns::{PatternObservation, run_ask_pipe_to_self};
use kairo_examples::sharding_local::LocalShardingExample;

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

fn example_config_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/kairo.local.toml")
}
