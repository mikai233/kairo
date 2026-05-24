use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use kairo::prelude::*;
use kairo_examples::counter::{CounterCmd, spawn_counter};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = load_toml_file(example_config_path())?;
    let system = settings
        .actor
        .actor_system_builder("configured-counter")?
        .build()?;
    let counter = spawn_counter(&system, "counter", 10)?;
    let (reply_to, replies) = mpsc::channel();

    counter.tell(CounterCmd::Increment)?;
    counter.tell(CounterCmd::Get { reply_to })?;

    let value = replies.recv_timeout(Duration::from_secs(1))?;
    println!(
        "counter value: {value}; dispatcher throughput: {}",
        system.dispatcher_settings().throughput()
    );

    counter.tell(CounterCmd::Stop)?;
    if !counter.wait_for_stop(Duration::from_secs(1)) {
        return Err("counter did not stop within one second".into());
    }
    system.terminate(Duration::from_secs(1))?;
    Ok(())
}

fn example_config_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/kairo.local.toml")
}
