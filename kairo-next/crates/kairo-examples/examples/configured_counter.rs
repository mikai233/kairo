use std::path::PathBuf;
use std::time::Duration;

use kairo_examples::configured_counter::run_configured_counter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let observation = run_configured_counter(
        "configured-counter",
        example_config_path(),
        10,
        Duration::from_secs(1),
    )?;
    println!(
        "counter value: {}; dispatcher throughput: {}",
        observation.value, observation.dispatcher_throughput
    );
    Ok(())
}

fn example_config_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/kairo.local.toml")
}
