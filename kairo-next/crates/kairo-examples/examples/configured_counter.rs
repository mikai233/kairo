use std::path::PathBuf;
use std::time::Duration;

use kairo_examples::configured_counter::run_configured_counter_layers;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let observation = run_configured_counter_layers(
        "configured-counter",
        example_config_paths(),
        10,
        Duration::from_secs(1),
    )?;
    println!(
        "counter value: {}; dispatcher throughput: {}; dead-letter diagnostics: {}; remote: {}:{}; shards: {}",
        observation.value,
        observation.dispatcher_throughput,
        observation.dead_letter_diagnostics_published,
        observation.remote_hostname,
        observation.remote_port,
        observation.sharding_shards
    );
    Ok(())
}

fn example_config_paths() -> [PathBuf; 2] {
    let examples = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples");
    [
        examples.join("kairo.toml"),
        examples.join("kairo.local.toml"),
    ]
}
