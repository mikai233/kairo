use std::time::Duration;

use kairo_examples::configured_counter::run_configured_counter_standard;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let observation = run_configured_counter_standard(
        "configured-counter",
        example_config_dir(),
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

fn example_config_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples")
}
