use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

use kairo::prelude::*;

use crate::counter::{CounterCmd, spawn_counter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredCounterObservation {
    pub value: i64,
    pub dispatcher_throughput: usize,
}

pub fn run_configured_counter(
    system_name: impl Into<String>,
    config_path: impl AsRef<Path>,
    initial_value: i64,
    timeout: Duration,
) -> Result<ConfiguredCounterObservation, Box<dyn std::error::Error>> {
    let settings = load_toml_file(config_path)?;
    let system = settings
        .actor
        .actor_system_builder(system_name.into())?
        .build()?;
    let counter = spawn_counter(&system, "counter", initial_value)?;
    let (reply_to, replies) = mpsc::channel();

    let result = (|| {
        counter.tell(CounterCmd::Increment)?;
        counter.tell(CounterCmd::Get { reply_to })?;

        let value = replies.recv_timeout(timeout)?;
        Ok(ConfiguredCounterObservation {
            value,
            dispatcher_throughput: system.dispatcher_settings().throughput(),
        })
    })();

    let _ = counter.tell(CounterCmd::Stop);
    if !counter.wait_for_stop(timeout) {
        system.terminate(timeout)?;
        return Err("counter did not stop within timeout".into());
    }
    system.terminate(timeout)?;
    result
}
