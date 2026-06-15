use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

use kairo::prelude::*;

use crate::counter::{CounterCmd, spawn_counter};

#[derive(Debug, Clone, PartialEq)]
pub struct ConfiguredCounterObservation {
    pub value: i64,
    pub dispatcher_throughput: usize,
    pub dead_letter_diagnostics_published: bool,
    pub remote_hostname: String,
    pub remote_port: u16,
    pub remote_connect_timeout: Option<Duration>,
    pub sharding_shards: u64,
    pub remember_entities: bool,
    pub sharding_allocation_absolute_limit: usize,
    pub sharding_allocation_relative_limit: f64,
    pub sharding_retry_interval: Duration,
    pub sharding_handoff_timeout: Duration,
    pub sharding_failure_backoff: Duration,
    pub sharding_rebalance_interval: Duration,
    pub sharding_query_timeout: Duration,
}

struct DeadLetterForwarder {
    observed: mpsc::Sender<DeadLetter>,
}

impl Actor for DeadLetterForwarder {
    type Msg = DeadLetter;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.observed
            .send(msg)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

pub fn run_configured_counter(
    system_name: impl Into<String>,
    config_path: impl AsRef<Path>,
    initial_value: i64,
    timeout: Duration,
) -> Result<ConfiguredCounterObservation, Box<dyn std::error::Error>> {
    let settings = load_toml_file(config_path)?;
    run_configured_counter_with_settings(system_name, settings, initial_value, timeout)
}

pub fn run_configured_counter_layers<I, P>(
    system_name: impl Into<String>,
    config_paths: I,
    initial_value: i64,
    timeout: Duration,
) -> Result<ConfiguredCounterObservation, Box<dyn std::error::Error>>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
{
    let settings = load_toml_files(config_paths)?;
    run_configured_counter_with_settings(system_name, settings, initial_value, timeout)
}

fn run_configured_counter_with_settings(
    system_name: impl Into<String>,
    settings: KairoSettings,
    initial_value: i64,
    timeout: Duration,
) -> Result<ConfiguredCounterObservation, Box<dyn std::error::Error>> {
    let dead_letter_diagnostics_enabled = settings.observability.diagnostics.dead_letters;
    let system = settings.actor_system_builder(system_name.into())?.build()?;
    let (dead_letter_tx, dead_letter_rx) = mpsc::channel();
    let dead_letter_subscriber = system.spawn(
        "dead-letter-diagnostics",
        Props::new(move || DeadLetterForwarder {
            observed: dead_letter_tx,
        }),
    )?;
    system.event_stream().subscribe(dead_letter_subscriber);
    let missing: ActorRef<CounterCmd> =
        system.missing_ref(format!("{}/user/missing-diagnostics#404", system.address()));
    let _ = missing.tell(CounterCmd::Increment);

    let counter = spawn_counter(&system, "counter", initial_value)?;
    let (reply_to, replies) = mpsc::channel();

    let result = (|| {
        counter.tell(CounterCmd::Increment)?;
        counter.tell(CounterCmd::Get { reply_to })?;

        let value = replies.recv_timeout(timeout)?;
        let allocation_strategy = settings
            .cluster
            .sharding
            .to_least_shard_allocation_strategy()?;
        let dead_letter_diagnostics_published = if dead_letter_diagnostics_enabled {
            dead_letter_rx.recv_timeout(timeout).is_ok()
        } else {
            dead_letter_rx
                .recv_timeout(Duration::from_millis(100))
                .is_ok()
        };
        Ok(ConfiguredCounterObservation {
            value,
            dispatcher_throughput: system.dispatcher_settings().throughput(),
            dead_letter_diagnostics_published,
            remote_hostname: settings.remote.transport.canonical_hostname.clone(),
            remote_port: settings.remote.transport.canonical_port,
            remote_connect_timeout: settings.remote.transport.connect_timeout,
            sharding_shards: settings.cluster.sharding.to_shard_count()?,
            remember_entities: settings.cluster.sharding.remember_entities_enabled(),
            sharding_allocation_absolute_limit: allocation_strategy.absolute_limit(),
            sharding_allocation_relative_limit: allocation_strategy.relative_limit(),
            sharding_retry_interval: settings.cluster.sharding.to_retry_interval()?,
            sharding_handoff_timeout: settings.cluster.sharding.to_handoff_timeout()?,
            sharding_failure_backoff: settings.cluster.sharding.to_shard_failure_backoff()?,
            sharding_rebalance_interval: settings.cluster.sharding.to_rebalance_interval()?,
            sharding_query_timeout: settings.cluster.sharding.to_shard_region_query_timeout()?,
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
