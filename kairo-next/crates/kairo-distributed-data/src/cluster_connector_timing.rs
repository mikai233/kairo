use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const DEFAULT_PERIODIC_TASKS_INITIAL_DELAY: Duration = Duration::ZERO;

pub const CLOCK_TIMER_KEY: &str = "ddata-replicator-cluster-clock";
pub const PRUNING_TIMER_KEY: &str = "ddata-replicator-cluster-pruning";

pub trait ReplicatorClusterConnectorClock: Send + Sync + 'static {
    fn monotonic_nanos(&self) -> u64;
    fn wall_millis(&self) -> u64;
}

#[derive(Debug)]
pub struct SystemReplicatorClusterConnectorClock {
    started_at: Instant,
}

impl SystemReplicatorClusterConnectorClock {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }
}

impl Default for SystemReplicatorClusterConnectorClock {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicatorClusterConnectorClock for SystemReplicatorClusterConnectorClock {
    fn monotonic_nanos(&self) -> u64 {
        self.started_at
            .elapsed()
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64
    }

    fn wall_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorClusterConnectorTimingSettings {
    pub clock_interval: Option<Duration>,
    pub pruning_interval: Option<Duration>,
    pub periodic_tasks_initial_delay: Duration,
}

impl ReplicatorClusterConnectorTimingSettings {
    pub fn disabled() -> Self {
        Self {
            clock_interval: None,
            pruning_interval: None,
            periodic_tasks_initial_delay: DEFAULT_PERIODIC_TASKS_INITIAL_DELAY,
        }
    }

    pub fn new(clock_interval: Duration, pruning_interval: Duration) -> Self {
        Self {
            clock_interval: Some(clock_interval),
            pruning_interval: Some(pruning_interval),
            periodic_tasks_initial_delay: DEFAULT_PERIODIC_TASKS_INITIAL_DELAY,
        }
    }

    pub fn with_clock_interval(mut self, interval: Option<Duration>) -> Self {
        self.clock_interval = interval;
        self
    }

    pub fn with_pruning_interval(mut self, interval: Option<Duration>) -> Self {
        self.pruning_interval = interval;
        self
    }

    pub fn with_periodic_tasks_initial_delay(mut self, delay: Duration) -> Self {
        self.periodic_tasks_initial_delay = delay;
        self
    }
}

impl Default for ReplicatorClusterConnectorTimingSettings {
    fn default() -> Self {
        Self::disabled()
    }
}

pub type SharedReplicatorClusterConnectorClock =
    Arc<dyn ReplicatorClusterConnectorClock + Send + Sync>;
