#![deny(missing_docs)]
//! Clock and scheduling configuration for the actor-backed cluster connector.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const DEFAULT_PERIODIC_TASKS_INITIAL_DELAY: Duration = Duration::ZERO;

pub(crate) const CLOCK_TIMER_KEY: &str = "ddata-replicator-cluster-clock";
pub(crate) const PRUNING_TIMER_KEY: &str = "ddata-replicator-cluster-pruning";

/// Supplies monotonic and wall-clock time to connector lifecycle tasks.
pub trait ReplicatorClusterConnectorClock: Send + Sync + 'static {
    /// Returns a monotonic timestamp in nanoseconds for reachability duration accounting.
    fn monotonic_nanos(&self) -> u64;
    /// Returns milliseconds since the Unix epoch for pruning-marker expiry.
    fn wall_millis(&self) -> u64;
}

#[derive(Debug)]
/// Production connector clock backed by [`Instant`] and [`SystemTime`].
pub struct SystemReplicatorClusterConnectorClock {
    started_at: Instant,
}

impl SystemReplicatorClusterConnectorClock {
    /// Starts a new monotonic clock at the current instant.
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
/// Optional schedules for connector-owned reachability and pruning ticks.
///
/// Enabled intervals should be non-zero. The composed extension validates that
/// contract before actor construction; [`Self::disabled`] supports deterministic
/// manual driving and focused tests.
pub struct ReplicatorClusterConnectorTimingSettings {
    /// Interval for advancing the monotonic all-reachable clock.
    pub clock_interval: Option<Duration>,
    /// Interval for running removed-node pruning.
    pub pruning_interval: Option<Duration>,
    /// Delay before both periodic tasks first run.
    pub periodic_tasks_initial_delay: Duration,
}

impl ReplicatorClusterConnectorTimingSettings {
    /// Disables both connector-owned periodic tasks.
    pub fn disabled() -> Self {
        Self {
            clock_interval: None,
            pruning_interval: None,
            periodic_tasks_initial_delay: DEFAULT_PERIODIC_TASKS_INITIAL_DELAY,
        }
    }

    /// Enables both periodic tasks with explicit non-zero intervals.
    pub fn new(clock_interval: Duration, pruning_interval: Duration) -> Self {
        Self {
            clock_interval: Some(clock_interval),
            pruning_interval: Some(pruning_interval),
            periodic_tasks_initial_delay: DEFAULT_PERIODIC_TASKS_INITIAL_DELAY,
        }
    }

    /// Replaces or disables the monotonic clock interval.
    pub fn with_clock_interval(mut self, interval: Option<Duration>) -> Self {
        self.clock_interval = interval;
        self
    }

    /// Replaces or disables the removed-node pruning interval.
    pub fn with_pruning_interval(mut self, interval: Option<Duration>) -> Self {
        self.pruning_interval = interval;
        self
    }

    /// Sets the delay before either enabled periodic task first runs.
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

/// Shared connector clock suitable for actor construction and test substitution.
pub type SharedReplicatorClusterConnectorClock =
    Arc<dyn ReplicatorClusterConnectorClock + Send + Sync>;
