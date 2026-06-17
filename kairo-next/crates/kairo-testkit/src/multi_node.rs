use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use kairo_actor::{Actor, ActorError, ActorRef, ActorSystem, DeadLetter, Props};

use crate::{ActorSystemTestKit, ManualTime, TestProbe};

/// Result type returned by multi-node testkit helpers.
pub type MultiNodeResult<T> = std::result::Result<T, MultiNodeError>;

/// Errors reported by the local multi-node test harness.
#[derive(Debug)]
pub enum MultiNodeError {
    /// No nodes were supplied when constructing the harness.
    EmptyNodeSet,
    /// One supplied node name was empty, whitespace-only, or had surrounding whitespace.
    InvalidNodeName(String),
    /// The same node name was supplied more than once.
    DuplicateNode(String),
    /// A helper was asked to operate on a node name that is not part of the harness.
    UnknownNode(String),
    /// Manual time was requested for a node that was built with the real scheduler.
    ManualTimeDisabled(String),
    /// A node entered a different barrier while another named barrier is active.
    WrongBarrier {
        expected: String,
        actual: String,
        node: String,
    },
    /// The same node entered the same active barrier more than once.
    DuplicateBarrierArrival { name: String, node: String },
    /// A blocking barrier wait timed out before every node arrived.
    BarrierTimeout {
        name: String,
        node: String,
        timeout: Duration,
        arrived: BTreeSet<String>,
        remaining: BTreeSet<String>,
    },
    /// A previous wrong-order barrier entry failed the active barrier sequence.
    BarrierFailed {
        name: String,
        node: String,
        reason: String,
        arrived: BTreeSet<String>,
        remaining: BTreeSet<String>,
    },
    /// The shared barrier state lock was poisoned by a panic in another thread.
    PoisonedBarrier,
    /// An underlying actor-system operation failed.
    Actor(ActorError),
}

impl Display for MultiNodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyNodeSet => write!(f, "multi-node testkit requires at least one node"),
            Self::InvalidNodeName(name) => {
                write!(f, "invalid multi-node testkit node name `{name}`")
            }
            Self::DuplicateNode(name) => write!(f, "duplicate multi-node testkit node `{name}`"),
            Self::UnknownNode(name) => write!(f, "unknown multi-node testkit node `{name}`"),
            Self::ManualTimeDisabled(name) => {
                write!(
                    f,
                    "multi-node testkit node `{name}` was not built with manual time"
                )
            }
            Self::WrongBarrier {
                expected,
                actual,
                node,
            } => {
                write!(
                    f,
                    "multi-node testkit node `{node}` entered barrier `{actual}` while `{expected}` is active"
                )
            }
            Self::DuplicateBarrierArrival { name, node } => {
                write!(
                    f,
                    "multi-node testkit node `{node}` entered barrier `{name}` more than once"
                )
            }
            Self::BarrierTimeout {
                name,
                node,
                timeout,
                arrived,
                remaining,
            } => {
                write!(
                    f,
                    "multi-node testkit node `{node}` timed out after {timeout:?} waiting for barrier `{name}`; arrived: {arrived:?}, remaining: {remaining:?}"
                )
            }
            Self::BarrierFailed {
                name,
                node,
                reason,
                arrived,
                remaining,
            } => {
                write!(
                    f,
                    "multi-node testkit node `{node}` failed barrier `{name}` after {reason}; arrived: {arrived:?}, remaining: {remaining:?}"
                )
            }
            Self::PoisonedBarrier => write!(f, "multi-node testkit barrier state is poisoned"),
            Self::Actor(error) => Display::fmt(error, f),
        }
    }
}

impl std::error::Error for MultiNodeError {}

impl From<ActorError> for MultiNodeError {
    fn from(error: ActorError) -> Self {
        Self::Actor(error)
    }
}

/// Local multi-node harness built from named actor-system testkits.
///
/// `MultiNodeTestKit` is intentionally local: it gives tests several named
/// actor systems, optional per-node manual time, node-local probe/actor
/// helpers, and in-process barriers without pretending to implement cluster
/// membership. Cluster and sharding tests can layer their real protocols on
/// top of these deterministic node fixtures.
#[derive(Debug)]
pub struct MultiNodeTestKit {
    nodes: Vec<MultiNode>,
    barriers: Mutex<BarrierState>,
    barrier_changed: Condvar,
}

impl MultiNodeTestKit {
    /// Creates one local [`ActorSystemTestKit`] per named node.
    ///
    /// The node names must be non-empty, trimmed, and unique. Systems created
    /// this way use the normal scheduler; use [`Self::with_manual_time`] when
    /// tests need deterministic clock advancement across every node.
    pub fn new<I, S>(node_names: I) -> MultiNodeResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::build(node_names, false)
    }

    /// Creates one local node per name with manual time enabled for each node.
    ///
    /// The returned harness can advance all node clocks with [`Self::advance_all`],
    /// step them to the next shared deadline with [`Self::advance_all_to_next`],
    /// drain them with [`Self::advance_all_until_idle`], or access one node's
    /// [`ManualTime`] handle through [`Self::manual_time`].
    pub fn with_manual_time<I, S>(node_names: I) -> MultiNodeResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::build(node_names, true)
    }

    /// Returns the number of local actor-system nodes owned by the harness.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns whether the harness owns no nodes.
    ///
    /// Constructed harnesses are never empty because construction rejects an
    /// empty node set, but this mirrors the slice-style API next to [`Self::len`].
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Returns all nodes in construction order.
    pub fn nodes(&self) -> &[MultiNode] {
        &self.nodes
    }

    /// Iterates over node names in construction order.
    pub fn node_names(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(|node| node.name())
    }

    /// Looks up a node by name.
    pub fn node(&self, name: impl AsRef<str>) -> MultiNodeResult<&MultiNode> {
        let name = name.as_ref();
        self.nodes
            .iter()
            .find(|node| node.name == name)
            .ok_or_else(|| MultiNodeError::UnknownNode(name.to_string()))
    }

    /// Returns the [`ActorSystemTestKit`] owned by a named node.
    pub fn kit(&self, name: impl AsRef<str>) -> MultiNodeResult<&ActorSystemTestKit> {
        Ok(self.node(name)?.kit())
    }

    /// Returns the local actor system owned by a named node.
    pub fn system(&self, name: impl AsRef<str>) -> MultiNodeResult<&ActorSystem> {
        Ok(self.node(name)?.system())
    }

    /// Returns the manual-time controller for a named node.
    ///
    /// Returns [`MultiNodeError::ManualTimeDisabled`] when the harness was built
    /// with [`Self::new`] instead of [`Self::with_manual_time`].
    pub fn manual_time(&self, name: impl AsRef<str>) -> MultiNodeResult<&ManualTime> {
        let node = self.node(name)?;
        node.manual_time()
            .ok_or_else(|| MultiNodeError::ManualTimeDisabled(node.name().to_string()))
    }

    /// Advances every node's manual scheduler by the same duration.
    ///
    /// All nodes must have manual time enabled. This helper is useful for
    /// multi-node scenarios that need all local systems to observe a timer tick
    /// without giving each node a separate timeout budget.
    pub fn advance_all(&self, duration: Duration) -> MultiNodeResult<()> {
        for node in &self.nodes {
            let manual_time = node
                .manual_time()
                .ok_or_else(|| MultiNodeError::ManualTimeDisabled(node.name().to_string()))?;
            manual_time.advance(duration);
        }
        Ok(())
    }

    /// Advances every node's manual scheduler by the smallest next due duration.
    ///
    /// Returns `false` when no node has active scheduled work. Otherwise every
    /// node clock advances by the same duration, and at least one node reaches
    /// its next scheduled deadline.
    pub fn advance_all_to_next(&self) -> MultiNodeResult<bool> {
        let manual_times = self.manual_times()?;
        let Some(next_delta) = Self::next_shared_delta(&manual_times) else {
            return Ok(false);
        };

        for manual_time in manual_times {
            manual_time.advance(next_delta);
        }
        Ok(true)
    }

    /// Advances every node's manual scheduler until idle or `max_steps` is reached.
    ///
    /// Returns `true` only when every manual-time node is idle after the bounded
    /// advancement. Repeated timers can keep a node non-idle, so callers must
    /// provide a bound.
    pub fn advance_all_until_idle(&self, max_steps: usize) -> MultiNodeResult<bool> {
        let manual_times = self.manual_times()?;
        for _ in 0..max_steps {
            let Some(next_delta) = Self::next_shared_delta(&manual_times) else {
                return Ok(true);
            };
            for manual_time in &manual_times {
                manual_time.advance(next_delta);
            }
        }
        Ok(Self::next_shared_delta(&manual_times).is_none())
    }

    /// Marks one node as having entered a named barrier without blocking.
    ///
    /// The returned status is [`MultiNodeBarrierStatus::Waiting`] until every
    /// node in the harness has entered the same barrier, and
    /// [`MultiNodeBarrierStatus::Passed`] for the arrival that completes it.
    /// Only one barrier may be active at a time.
    pub fn enter_barrier(
        &self,
        name: impl Into<String>,
        node_name: impl AsRef<str>,
    ) -> MultiNodeResult<MultiNodeBarrierStatus> {
        let name = name.into();
        let node_name = node_name.as_ref();
        self.node(node_name)?;

        let mut barriers = self
            .barriers
            .lock()
            .map_err(|_| MultiNodeError::PoisonedBarrier)?;
        let entered = match barriers.enter(name, node_name, self.expected_barrier_nodes()) {
            Ok(entered) => entered,
            Err(error) => {
                if matches!(error, MultiNodeError::WrongBarrier { .. }) {
                    self.barrier_changed.notify_all();
                }
                return Err(error);
            }
        };
        if entered.status.passed() {
            self.barrier_changed.notify_all();
        }
        Ok(entered.status)
    }

    /// Enters a named barrier and waits until all nodes arrive or the timeout expires.
    ///
    /// This is the blocking counterpart to [`Self::enter_barrier`]. Timeout
    /// errors include the nodes that arrived and the nodes still missing when
    /// the wait budget expired.
    pub fn await_barrier(
        &self,
        name: impl Into<String>,
        node_name: impl AsRef<str>,
        timeout: Duration,
    ) -> MultiNodeResult<MultiNodeBarrierStatus> {
        let name = name.into();
        let node_name = node_name.as_ref().to_string();
        self.node(&node_name)?;

        let deadline = Instant::now() + timeout;
        let mut barriers = self
            .barriers
            .lock()
            .map_err(|_| MultiNodeError::PoisonedBarrier)?;
        let entered = match barriers.enter(name.clone(), &node_name, self.expected_barrier_nodes())
        {
            Ok(entered) => entered,
            Err(error) => {
                if matches!(error, MultiNodeError::WrongBarrier { .. }) {
                    self.barrier_changed.notify_all();
                }
                return Err(error);
            }
        };
        let barrier_id = entered.id;
        if entered.status.passed() {
            self.barrier_changed.notify_all();
            return Ok(entered.status);
        }

        loop {
            if let Some(status) = barriers.completed_status(barrier_id) {
                return Ok(status);
            }
            if let Some((reason, arrived, remaining)) = barriers.failed_snapshot(barrier_id) {
                return Err(MultiNodeError::BarrierFailed {
                    name,
                    node: node_name,
                    reason,
                    arrived,
                    remaining,
                });
            }

            let remaining_timeout = deadline.saturating_duration_since(Instant::now());
            if remaining_timeout.is_zero() {
                let (arrived, remaining) = barriers.waiting_snapshot(barrier_id);
                return Err(MultiNodeError::BarrierTimeout {
                    name,
                    node: node_name,
                    timeout,
                    arrived,
                    remaining,
                });
            }

            let (next_barriers, wait_result) = self
                .barrier_changed
                .wait_timeout(barriers, remaining_timeout)
                .map_err(|_| MultiNodeError::PoisonedBarrier)?;
            barriers = next_barriers;

            if wait_result.timed_out() {
                if let Some(status) = barriers.completed_status(barrier_id) {
                    return Ok(status);
                }
                if let Some((reason, arrived, remaining)) = barriers.failed_snapshot(barrier_id) {
                    return Err(MultiNodeError::BarrierFailed {
                        name,
                        node: node_name,
                        reason,
                        arrived,
                        remaining,
                    });
                }
                let (arrived, remaining) = barriers.waiting_snapshot(barrier_id);
                return Err(MultiNodeError::BarrierTimeout {
                    name,
                    node: node_name,
                    timeout,
                    arrived,
                    remaining,
                });
            }
        }
    }

    /// Runs several barriers in order under one shared timeout budget.
    ///
    /// Each barrier consumes time from the same deadline. This mirrors
    /// Pekko-style sequential multi-node phases while returning explicit
    /// per-barrier statuses to Rust tests.
    pub fn await_barriers<I, S>(
        &self,
        names: I,
        node_name: impl AsRef<str>,
        timeout: Duration,
    ) -> MultiNodeResult<Vec<MultiNodeBarrierStatus>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let names = names.into_iter().map(Into::into).collect::<Vec<_>>();
        let node_name = node_name.as_ref().to_string();
        let deadline = Instant::now() + timeout;
        let mut statuses = Vec::with_capacity(names.len());

        for name in names {
            let remaining_timeout = deadline.saturating_duration_since(Instant::now());
            statuses.push(self.await_barrier(name, &node_name, remaining_timeout)?);
        }

        Ok(statuses)
    }

    /// Creates a typed probe actor on a named node.
    pub fn create_probe_on<M>(
        &self,
        node_name: impl AsRef<str>,
        probe_name: impl AsRef<str>,
    ) -> MultiNodeResult<TestProbe<M>>
    where
        M: Send + 'static,
    {
        Ok(self.node(node_name)?.kit().create_probe(probe_name)?)
    }

    /// Spawns a typed user actor under `/user` on a named node.
    pub fn spawn_on<A>(
        &self,
        node_name: impl AsRef<str>,
        actor_name: impl AsRef<str>,
        props: Props<A>,
    ) -> MultiNodeResult<ActorRef<A::Msg>>
    where
        A: Actor,
    {
        Ok(self.node(node_name)?.system().spawn(actor_name, props)?)
    }

    /// Spawns a framework-owned actor under `/system` on a named node.
    ///
    /// This is intended for integration tests around remoting, cluster,
    /// distributed-data, sharding, and cluster tools, where framework services
    /// should not occupy the user guardian namespace.
    pub fn spawn_system_on<A>(
        &self,
        node_name: impl AsRef<str>,
        actor_name: impl AsRef<str>,
        props: Props<A>,
    ) -> MultiNodeResult<ActorRef<A::Msg>>
    where
        A: Actor,
    {
        Ok(self
            .node(node_name)?
            .system()
            .spawn_system(actor_name, props)?)
    }

    /// Creates and subscribes a typed event-stream probe on a named node.
    pub fn create_event_probe_on<M>(
        &self,
        node_name: impl AsRef<str>,
        probe_name: impl AsRef<str>,
    ) -> MultiNodeResult<TestProbe<M>>
    where
        M: Clone + Send + 'static,
    {
        Ok(self.node(node_name)?.kit().create_event_probe(probe_name)?)
    }

    /// Creates and subscribes a dead-letter event probe on a named node.
    pub fn create_dead_letter_probe_on(
        &self,
        node_name: impl AsRef<str>,
        probe_name: impl AsRef<str>,
    ) -> MultiNodeResult<TestProbe<DeadLetter>> {
        self.create_event_probe_on(node_name, probe_name)
    }

    /// Terminates every node-owned actor system.
    ///
    /// Shutdown continues across all nodes and returns the first termination
    /// error observed, if any.
    pub fn shutdown(self, timeout: Duration) -> MultiNodeResult<()> {
        let mut first_error = None;
        for node in self.nodes {
            if let Err(error) = node.shutdown(timeout)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn build<I, S>(node_names: I, manual_time: bool) -> MultiNodeResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let names = node_names.into_iter().map(Into::into).collect::<Vec<_>>();
        validate_node_names(&names)?;

        let mut nodes = Vec::with_capacity(names.len());
        for name in names {
            let node = if manual_time {
                let (kit, time) = ActorSystemTestKit::with_manual_time(name.clone())?;
                MultiNode::new(name, kit, Some(time))
            } else {
                let kit = ActorSystemTestKit::new(name.clone())?;
                MultiNode::new(name, kit, None)
            };
            nodes.push(node);
        }

        Ok(Self {
            nodes,
            barriers: Mutex::new(BarrierState::default()),
            barrier_changed: Condvar::new(),
        })
    }

    fn expected_barrier_nodes(&self) -> BTreeSet<String> {
        self.nodes.iter().map(|node| node.name.clone()).collect()
    }

    fn manual_times(&self) -> MultiNodeResult<Vec<&ManualTime>> {
        self.nodes
            .iter()
            .map(|node| {
                node.manual_time()
                    .ok_or_else(|| MultiNodeError::ManualTimeDisabled(node.name().to_string()))
            })
            .collect()
    }

    fn next_shared_delta(manual_times: &[&ManualTime]) -> Option<Duration> {
        manual_times
            .iter()
            .filter_map(|manual_time| {
                manual_time
                    .next_deadline()
                    .map(|deadline| deadline.saturating_sub(manual_time.now()))
            })
            .min()
    }
}

#[derive(Debug)]
pub struct MultiNode {
    name: String,
    kit: ActorSystemTestKit,
    manual_time: Option<ManualTime>,
}

impl MultiNode {
    fn new(name: String, kit: ActorSystemTestKit, manual_time: Option<ManualTime>) -> Self {
        Self {
            name,
            kit,
            manual_time,
        }
    }

    /// Returns the stable name used to identify this local node in the harness.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the actor-system testkit owned by this node.
    pub fn kit(&self) -> &ActorSystemTestKit {
        &self.kit
    }

    /// Returns the local actor system owned by this node.
    pub fn system(&self) -> &ActorSystem {
        self.kit.system()
    }

    /// Returns the node's manual-time controller when manual time is enabled.
    pub fn manual_time(&self) -> Option<&ManualTime> {
        self.manual_time.as_ref()
    }

    fn shutdown(self, timeout: Duration) -> MultiNodeResult<()> {
        self.kit.shutdown(timeout)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiNodeBarrierStatus {
    /// The barrier is active and not every node has arrived.
    Waiting {
        name: String,
        arrived: BTreeSet<String>,
        remaining: BTreeSet<String>,
    },
    /// The barrier completed after every participant arrived.
    Passed {
        name: String,
        participants: BTreeSet<String>,
    },
}

impl MultiNodeBarrierStatus {
    /// Returns the barrier name associated with this status.
    pub fn name(&self) -> &str {
        match self {
            Self::Waiting { name, .. } | Self::Passed { name, .. } => name,
        }
    }

    /// Returns whether the barrier has passed.
    pub fn passed(&self) -> bool {
        matches!(self, Self::Passed { .. })
    }
}

#[derive(Debug, Default)]
struct BarrierState {
    active: Option<ActiveBarrier>,
    completed: Vec<CompletedBarrier>,
    failed: Vec<FailedBarrier>,
    sequence_failure: Option<String>,
    next_id: u64,
}

impl BarrierState {
    fn enter(
        &mut self,
        name: String,
        node: &str,
        participants: BTreeSet<String>,
    ) -> MultiNodeResult<EnteredBarrier> {
        if let Some(reason) = &self.sequence_failure {
            return Err(MultiNodeError::BarrierFailed {
                name,
                node: node.to_string(),
                reason: reason.clone(),
                arrived: BTreeSet::new(),
                remaining: participants,
            });
        }

        if self.active.is_none() {
            let id = self.next_id;
            self.next_id += 1;
            self.active = Some(ActiveBarrier {
                id,
                name: name.clone(),
                participants,
                arrived: BTreeSet::new(),
            });
        }

        if self
            .active
            .as_ref()
            .is_some_and(|active| active.name != name)
        {
            let failed = self
                .active
                .take()
                .expect("active barrier must exist when failing wrong barrier");
            let reason = format!(
                "node `{node}` entered barrier `{name}` while `{}` is active",
                failed.name
            );
            let remaining = failed
                .participants
                .difference(&failed.arrived)
                .cloned()
                .collect();
            self.failed.push(FailedBarrier {
                id: failed.id,
                arrived: failed.arrived,
                remaining,
                reason: reason.clone(),
            });
            self.sequence_failure = Some(reason);
            return Err(MultiNodeError::WrongBarrier {
                expected: failed.name,
                actual: name,
                node: node.to_string(),
            });
        }

        let active = self
            .active
            .as_mut()
            .expect("active barrier must exist after setup");
        let id = active.id;

        if !active.arrived.insert(node.to_string()) {
            return Err(MultiNodeError::DuplicateBarrierArrival {
                name,
                node: node.to_string(),
            });
        }

        if active.arrived == active.participants {
            let completed = self
                .active
                .take()
                .expect("active barrier must exist when completing");
            self.completed.push(CompletedBarrier {
                id: completed.id,
                name: completed.name.clone(),
                participants: completed.participants.clone(),
            });
            Ok(EnteredBarrier {
                id,
                status: MultiNodeBarrierStatus::Passed {
                    name: completed.name,
                    participants: completed.participants,
                },
            })
        } else {
            let remaining = active
                .participants
                .difference(&active.arrived)
                .cloned()
                .collect();
            Ok(EnteredBarrier {
                id,
                status: MultiNodeBarrierStatus::Waiting {
                    name: active.name.clone(),
                    arrived: active.arrived.clone(),
                    remaining,
                },
            })
        }
    }

    fn completed_status(&self, id: u64) -> Option<MultiNodeBarrierStatus> {
        self.completed
            .iter()
            .find(|completed| completed.id == id)
            .map(|completed| MultiNodeBarrierStatus::Passed {
                name: completed.name.clone(),
                participants: completed.participants.clone(),
            })
    }

    fn waiting_snapshot(&self, id: u64) -> (BTreeSet<String>, BTreeSet<String>) {
        let Some(active) = &self.active else {
            return (BTreeSet::new(), BTreeSet::new());
        };
        if active.id != id {
            return (BTreeSet::new(), BTreeSet::new());
        }
        let remaining = active
            .participants
            .difference(&active.arrived)
            .cloned()
            .collect();
        (active.arrived.clone(), remaining)
    }

    fn failed_snapshot(&self, id: u64) -> Option<(String, BTreeSet<String>, BTreeSet<String>)> {
        self.failed
            .iter()
            .find(|failed| failed.id == id)
            .map(|failed| {
                (
                    failed.reason.clone(),
                    failed.arrived.clone(),
                    failed.remaining.clone(),
                )
            })
    }
}

#[derive(Debug)]
struct ActiveBarrier {
    id: u64,
    name: String,
    participants: BTreeSet<String>,
    arrived: BTreeSet<String>,
}

#[derive(Debug)]
struct CompletedBarrier {
    id: u64,
    name: String,
    participants: BTreeSet<String>,
}

#[derive(Debug)]
struct FailedBarrier {
    id: u64,
    arrived: BTreeSet<String>,
    remaining: BTreeSet<String>,
    reason: String,
}

#[derive(Debug)]
struct EnteredBarrier {
    id: u64,
    status: MultiNodeBarrierStatus,
}

fn validate_node_names(names: &[String]) -> MultiNodeResult<()> {
    if names.is_empty() {
        return Err(MultiNodeError::EmptyNodeSet);
    }

    let mut seen = BTreeSet::new();
    for name in names {
        if name.trim().is_empty() || name.trim() != name {
            return Err(MultiNodeError::InvalidNodeName(name.clone()));
        }
        if !seen.insert(name) {
            return Err(MultiNodeError::DuplicateNode(name.clone()));
        }
    }

    Ok(())
}
