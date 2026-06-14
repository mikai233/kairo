use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use kairo_actor::{ActorError, ActorSystem};

use crate::{ActorSystemTestKit, ManualTime, TestProbe};

pub type MultiNodeResult<T> = std::result::Result<T, MultiNodeError>;

#[derive(Debug)]
pub enum MultiNodeError {
    EmptyNodeSet,
    DuplicateNode(String),
    UnknownNode(String),
    ManualTimeDisabled(String),
    WrongBarrier {
        expected: String,
        actual: String,
        node: String,
    },
    DuplicateBarrierArrival {
        name: String,
        node: String,
    },
    BarrierTimeout {
        name: String,
        node: String,
        timeout: Duration,
        arrived: BTreeSet<String>,
        remaining: BTreeSet<String>,
    },
    PoisonedBarrier,
    Actor(ActorError),
}

impl Display for MultiNodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyNodeSet => write!(f, "multi-node testkit requires at least one node"),
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

#[derive(Debug)]
pub struct MultiNodeTestKit {
    nodes: Vec<MultiNode>,
    barriers: Mutex<BarrierState>,
    barrier_changed: Condvar,
}

impl MultiNodeTestKit {
    pub fn new<I, S>(node_names: I) -> MultiNodeResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::build(node_names, false)
    }

    pub fn with_manual_time<I, S>(node_names: I) -> MultiNodeResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::build(node_names, true)
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn nodes(&self) -> &[MultiNode] {
        &self.nodes
    }

    pub fn node_names(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(|node| node.name())
    }

    pub fn node(&self, name: impl AsRef<str>) -> MultiNodeResult<&MultiNode> {
        let name = name.as_ref();
        self.nodes
            .iter()
            .find(|node| node.name == name)
            .ok_or_else(|| MultiNodeError::UnknownNode(name.to_string()))
    }

    pub fn kit(&self, name: impl AsRef<str>) -> MultiNodeResult<&ActorSystemTestKit> {
        Ok(self.node(name)?.kit())
    }

    pub fn system(&self, name: impl AsRef<str>) -> MultiNodeResult<&ActorSystem> {
        Ok(self.node(name)?.system())
    }

    pub fn manual_time(&self, name: impl AsRef<str>) -> MultiNodeResult<&ManualTime> {
        let node = self.node(name)?;
        node.manual_time()
            .ok_or_else(|| MultiNodeError::ManualTimeDisabled(node.name().to_string()))
    }

    pub fn advance_all(&self, duration: Duration) -> MultiNodeResult<()> {
        for node in &self.nodes {
            let manual_time = node
                .manual_time()
                .ok_or_else(|| MultiNodeError::ManualTimeDisabled(node.name().to_string()))?;
            manual_time.advance(duration);
        }
        Ok(())
    }

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
        let entered = barriers.enter(name, node_name, self.expected_barrier_nodes())?;
        if entered.status.passed() {
            self.barrier_changed.notify_all();
        }
        Ok(entered.status)
    }

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
        let entered = barriers.enter(name.clone(), &node_name, self.expected_barrier_nodes())?;
        let barrier_id = entered.id;
        if entered.status.passed() {
            self.barrier_changed.notify_all();
            return Ok(entered.status);
        }

        loop {
            if let Some(status) = barriers.completed_status(barrier_id) {
                return Ok(status);
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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn kit(&self) -> &ActorSystemTestKit {
        &self.kit
    }

    pub fn system(&self) -> &ActorSystem {
        self.kit.system()
    }

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
    Waiting {
        name: String,
        arrived: BTreeSet<String>,
        remaining: BTreeSet<String>,
    },
    Passed {
        name: String,
        participants: BTreeSet<String>,
    },
}

impl MultiNodeBarrierStatus {
    pub fn name(&self) -> &str {
        match self {
            Self::Waiting { name, .. } | Self::Passed { name, .. } => name,
        }
    }

    pub fn passed(&self) -> bool {
        matches!(self, Self::Passed { .. })
    }
}

#[derive(Debug, Default)]
struct BarrierState {
    active: Option<ActiveBarrier>,
    completed: Vec<CompletedBarrier>,
    next_id: u64,
}

impl BarrierState {
    fn enter(
        &mut self,
        name: String,
        node: &str,
        participants: BTreeSet<String>,
    ) -> MultiNodeResult<EnteredBarrier> {
        let active = match &mut self.active {
            Some(active) => {
                if active.name != name {
                    return Err(MultiNodeError::WrongBarrier {
                        expected: active.name.clone(),
                        actual: name,
                        node: node.to_string(),
                    });
                }
                active
            }
            None => {
                let id = self.next_id;
                self.next_id += 1;
                self.active.insert(ActiveBarrier {
                    id,
                    name: name.clone(),
                    participants,
                    arrived: BTreeSet::new(),
                })
            }
        };
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
        if !seen.insert(name) {
            return Err(MultiNodeError::DuplicateNode(name.clone()));
        }
    }

    Ok(())
}
