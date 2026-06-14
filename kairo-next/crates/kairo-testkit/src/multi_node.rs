use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use std::sync::Mutex;
use std::time::Duration;

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
        barriers.enter(name, node_name, self.expected_barrier_nodes())
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
}

impl BarrierState {
    fn enter(
        &mut self,
        name: String,
        node: &str,
        participants: BTreeSet<String>,
    ) -> MultiNodeResult<MultiNodeBarrierStatus> {
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
            None => self.active.insert(ActiveBarrier {
                name: name.clone(),
                participants,
                arrived: BTreeSet::new(),
            }),
        };

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
            Ok(MultiNodeBarrierStatus::Passed {
                name: completed.name,
                participants: completed.participants,
            })
        } else {
            let remaining = active
                .participants
                .difference(&active.arrived)
                .cloned()
                .collect();
            Ok(MultiNodeBarrierStatus::Waiting {
                name: active.name.clone(),
                arrived: active.arrived.clone(),
                remaining,
            })
        }
    }
}

#[derive(Debug)]
struct ActiveBarrier {
    name: String,
    participants: BTreeSet<String>,
    arrived: BTreeSet<String>,
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
