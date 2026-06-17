use std::collections::HashSet;
use std::time::Duration;

use kairo_cluster::UniqueAddress;

use crate::{SingletonOldestChange, SingletonOldestObservation};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingletonManagerState {
    Start,
    Younger {
        previous_oldest: Vec<UniqueAddress>,
    },
    BecomingOldest {
        previous_oldest: Vec<UniqueAddress>,
        handover_started: bool,
    },
    Oldest {
        singleton_running: bool,
    },
    WasOldest {
        singleton_running: bool,
        new_oldest: Option<UniqueAddress>,
    },
    HandingOver {
        singleton_running: bool,
        handover_to: Option<UniqueAddress>,
    },
    End,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingletonManagerEffect {
    StartSingleton,
    StopSingleton,
    SendHandOverToMe { to: UniqueAddress },
    SendHandOverInProgress { to: UniqueAddress },
    SendHandOverDone { to: UniqueAddress },
    SendTakeOverFromMe { to: UniqueAddress },
    StopManager,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingletonManagerSettingsError {
    ZeroHandOverRetryInterval,
}

impl std::fmt::Display for SingletonManagerSettingsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroHandOverRetryInterval => {
                write!(
                    f,
                    "singleton manager handover retry interval must be non-zero"
                )
            }
        }
    }
}

impl std::error::Error for SingletonManagerSettingsError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonManagerSettings {
    hand_over_retry_interval: Duration,
    automatic_hand_over_retries: bool,
}

impl SingletonManagerSettings {
    pub fn new(hand_over_retry_interval: Duration) -> Result<Self, SingletonManagerSettingsError> {
        if hand_over_retry_interval.is_zero() {
            return Err(SingletonManagerSettingsError::ZeroHandOverRetryInterval);
        }
        Ok(Self {
            hand_over_retry_interval,
            automatic_hand_over_retries: true,
        })
    }

    pub fn with_automatic_hand_over_retries(mut self, automatic: bool) -> Self {
        self.automatic_hand_over_retries = automatic;
        self
    }

    pub fn hand_over_retry_interval(&self) -> Duration {
        self.hand_over_retry_interval
    }

    pub fn automatic_hand_over_retries(&self) -> bool {
        self.automatic_hand_over_retries
    }
}

impl Default for SingletonManagerSettings {
    fn default() -> Self {
        Self {
            hand_over_retry_interval: Duration::from_secs(1),
            automatic_hand_over_retries: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonManagerRuntime {
    self_node: UniqueAddress,
    state: SingletonManagerState,
    removed: HashSet<UniqueAddress>,
}

impl SingletonManagerRuntime {
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            self_node,
            state: SingletonManagerState::Start,
            removed: HashSet::new(),
        }
    }

    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    pub fn state(&self) -> &SingletonManagerState {
        &self.state
    }

    pub fn hand_over_retry_target(&self) -> Option<&UniqueAddress> {
        match &self.state {
            SingletonManagerState::BecomingOldest {
                previous_oldest,
                handover_started: false,
            } => previous_oldest.first(),
            _ => None,
        }
    }

    pub fn take_over_retry_target(&self) -> Option<&UniqueAddress> {
        match &self.state {
            SingletonManagerState::WasOldest {
                new_oldest: Some(new_oldest),
                ..
            } => Some(new_oldest),
            _ => None,
        }
    }

    pub fn removed_members(&self) -> &HashSet<UniqueAddress> {
        &self.removed
    }

    pub fn apply_initial_observation(
        &mut self,
        observation: SingletonOldestObservation,
    ) -> Vec<SingletonManagerEffect> {
        if observation.oldest() == Some(&self.self_node) && observation.safe_to_be_oldest() {
            self.goto_oldest()
        } else if observation.oldest() == Some(&self.self_node) {
            self.state = SingletonManagerState::BecomingOldest {
                previous_oldest: without_self(observation.older_or_self(), &self.self_node),
                handover_started: false,
            };
            Vec::new()
        } else {
            self.state = SingletonManagerState::Younger {
                previous_oldest: without_self(observation.older_or_self(), &self.self_node),
            };
            Vec::new()
        }
    }

    pub fn apply_oldest_change(
        &mut self,
        change: SingletonOldestChange,
    ) -> Vec<SingletonManagerEffect> {
        match change {
            SingletonOldestChange::OldestChanged(oldest) => self.oldest_changed(oldest),
        }
    }

    pub fn mark_removed(&mut self, node: UniqueAddress) -> Vec<SingletonManagerEffect> {
        self.removed.insert(node.clone());
        if node == self.self_node {
            return self.stop_manager();
        }
        match self.state.clone() {
            SingletonManagerState::Younger {
                mut previous_oldest,
            } => {
                previous_oldest.retain(|oldest| oldest != &node);
                self.state = SingletonManagerState::Younger { previous_oldest };
                Vec::new()
            }
            SingletonManagerState::BecomingOldest {
                mut previous_oldest,
                handover_started,
            } => {
                previous_oldest.retain(|oldest| oldest != &node);
                if previous_oldest
                    .iter()
                    .all(|oldest| self.removed.contains(oldest))
                {
                    self.goto_oldest()
                } else {
                    self.state = SingletonManagerState::BecomingOldest {
                        previous_oldest,
                        handover_started,
                    };
                    Vec::new()
                }
            }
            SingletonManagerState::WasOldest {
                singleton_running,
                new_oldest: Some(new_oldest),
            } if new_oldest == node => self.goto_handing_over(singleton_running, None),
            _ => Vec::new(),
        }
    }

    pub fn hand_over_to_me(&mut self, from: UniqueAddress) -> Vec<SingletonManagerEffect> {
        match &self.state {
            SingletonManagerState::Start => Vec::new(),
            SingletonManagerState::Younger { .. } => {
                vec![SingletonManagerEffect::SendHandOverDone { to: from }]
            }
            SingletonManagerState::Oldest { singleton_running }
            | SingletonManagerState::WasOldest {
                singleton_running, ..
            } => self.goto_handing_over(*singleton_running, Some(from)),
            SingletonManagerState::HandingOver {
                handover_to: Some(handover_to),
                ..
            } if handover_to == &from => {
                vec![SingletonManagerEffect::SendHandOverInProgress { to: from }]
            }
            SingletonManagerState::BecomingOldest { .. }
            | SingletonManagerState::HandingOver { .. }
            | SingletonManagerState::End
            | SingletonManagerState::Stopped => Vec::new(),
        }
    }

    pub fn hand_over_in_progress(&mut self, from: &UniqueAddress) -> Vec<SingletonManagerEffect> {
        if let SingletonManagerState::BecomingOldest {
            previous_oldest,
            handover_started,
        } = &mut self.state
            && previous_oldest.first() == Some(from)
        {
            *handover_started = true;
        }
        Vec::new()
    }

    pub fn hand_over_done(&mut self, from: &UniqueAddress) -> Vec<SingletonManagerEffect> {
        match &self.state {
            SingletonManagerState::BecomingOldest {
                previous_oldest, ..
            } if previous_oldest.first() == Some(from) => self.goto_oldest(),
            _ => Vec::new(),
        }
    }

    pub fn hand_over_retry(&mut self) -> Vec<SingletonManagerEffect> {
        match &self.state {
            SingletonManagerState::BecomingOldest {
                previous_oldest,
                handover_started: false,
            } => previous_oldest
                .first()
                .cloned()
                .map(|to| vec![SingletonManagerEffect::SendHandOverToMe { to }])
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    pub fn take_over_retry(&mut self) -> Vec<SingletonManagerEffect> {
        match &self.state {
            SingletonManagerState::WasOldest {
                new_oldest: Some(new_oldest),
                ..
            } => vec![SingletonManagerEffect::SendTakeOverFromMe {
                to: new_oldest.clone(),
            }],
            _ => Vec::new(),
        }
    }

    pub fn take_over_from_me(&mut self, from: UniqueAddress) -> Vec<SingletonManagerEffect> {
        match &mut self.state {
            SingletonManagerState::BecomingOldest {
                previous_oldest, ..
            } => match previous_oldest.first() {
                Some(oldest) if oldest == &from => {
                    vec![SingletonManagerEffect::SendHandOverToMe { to: from }]
                }
                None => {
                    previous_oldest.push(from.clone());
                    vec![SingletonManagerEffect::SendHandOverToMe { to: from }]
                }
                _ => Vec::new(),
            },
            SingletonManagerState::Oldest { .. } => {
                vec![SingletonManagerEffect::SendHandOverToMe { to: from }]
            }
            _ => Vec::new(),
        }
    }

    pub fn singleton_terminated(&mut self) -> Vec<SingletonManagerEffect> {
        match &self.state {
            SingletonManagerState::Oldest { .. } => {
                self.state = SingletonManagerState::Oldest {
                    singleton_running: false,
                };
                Vec::new()
            }
            SingletonManagerState::WasOldest { new_oldest, .. } => {
                self.state = SingletonManagerState::WasOldest {
                    singleton_running: false,
                    new_oldest: new_oldest.clone(),
                };
                Vec::new()
            }
            SingletonManagerState::HandingOver { handover_to, .. } => {
                self.handover_done_to(handover_to.clone())
            }
            SingletonManagerState::Start
            | SingletonManagerState::Younger { .. }
            | SingletonManagerState::BecomingOldest { .. }
            | SingletonManagerState::End
            | SingletonManagerState::Stopped => Vec::new(),
        }
    }

    pub fn stop_manager(&mut self) -> Vec<SingletonManagerEffect> {
        self.state = SingletonManagerState::Stopped;
        vec![SingletonManagerEffect::StopManager]
    }

    fn oldest_changed(&mut self, oldest: Option<UniqueAddress>) -> Vec<SingletonManagerEffect> {
        match self.state.clone() {
            SingletonManagerState::Younger {
                mut previous_oldest,
            } => {
                if oldest.as_ref() == Some(&self.self_node) {
                    if previous_oldest
                        .iter()
                        .all(|oldest| self.removed.contains(oldest))
                    {
                        self.goto_oldest()
                    } else if let Some(previous) = previous_oldest.first().cloned() {
                        self.state = SingletonManagerState::BecomingOldest {
                            previous_oldest: previous_oldest.clone(),
                            handover_started: false,
                        };
                        vec![SingletonManagerEffect::SendHandOverToMe { to: previous }]
                    } else {
                        self.goto_oldest()
                    }
                } else {
                    if let Some(oldest) = oldest
                        && !previous_oldest.contains(&oldest)
                    {
                        previous_oldest.insert(0, oldest);
                    }
                    Vec::new()
                }
            }
            SingletonManagerState::Oldest { singleton_running }
            | SingletonManagerState::WasOldest {
                singleton_running, ..
            } => self.oldest_changed_while_oldest(singleton_running, oldest),
            _ => Vec::new(),
        }
    }

    fn oldest_changed_while_oldest(
        &mut self,
        singleton_running: bool,
        oldest: Option<UniqueAddress>,
    ) -> Vec<SingletonManagerEffect> {
        match oldest {
            Some(oldest) if oldest == self.self_node => Vec::new(),
            Some(oldest) if self.removed.contains(&oldest) => {
                self.goto_handing_over(singleton_running, None)
            }
            Some(oldest) => {
                self.state = SingletonManagerState::WasOldest {
                    singleton_running,
                    new_oldest: Some(oldest.clone()),
                };
                vec![SingletonManagerEffect::SendTakeOverFromMe { to: oldest }]
            }
            None => {
                self.state = SingletonManagerState::WasOldest {
                    singleton_running,
                    new_oldest: None,
                };
                Vec::new()
            }
        }
    }

    fn goto_oldest(&mut self) -> Vec<SingletonManagerEffect> {
        self.state = SingletonManagerState::Oldest {
            singleton_running: true,
        };
        vec![SingletonManagerEffect::StartSingleton]
    }

    fn goto_handing_over(
        &mut self,
        singleton_running: bool,
        handover_to: Option<UniqueAddress>,
    ) -> Vec<SingletonManagerEffect> {
        if singleton_running {
            self.state = SingletonManagerState::HandingOver {
                singleton_running: true,
                handover_to: handover_to.clone(),
            };
            let mut effects = Vec::new();
            if let Some(to) = handover_to {
                effects.push(SingletonManagerEffect::SendHandOverInProgress { to });
            }
            effects.push(SingletonManagerEffect::StopSingleton);
            effects
        } else {
            self.handover_done_to(handover_to)
        }
    }

    fn handover_done_to(
        &mut self,
        handover_to: Option<UniqueAddress>,
    ) -> Vec<SingletonManagerEffect> {
        let effects = handover_to
            .clone()
            .map(|to| vec![SingletonManagerEffect::SendHandOverDone { to }])
            .unwrap_or_default();
        self.state = if handover_to.is_some() {
            SingletonManagerState::End
        } else {
            SingletonManagerState::Younger {
                previous_oldest: Vec::new(),
            }
        };
        effects
    }
}

fn without_self(nodes: &[UniqueAddress], self_node: &UniqueAddress) -> Vec<UniqueAddress> {
    nodes
        .iter()
        .filter(|node| *node != self_node)
        .cloned()
        .collect()
}
