use std::time::Duration;

use crate::error::ActorError;

use super::{
    PHASE_ACTOR_SYSTEM_TERMINATE, PHASE_BEFORE_ACTOR_SYSTEM_TERMINATE,
    PHASE_BEFORE_CLUSTER_SHUTDOWN, PHASE_BEFORE_SERVICE_UNBIND, PHASE_CLUSTER_EXITING,
    PHASE_CLUSTER_EXITING_DONE, PHASE_CLUSTER_LEAVE, PHASE_CLUSTER_SHARDING_SHUTDOWN_REGION,
    PHASE_CLUSTER_SHUTDOWN, PHASE_SERVICE_REQUESTS_DONE, PHASE_SERVICE_STOP, PHASE_SERVICE_UNBIND,
    ShutdownState,
};

const DEFAULT_PHASE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub(super) struct PhaseDefinition {
    pub(super) name: String,
    pub(super) timeout: Duration,
    pub(super) recover: bool,
}

impl PhaseDefinition {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            timeout: DEFAULT_PHASE_TIMEOUT,
            recover: false,
        }
    }
}

pub(super) fn ensure_phase(state: &ShutdownState, phase: &str) -> Result<(), ActorError> {
    if state.phases.iter().any(|known| known.name == phase) {
        Ok(())
    } else {
        Err(ActorError::UnknownShutdownPhase(phase.to_string()))
    }
}

pub(super) fn default_phases() -> Vec<PhaseDefinition> {
    [
        PHASE_BEFORE_SERVICE_UNBIND,
        PHASE_SERVICE_UNBIND,
        PHASE_SERVICE_REQUESTS_DONE,
        PHASE_SERVICE_STOP,
        PHASE_BEFORE_CLUSTER_SHUTDOWN,
        PHASE_CLUSTER_SHARDING_SHUTDOWN_REGION,
        PHASE_CLUSTER_LEAVE,
        PHASE_CLUSTER_EXITING,
        PHASE_CLUSTER_EXITING_DONE,
        PHASE_CLUSTER_SHUTDOWN,
        PHASE_BEFORE_ACTOR_SYSTEM_TERMINATE,
        PHASE_ACTOR_SYSTEM_TERMINATE,
    ]
    .into_iter()
    .map(PhaseDefinition::new)
    .collect()
}
