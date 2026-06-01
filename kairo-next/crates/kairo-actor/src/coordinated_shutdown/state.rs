use std::collections::HashMap;
use std::sync::{Condvar, Mutex};

use crate::error::ActorError;

use super::{PhaseDefinition, TaskEntry};

#[derive(Debug)]
pub(super) struct CoordinatedShutdownInner {
    pub(super) state: Mutex<ShutdownState>,
    pub(super) completed: Condvar,
}

impl CoordinatedShutdownInner {
    pub(super) fn new(phases: Vec<PhaseDefinition>) -> Self {
        Self {
            state: Mutex::new(ShutdownState::new(phases)),
            completed: Condvar::new(),
        }
    }
}

#[derive(Debug)]
pub(super) struct ShutdownState {
    pub(super) phases: Vec<PhaseDefinition>,
    pub(super) tasks: HashMap<String, Vec<TaskEntry>>,
    pub(super) started: bool,
    pub(super) completed: bool,
    pub(super) reason: Option<String>,
    pub(super) result: Option<ActorError>,
}

impl ShutdownState {
    fn new(phases: Vec<PhaseDefinition>) -> Self {
        Self {
            phases,
            tasks: HashMap::new(),
            started: false,
            completed: false,
            reason: None,
            result: None,
        }
    }
}
