use std::collections::VecDeque;

use crate::error::ActorError;

#[derive(Debug)]
pub(crate) struct StashState<M> {
    capacity: Option<usize>,
    messages: VecDeque<M>,
}

impl<M> StashState<M> {
    pub(crate) fn new(capacity: Option<usize>) -> Self {
        Self {
            capacity,
            messages: VecDeque::new(),
        }
    }

    pub(crate) fn stash(&mut self, message: M) -> Result<(), ActorError> {
        let Some(capacity) = self.capacity else {
            return Err(ActorError::StashDisabled);
        };
        if self.messages.len() >= capacity {
            return Err(ActorError::StashFull { capacity });
        }
        self.messages.push_back(message);
        Ok(())
    }

    pub(crate) fn take(&mut self, limit: usize) -> Vec<M> {
        let count = limit.min(self.messages.len());
        self.messages.drain(..count).collect()
    }

    pub(crate) fn take_all(&mut self) -> Vec<M> {
        self.messages.drain(..).collect()
    }

    pub(crate) fn clear(&mut self) {
        self.messages.clear();
    }

    pub(crate) fn len(&self) -> usize {
        self.messages.len()
    }

    pub(crate) fn capacity(&self) -> Option<usize> {
        self.capacity
    }

    pub(crate) fn is_full(&self) -> bool {
        self.capacity
            .is_some_and(|capacity| self.messages.len() >= capacity)
    }
}
