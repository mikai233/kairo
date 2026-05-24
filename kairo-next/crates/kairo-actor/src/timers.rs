use std::collections::HashMap;

use crate::scheduler::Cancellable;

#[derive(Debug)]
pub(crate) struct TimerEnvelope<M> {
    key: String,
    generation: u64,
    message: M,
}

impl<M> TimerEnvelope<M> {
    pub(crate) fn new(key: String, generation: u64, message: M) -> Self {
        Self {
            key,
            generation,
            message,
        }
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn into_message(self) -> M {
        self.message
    }
}

#[derive(Debug, Default)]
pub(crate) struct TimerState {
    next_generation: u64,
    active: HashMap<String, TimerEntry>,
}

#[derive(Debug)]
struct TimerEntry {
    generation: u64,
    cancellable: Cancellable,
}

impl TimerState {
    pub(crate) fn next_generation(&mut self) -> u64 {
        self.next_generation = self.next_generation.saturating_add(1);
        self.next_generation
    }

    pub(crate) fn start(&mut self, key: String, generation: u64, cancellable: Cancellable) {
        if let Some(existing) = self.active.remove(&key) {
            existing.cancellable.cancel();
        }
        self.active.insert(
            key,
            TimerEntry {
                generation,
                cancellable,
            },
        );
    }

    pub(crate) fn cancel(&mut self, key: &str) {
        if let Some(existing) = self.active.remove(key) {
            existing.cancellable.cancel();
        }
    }

    pub(crate) fn cancel_all(&mut self) {
        for (_, entry) in self.active.drain() {
            entry.cancellable.cancel();
        }
    }

    pub(crate) fn is_active(&self, key: &str) -> bool {
        self.active.contains_key(key)
    }

    pub(crate) fn accept(&mut self, key: &str, generation: u64) -> bool {
        let Some(existing) = self.active.get(key) else {
            return false;
        };
        if existing.generation != generation {
            return false;
        }
        self.active.remove(key);
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerKey(String);

impl TimerKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for TimerKey {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for TimerKey {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}
