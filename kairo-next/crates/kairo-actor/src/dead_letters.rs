use std::any::type_name;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::ActorPath;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetter {
    recipient: ActorPath,
    message_type: &'static str,
    reason: String,
}

impl DeadLetter {
    pub fn recipient(&self) -> &ActorPath {
        &self.recipient
    }

    pub fn message_type(&self) -> &'static str {
        self.message_type
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

#[derive(Debug, Default)]
struct DeadLettersInner {
    records: Mutex<Vec<DeadLetter>>,
    changed: Condvar,
}

#[derive(Debug, Clone, Default)]
pub struct DeadLetters {
    inner: Arc<DeadLettersInner>,
}

impl DeadLetters {
    pub fn len(&self) -> usize {
        self.inner
            .records
            .lock()
            .expect("dead letters poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn records(&self) -> Vec<DeadLetter> {
        self.inner
            .records
            .lock()
            .expect("dead letters poisoned")
            .clone()
    }

    pub fn wait_for_len(&self, expected: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut records = self.inner.records.lock().expect("dead letters poisoned");
        while records.len() < expected {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            let (next_records, wait) = self
                .inner
                .changed
                .wait_timeout(records, remaining)
                .expect("dead letters poisoned");
            records = next_records;
            if wait.timed_out() && records.len() < expected {
                return false;
            }
        }
        true
    }

    pub(crate) fn publish<M: Send + 'static>(
        &self,
        recipient: ActorPath,
        reason: impl Into<String>,
    ) {
        let mut records = self.inner.records.lock().expect("dead letters poisoned");
        records.push(DeadLetter {
            recipient,
            message_type: type_name::<M>(),
            reason: reason.into(),
        });
        self.inner.changed.notify_all();
    }
}
