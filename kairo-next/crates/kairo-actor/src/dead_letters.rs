use std::any::type_name;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::ActorPath;
use crate::EventStream;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Diagnostic record for a message that could not reach its recipient.
pub struct DeadLetter {
    recipient: ActorPath,
    message_type: &'static str,
    reason: String,
}

impl DeadLetter {
    /// Returns the intended recipient path.
    pub fn recipient(&self) -> &ActorPath {
        &self.recipient
    }

    /// Returns the Rust type name used only for local diagnostics.
    pub fn message_type(&self) -> &'static str {
        self.message_type
    }

    /// Returns the delivery failure reason.
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
/// Actor-system dead-letter journal and optional event-stream publisher.
pub struct DeadLetters {
    inner: Arc<DeadLettersInner>,
    event_stream: Option<EventStream>,
}

impl DeadLetters {
    pub(crate) fn new(event_stream: EventStream) -> Self {
        Self::with_event_stream(Some(event_stream))
    }

    pub(crate) fn without_event_stream() -> Self {
        Self::with_event_stream(None)
    }

    fn with_event_stream(event_stream: Option<EventStream>) -> Self {
        Self {
            inner: Arc::default(),
            event_stream,
        }
    }

    /// Returns the number of recorded dead letters.
    pub fn len(&self) -> usize {
        self.inner
            .records
            .lock()
            .expect("dead letters poisoned")
            .len()
    }

    /// Returns whether no dead letters have been recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a snapshot of all recorded dead letters.
    pub fn records(&self) -> Vec<DeadLetter> {
        self.inner
            .records
            .lock()
            .expect("dead letters poisoned")
            .clone()
    }

    /// Waits until at least `expected` records exist or `timeout` expires.
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
        let record = DeadLetter {
            recipient,
            message_type: type_name::<M>(),
            reason: reason.into(),
        };
        let mut records = self.inner.records.lock().expect("dead letters poisoned");
        records.push(record.clone());
        self.inner.changed.notify_all();
        drop(records);
        if let Some(event_stream) = &self.event_stream {
            event_stream.publish(record);
        }
    }
}
