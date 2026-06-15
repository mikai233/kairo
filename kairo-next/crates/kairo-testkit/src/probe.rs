use std::fmt::{self, Display, Formatter};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, AnyActorRef, Context, Props,
};

use crate::fishing::{FishingOutcome, remaining_until};
use crate::within::{Within, WithinError, within};

/// Typed test probe backed by a local actor.
///
/// A probe exposes an [`ActorRef<M>`] to code under test and collects delivered
/// messages in a test-side queue. Assertions then stay typed while exercising
/// the same local actor send, death-watch, and stop paths used by production
/// actors.
pub struct TestProbe<M> {
    system: ActorSystem,
    actor_ref: ActorRef<M>,
    receiver: Receiver<M>,
}

impl<M: Send + 'static> TestProbe<M> {
    /// Spawns a probe actor under `system`.
    ///
    /// The probe actor forwards every received `M` into a test-side queue so
    /// assertions can remain typed while code under test only sees an
    /// [`ActorRef<M>`].
    pub fn spawn(system: &ActorSystem, name: impl AsRef<str>) -> Result<Self, ActorError> {
        let (sender, receiver) = mpsc::channel();
        let actor_ref = system.spawn(
            name,
            Props::new(move || ProbeActor {
                sender: sender.clone(),
            }),
        )?;
        Ok(Self {
            system: system.clone(),
            actor_ref,
            receiver,
        })
    }

    /// Returns the typed actor ref for code under test.
    pub fn actor_ref(&self) -> ActorRef<M> {
        self.actor_ref.clone()
    }

    /// Requests probe actor stop through the owning actor system.
    pub fn stop(&self) {
        self.system.stop(&self.actor_ref);
    }

    /// Waits until the backing probe actor stops.
    pub fn expect_stopped(&self, timeout: Duration) -> Result<(), ProbeError> {
        if self.actor_ref.wait_for_stop(timeout) {
            Ok(())
        } else {
            Err(ProbeError::StopTimeout {
                actor: self.actor_ref.path().to_string(),
                timeout,
            })
        }
    }

    /// Watches `subject` and delivers `message` to this probe on termination.
    ///
    /// This uses the same local death-watch path as actor `Context::watch_with`.
    pub fn watch_with<N: Send + 'static>(&self, subject: &ActorRef<N>, message: M) -> ActorResult {
        self.system
            .watch_with(self.actor_ref.clone(), subject.clone(), message)
    }

    /// Removes a previous watch registration for `subject`.
    pub fn unwatch<N: Send + 'static>(&self, subject: &ActorRef<N>) {
        self.system.unwatch(&self.actor_ref, subject);
    }

    /// Receives one message within `timeout`.
    pub fn expect_msg(&self, timeout: Duration) -> Result<M, ProbeError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(message) => Ok(message),
            Err(RecvTimeoutError::Timeout) => Err(ProbeError::Timeout(timeout)),
            Err(RecvTimeoutError::Disconnected) => Err(ProbeError::Closed),
        }
    }

    /// Receives one message using the remaining time from a shared
    /// [`Within`] deadline.
    pub fn expect_msg_within(&self, scope: &Within) -> Result<M, ProbeError> {
        self.expect_msg(scope.remaining())
    }

    /// Receives one message and checks it for exact equality.
    pub fn expect_msg_eq(&self, expected: M, timeout: Duration) -> Result<M, ProbeError>
    where
        M: fmt::Debug + PartialEq,
    {
        let actual = self.expect_msg(timeout)?;
        if actual == expected {
            Ok(actual)
        } else {
            Err(ProbeError::UnexpectedMessage {
                expected: format!("{expected:?}"),
                actual: format!("{actual:?}"),
            })
        }
    }

    /// Receives one message under a shared [`Within`] deadline and checks it
    /// for exact equality.
    pub fn expect_msg_eq_within(&self, expected: M, scope: &Within) -> Result<M, ProbeError>
    where
        M: fmt::Debug + PartialEq,
    {
        self.expect_msg_eq(expected, scope.remaining())
    }

    /// Receives one message and checks it with `predicate`.
    pub fn expect_msg_matching<F>(&self, timeout: Duration, predicate: F) -> Result<M, ProbeError>
    where
        M: fmt::Debug,
        F: FnOnce(&M) -> bool,
    {
        let actual = self.expect_msg(timeout)?;
        if predicate(&actual) {
            Ok(actual)
        } else {
            Err(ProbeError::UnexpectedMessage {
                expected: "message matching predicate".to_string(),
                actual: format!("{actual:?}"),
            })
        }
    }

    /// Receives one message under a shared [`Within`] deadline and checks it
    /// with `predicate`.
    pub fn expect_msg_matching_within<F>(
        &self,
        scope: &Within,
        predicate: F,
    ) -> Result<M, ProbeError>
    where
        M: fmt::Debug,
        F: FnOnce(&M) -> bool,
    {
        self.expect_msg_matching(scope.remaining(), predicate)
    }

    /// Asserts that no message arrives during `duration`.
    pub fn expect_no_msg(&self, duration: Duration) -> Result<(), ProbeError> {
        match self.receiver.recv_timeout(duration) {
            Ok(_message) => Err(ProbeError::UnexpectedMessage {
                expected: "no message".to_string(),
                actual: message_type_name::<M>().to_string(),
            }),
            Err(RecvTimeoutError::Timeout) => Ok(()),
            Err(RecvTimeoutError::Disconnected) => Err(ProbeError::Closed),
        }
    }

    /// Receives exactly `count` messages under one timeout budget.
    ///
    /// The timeout is shared across the whole batch instead of being restarted
    /// for each message.
    pub fn receive_messages(&self, count: usize, timeout: Duration) -> Result<Vec<M>, ProbeError> {
        let deadline = std::time::Instant::now() + timeout;
        let mut messages = Vec::with_capacity(count);

        while messages.len() < count {
            let remaining = remaining_until(deadline).unwrap_or(Duration::ZERO);
            match self.receiver.recv_timeout(remaining) {
                Ok(message) => messages.push(message),
                Err(RecvTimeoutError::Timeout) => {
                    return Err(ProbeError::ReceiveMessagesTimeout {
                        timeout,
                        expected: count,
                        received: messages.len(),
                    });
                }
                Err(RecvTimeoutError::Disconnected) => return Err(ProbeError::Closed),
            }
        }

        Ok(messages)
    }

    /// Receives exactly `count` messages under a shared [`Within`] deadline.
    pub fn receive_messages_within(
        &self,
        count: usize,
        scope: &Within,
    ) -> Result<Vec<M>, ProbeError> {
        self.receive_messages(count, scope.remaining())
    }

    /// Receives messages until `fisher` completes or fails.
    ///
    /// The returned vector contains messages that were not ignored by the
    /// [`FishingOutcome`] classifier.
    pub fn fish_for_message<F>(
        &self,
        timeout: Duration,
        mut fisher: F,
    ) -> Result<Vec<M>, ProbeError>
    where
        F: FnMut(&M) -> FishingOutcome,
    {
        let deadline = std::time::Instant::now() + timeout;
        let mut seen = Vec::new();

        loop {
            let remaining = remaining_until(deadline).unwrap_or(Duration::ZERO);
            let message = match self.receiver.recv_timeout(remaining) {
                Ok(message) => message,
                Err(RecvTimeoutError::Timeout) => {
                    return Err(ProbeError::FishTimeout {
                        timeout,
                        seen: seen.len(),
                    });
                }
                Err(RecvTimeoutError::Disconnected) => return Err(ProbeError::Closed),
            };

            match fisher(&message) {
                FishingOutcome::Complete => {
                    seen.push(message);
                    return Ok(seen);
                }
                FishingOutcome::Fail(reason) => return Err(ProbeError::FishingFailed(reason)),
                FishingOutcome::Continue => seen.push(message),
                FishingOutcome::ContinueAndIgnore => {}
            }
        }
    }

    /// Runs [`fish_for_message`] under a shared [`Within`] deadline.
    ///
    /// [`fish_for_message`]: Self::fish_for_message
    pub fn fish_for_message_within<F>(
        &self,
        scope: &Within,
        fisher: F,
    ) -> Result<Vec<M>, ProbeError>
    where
        F: FnMut(&M) -> FishingOutcome,
    {
        self.fish_for_message(scope.remaining(), fisher)
    }

    /// Runs multiple probe assertions under one shared deadline.
    ///
    /// Use this when a test needs several receives to share a single timeout
    /// budget instead of giving each assertion a fresh full timeout.
    pub fn within<T, E, F>(&self, timeout: Duration, assertion: F) -> Result<T, WithinError<E>>
    where
        F: FnOnce(&Self, &Within) -> Result<T, E>,
    {
        within(timeout, |scope| assertion(self, scope))
    }
}

impl TestProbe<AnyActorRef> {
    /// Watches `subject` and expects its erased ref as the termination message.
    pub fn watch_terminated<N: Send + 'static>(
        &self,
        subject: &ActorRef<N>,
    ) -> Result<(), ActorError> {
        self.watch_with(subject, subject.as_any())
    }

    /// Watches `subject` and waits for its termination notification.
    pub fn expect_terminated<N: Send + 'static>(
        &self,
        subject: &ActorRef<N>,
        timeout: Duration,
    ) -> Result<AnyActorRef, ProbeError> {
        self.watch_terminated(subject)
            .map_err(|error| ProbeError::WatchFailed(error.to_string()))?;
        let expected = subject.as_any();
        let actual = self.expect_msg(timeout)?;
        if actual == expected {
            Ok(actual)
        } else {
            Err(ProbeError::UnexpectedMessage {
                expected: expected.path().to_string(),
                actual: actual.path().to_string(),
            })
        }
    }
}

struct ProbeActor<M> {
    sender: mpsc::Sender<M>,
}

impl<M: Send + 'static> Actor for ProbeActor<M> {
    type Msg = M;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.sender.send(msg).map_err(|_| {
            ActorError::Message("test probe receiver was dropped before delivery".to_string())
        })
    }
}

/// Error returned by probe assertions.
///
/// Probe errors keep timeout budgets, expected counts, actor paths, and
/// mismatch details available so tests can assert on failure causes without
/// parsing formatted messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeError {
    /// No single message arrived before the timeout elapsed.
    Timeout(Duration),
    /// A fixed-size receive batch timed out before all expected messages
    /// arrived.
    ReceiveMessagesTimeout {
        /// Timeout budget shared by the whole batch receive.
        timeout: Duration,
        /// Number of messages the assertion expected.
        expected: usize,
        /// Number of messages collected before the timeout.
        received: usize,
    },
    /// A fishing assertion timed out before the classifier returned
    /// [`FishingOutcome::Complete`].
    FishTimeout {
        /// Timeout budget shared by the whole fishing operation.
        timeout: Duration,
        /// Number of non-ignored messages seen before the timeout.
        seen: usize,
    },
    /// The probe actor did not stop before the timeout elapsed.
    StopTimeout {
        /// Actor path for the backing probe actor.
        actor: String,
        /// Timeout budget used while waiting for termination.
        timeout: Duration,
    },
    /// The test-side receive channel closed before the expected observation.
    Closed,
    /// Registering a death-watch expectation failed.
    WatchFailed(String),
    /// The fishing classifier rejected the observed stream with a reason.
    FishingFailed(String),
    /// A probe assertion received a message different from the expected value.
    UnexpectedMessage {
        /// Human-readable expected value, predicate, or actor path.
        expected: String,
        /// Human-readable actual value or actor path.
        actual: String,
    },
}

impl Display for ProbeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout(timeout) => write!(f, "timed out after {timeout:?} waiting for message"),
            Self::ReceiveMessagesTimeout {
                timeout,
                expected,
                received,
            } => {
                write!(
                    f,
                    "timed out after {timeout:?} while expecting {expected} messages and receiving {received}"
                )
            }
            Self::FishTimeout { timeout, seen } => {
                write!(
                    f,
                    "timed out after {timeout:?} while fishing for message after seeing {seen} collected messages"
                )
            }
            Self::StopTimeout { actor, timeout } => {
                write!(f, "actor {actor} did not stop within {timeout:?}")
            }
            Self::Closed => f.write_str("test probe channel is closed"),
            Self::WatchFailed(error) => write!(f, "failed to watch actor termination: {error}"),
            Self::FishingFailed(reason) => write!(f, "message fishing failed: {reason}"),
            Self::UnexpectedMessage { expected, actual } => {
                write!(f, "expected {expected}, received {actual}")
            }
        }
    }
}

impl std::error::Error for ProbeError {}

fn message_type_name<M>() -> &'static str {
    std::any::type_name::<M>()
}
