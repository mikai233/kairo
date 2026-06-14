use std::fmt::{self, Display, Formatter};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, AnyActorRef, Context, Props,
};

use crate::fishing::{FishingOutcome, remaining_until};

pub struct TestProbe<M> {
    system: ActorSystem,
    actor_ref: ActorRef<M>,
    receiver: Receiver<M>,
}

impl<M: Send + 'static> TestProbe<M> {
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

    pub fn actor_ref(&self) -> ActorRef<M> {
        self.actor_ref.clone()
    }

    pub fn watch_with<N: Send + 'static>(&self, subject: &ActorRef<N>, message: M) -> ActorResult {
        self.system
            .watch_with(self.actor_ref.clone(), subject.clone(), message)
    }

    pub fn unwatch<N: Send + 'static>(&self, subject: &ActorRef<N>) {
        self.system.unwatch(&self.actor_ref, subject);
    }

    pub fn expect_msg(&self, timeout: Duration) -> Result<M, ProbeError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(message) => Ok(message),
            Err(RecvTimeoutError::Timeout) => Err(ProbeError::Timeout(timeout)),
            Err(RecvTimeoutError::Disconnected) => Err(ProbeError::Closed),
        }
    }

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
}

impl TestProbe<AnyActorRef> {
    pub fn watch_terminated<N: Send + 'static>(
        &self,
        subject: &ActorRef<N>,
    ) -> Result<(), ActorError> {
        self.watch_with(subject, subject.as_any())
    }

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeError {
    Timeout(Duration),
    ReceiveMessagesTimeout {
        timeout: Duration,
        expected: usize,
        received: usize,
    },
    FishTimeout {
        timeout: Duration,
        seen: usize,
    },
    Closed,
    WatchFailed(String),
    FishingFailed(String),
    UnexpectedMessage {
        expected: String,
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
