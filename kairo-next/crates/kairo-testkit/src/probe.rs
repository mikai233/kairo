use std::fmt::{self, Display, Formatter};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, AnyActorRef, Context, Props,
};

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
    Closed,
    WatchFailed(String),
    UnexpectedMessage { expected: String, actual: String },
}

impl Display for ProbeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout(timeout) => write!(f, "timed out after {timeout:?} waiting for message"),
            Self::Closed => f.write_str("test probe channel is closed"),
            Self::WatchFailed(error) => write!(f, "failed to watch actor termination: {error}"),
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
