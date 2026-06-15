use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorSystem, DeadLetter, Props, SendError};

use crate::{ActorSystemTestKit, ManualTime, TestProbe};

#[derive(Debug)]
pub struct ActorHarness<M> {
    kit: ActorSystemTestKit,
    actor_ref: ActorRef<M>,
}

impl<M: Send + 'static> ActorHarness<M> {
    pub fn spawn<A>(
        system_name: impl Into<String>,
        actor_name: impl AsRef<str>,
        props: Props<A>,
    ) -> Result<Self, ActorError>
    where
        A: Actor<Msg = M>,
    {
        let kit = ActorSystemTestKit::new(system_name)?;
        let actor_ref = kit.system().spawn(actor_name, props)?;
        Ok(Self { kit, actor_ref })
    }

    pub fn with_manual_time<A>(
        system_name: impl Into<String>,
        actor_name: impl AsRef<str>,
        props: Props<A>,
    ) -> Result<(Self, ManualTime), ActorError>
    where
        A: Actor<Msg = M>,
    {
        let (kit, manual_time) = ActorSystemTestKit::with_manual_time(system_name)?;
        let actor_ref = kit.system().spawn(actor_name, props)?;
        Ok((Self { kit, actor_ref }, manual_time))
    }

    pub fn system(&self) -> &ActorSystem {
        self.kit.system()
    }

    pub fn actor_ref(&self) -> ActorRef<M> {
        self.actor_ref.clone()
    }

    pub fn tell(&self, message: M) -> Result<(), SendError<M>> {
        self.actor_ref.tell(message)
    }

    pub fn create_probe<N>(&self, name: impl AsRef<str>) -> Result<TestProbe<N>, ActorError>
    where
        N: Send + 'static,
    {
        self.kit.create_probe(name)
    }

    pub fn create_event_probe<N>(&self, name: impl AsRef<str>) -> Result<TestProbe<N>, ActorError>
    where
        N: Clone + Send + 'static,
    {
        self.kit.create_event_probe(name)
    }

    pub fn create_dead_letter_probe(
        &self,
        name: impl AsRef<str>,
    ) -> Result<TestProbe<DeadLetter>, ActorError> {
        self.kit.create_dead_letter_probe(name)
    }

    pub fn stop(&self) {
        self.kit.system().stop(&self.actor_ref);
    }

    pub fn expect_stopped(&self, timeout: Duration) -> Result<(), ActorHarnessError> {
        if self.actor_ref.wait_for_stop(timeout) {
            Ok(())
        } else {
            Err(ActorHarnessError::StopTimeout {
                actor: self.actor_ref.path().to_string(),
                timeout,
            })
        }
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), ActorError> {
        self.kit.shutdown(timeout)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorHarnessError {
    StopTimeout { actor: String, timeout: Duration },
}

impl Display for ActorHarnessError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::StopTimeout { actor, timeout } => {
                write!(f, "actor {actor} did not stop within {timeout:?}")
            }
        }
    }
}

impl std::error::Error for ActorHarnessError {}
