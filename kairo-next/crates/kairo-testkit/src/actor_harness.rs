use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorRef, ActorSystem, AnyActorRef, DeadLetter, Props, SendError,
};

use crate::{ActorSystemTestKit, ManualTime, TestProbe};

/// Spawn-backed harness for tests centered on one actor under a real actor system.
///
/// `ActorHarness` owns an [`ActorSystemTestKit`] plus the subject actor ref. It
/// is useful when a test wants to drive one actor through normal runtime
/// semantics while still creating typed probes, event probes, dead-letter
/// probes, and optional manual time from the same system.
#[derive(Debug)]
pub struct ActorHarness<M> {
    kit: ActorSystemTestKit,
    actor_ref: ActorRef<M>,
}

impl<M: Send + 'static> ActorHarness<M> {
    /// Creates a new actor-system testkit and spawns the subject actor under `/user`.
    ///
    /// The returned harness owns both the actor system and the subject ref. Use
    /// [`Self::shutdown`] at the end of the test to terminate the system
    /// explicitly.
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

    /// Creates a harness whose actor system uses the manual scheduler backend.
    ///
    /// The returned [`ManualTime`] handle drives timers, scheduled self
    /// messages, and other scheduler-backed actor operations for the subject
    /// system.
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

    /// Returns the actor system that owns the subject and any probes.
    pub fn system(&self) -> &ActorSystem {
        self.kit.system()
    }

    /// Returns a clone of the subject actor ref.
    pub fn actor_ref(&self) -> ActorRef<M> {
        self.actor_ref.clone()
    }

    /// Sends one message to the subject actor.
    pub fn tell(&self, message: M) -> Result<(), SendError<M>> {
        self.actor_ref.tell(message)
    }

    /// Creates a typed probe actor in the same actor system as the subject.
    pub fn create_probe<N>(&self, name: impl AsRef<str>) -> Result<TestProbe<N>, ActorError>
    where
        N: Send + 'static,
    {
        self.kit.create_probe(name)
    }

    /// Creates and subscribes a typed event-stream probe in the subject system.
    pub fn create_event_probe<N>(&self, name: impl AsRef<str>) -> Result<TestProbe<N>, ActorError>
    where
        N: Clone + Send + 'static,
    {
        self.kit.create_event_probe(name)
    }

    /// Creates and subscribes a dead-letter probe in the subject system.
    pub fn create_dead_letter_probe(
        &self,
        name: impl AsRef<str>,
    ) -> Result<TestProbe<DeadLetter>, ActorError> {
        self.kit.create_dead_letter_probe(name)
    }

    /// Creates an erased-ref probe already watching the subject actor.
    pub fn watch_subject(
        &self,
        name: impl AsRef<str>,
    ) -> Result<TestProbe<AnyActorRef>, ActorError> {
        let probe = self.kit.create_probe(name)?;
        probe.watch_terminated(&self.actor_ref)?;
        Ok(probe)
    }

    /// Requests the subject actor to stop.
    pub fn stop(&self) {
        self.kit.system().stop(&self.actor_ref);
    }

    /// Waits for the subject actor to terminate.
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

    /// Terminates the harness-owned actor system.
    pub fn shutdown(self, timeout: Duration) -> Result<(), ActorError> {
        self.kit.shutdown(timeout)
    }
}

/// Errors returned by [`ActorHarness`] assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorHarnessError {
    /// The subject actor did not stop before the timeout expired.
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
