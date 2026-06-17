use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorRef, ActorSystem, AnyActorRef, DeadLetter, Props, SendError,
};

use crate::{ActorSystemTestKit, ManualTime, ProbeError, TestProbe, Within, WithinError, within};

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

    /// Creates an erased-ref probe, watches the subject, and waits for the
    /// subject termination notification.
    ///
    /// This asserts the death-watch observation, not only the subject's local
    /// termination latch. The explicit `watcher_name` keeps repeated assertions
    /// from colliding on probe actor names.
    pub fn expect_subject_terminated(
        &self,
        watcher_name: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<AnyActorRef, ProbeError> {
        let probe = self
            .kit
            .create_probe::<AnyActorRef>(watcher_name)
            .map_err(|error| ProbeError::WatchFailed(error.to_string()))?;
        probe.expect_terminated(&self.actor_ref, timeout)
    }

    /// Runs [`Self::expect_subject_terminated`] under a shared [`Within`]
    /// deadline.
    pub fn expect_subject_terminated_within(
        &self,
        watcher_name: impl AsRef<str>,
        scope: &Within,
    ) -> Result<AnyActorRef, ProbeError> {
        let probe = self
            .kit
            .create_probe::<AnyActorRef>(watcher_name)
            .map_err(|error| ProbeError::WatchFailed(error.to_string()))?;
        probe.expect_terminated_within(&self.actor_ref, scope)
    }

    /// Runs harness-centered assertions under one shared deadline.
    ///
    /// This mirrors [`TestProbe::within`] for tests that need to combine
    /// subject sends, probe assertions, and subject lifecycle checks without
    /// accidentally granting each step an independent timeout.
    pub fn within<T, E, F>(&self, timeout: Duration, assertion: F) -> Result<T, WithinError<E>>
    where
        F: FnOnce(&Self, &Within) -> Result<T, E>,
    {
        within(timeout, |scope| assertion(self, scope))
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

    /// Waits for the subject actor to terminate under a shared [`Within`]
    /// deadline.
    pub fn expect_stopped_within(&self, scope: &Within) -> Result<(), ActorHarnessError> {
        self.expect_stopped(scope.remaining())
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
