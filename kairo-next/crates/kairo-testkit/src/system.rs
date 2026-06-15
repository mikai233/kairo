use std::time::Duration;

use kairo_actor::{ActorError, ActorSystem, DeadLetter};

use crate::{ManualTime, TestProbe};

#[derive(Debug)]
pub struct ActorSystemTestKit {
    system: ActorSystem,
}

impl ActorSystemTestKit {
    /// Creates a test kit with a real local [`ActorSystem`].
    ///
    /// Use this for tests that should exercise normal scheduling, mailbox,
    /// event-stream, receptionist, and lifecycle behavior without manual time.
    pub fn new(name: impl Into<String>) -> Result<Self, ActorError> {
        Ok(Self {
            system: ActorSystem::builder(name).build()?,
        })
    }

    /// Creates a test kit whose actor system uses a manual scheduler backend.
    ///
    /// The returned [`ManualTime`] drives scheduled tasks, timers, and other
    /// manual-scheduler work deterministically. Actor message turns still run
    /// through the real local runtime.
    pub fn with_manual_time(name: impl Into<String>) -> Result<(Self, ManualTime), ActorError> {
        let manual_time = ManualTime::new();
        let system = ActorSystem::builder(name)
            .manual_scheduler(manual_time.scheduler())
            .build()?;
        Ok((Self { system }, manual_time))
    }

    /// Returns the actor system owned by this test kit.
    ///
    /// Tests can use this to spawn actors directly when a specialized helper is
    /// not needed.
    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Creates a typed probe actor under this test kit's actor system.
    ///
    /// The returned [`TestProbe`] exposes an `ActorRef<M>` for the code under
    /// test while collecting received messages on a deterministic test queue.
    pub fn create_probe<M>(&self, name: impl AsRef<str>) -> Result<TestProbe<M>, ActorError>
    where
        M: Send + 'static,
    {
        TestProbe::spawn(&self.system, name)
    }

    /// Creates a probe and subscribes it to the local typed event stream.
    ///
    /// The event type is exact Rust type matching; subscribing to one event
    /// type does not receive other event types.
    pub fn create_event_probe<M>(&self, name: impl AsRef<str>) -> Result<TestProbe<M>, ActorError>
    where
        M: Clone + Send + 'static,
    {
        let probe = self.create_probe(name)?;
        self.system.event_stream().subscribe(probe.actor_ref());
        Ok(probe)
    }

    /// Creates a probe subscribed to local dead-letter events.
    ///
    /// This is a convenience wrapper over [`create_event_probe`] for
    /// [`DeadLetter`] diagnostics.
    ///
    /// [`create_event_probe`]: Self::create_event_probe
    pub fn create_dead_letter_probe(
        &self,
        name: impl AsRef<str>,
    ) -> Result<TestProbe<DeadLetter>, ActorError> {
        self.create_event_probe(name)
    }

    /// Terminates the owned actor system within `timeout`.
    pub fn shutdown(self, timeout: Duration) -> Result<(), ActorError> {
        self.system.terminate(timeout)
    }
}
