use std::time::Duration;

use kairo_actor::{ActorError, ActorSystem};

use crate::{ManualTime, TestProbe};

#[derive(Debug)]
pub struct ActorSystemTestKit {
    system: ActorSystem,
}

impl ActorSystemTestKit {
    pub fn new(name: impl Into<String>) -> Result<Self, ActorError> {
        Ok(Self {
            system: ActorSystem::builder(name).build()?,
        })
    }

    pub fn with_manual_time(name: impl Into<String>) -> Result<(Self, ManualTime), ActorError> {
        let manual_time = ManualTime::new();
        let system = ActorSystem::builder(name)
            .manual_scheduler(manual_time.scheduler())
            .build()?;
        Ok((Self { system }, manual_time))
    }

    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    pub fn create_probe<M>(&self, name: impl AsRef<str>) -> Result<TestProbe<M>, ActorError>
    where
        M: Send + 'static,
    {
        TestProbe::spawn(&self.system, name)
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), ActorError> {
        self.system.terminate(timeout)
    }
}
