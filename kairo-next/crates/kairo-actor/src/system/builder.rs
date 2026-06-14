use std::sync::Arc;

use crate::dispatcher::DispatcherSettings;
use crate::error::ActorError;
use crate::mailbox::MailboxSettings;
use crate::path::Address;
use crate::scheduler::{ManualScheduler, Scheduler};

use super::{ActorSystem, ActorSystemInner};

#[derive(Debug)]
pub struct ActorSystemBuilder {
    name: String,
    dispatcher: DispatcherSettings,
    mailbox: MailboxSettings,
    scheduler: Scheduler,
}

impl ActorSystemBuilder {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            dispatcher: DispatcherSettings::default(),
            mailbox: MailboxSettings::default(),
            scheduler: Scheduler::default(),
        }
    }

    pub fn dispatcher_throughput(mut self, throughput: usize) -> Self {
        self.dispatcher = DispatcherSettings::new(throughput);
        self
    }

    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox = MailboxSettings::bounded(capacity);
        self
    }

    pub fn manual_scheduler(mut self, scheduler: ManualScheduler) -> Self {
        self.scheduler = scheduler.into_scheduler();
        self
    }

    pub fn build(self) -> Result<ActorSystem, ActorError> {
        if self.dispatcher.throughput() == 0 {
            return Err(ActorError::InvalidThroughput);
        }
        if self.mailbox.user_capacity() == Some(0) {
            return Err(ActorError::InvalidMailboxCapacity);
        }
        Ok(ActorSystem {
            address: Address::local(self.name.clone()),
            name: self.name,
            inner: Arc::new(ActorSystemInner {
                dispatcher: self.dispatcher,
                mailbox: self.mailbox,
                scheduler: self.scheduler,
                ..ActorSystemInner::default()
            }),
        })
    }
}
