use std::sync::Arc;

use crate::dead_letters::DeadLetters;
use crate::dispatcher::DispatcherSettings;
use crate::error::ActorError;
use crate::event_stream::EventStream;
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
    publish_dead_letters_to_event_stream: bool,
}

impl ActorSystemBuilder {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            dispatcher: DispatcherSettings::default(),
            mailbox: MailboxSettings::default(),
            scheduler: Scheduler::default(),
            publish_dead_letters_to_event_stream: true,
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

    pub fn publish_dead_letters_to_event_stream(mut self, enabled: bool) -> Self {
        self.publish_dead_letters_to_event_stream = enabled;
        self
    }

    pub fn build(self) -> Result<ActorSystem, ActorError> {
        if self.dispatcher.throughput() == 0 {
            return Err(ActorError::InvalidThroughput);
        }
        if self.mailbox.user_capacity() == Some(0) {
            return Err(ActorError::InvalidMailboxCapacity);
        }
        let event_stream = EventStream::default();
        let dead_letters = if self.publish_dead_letters_to_event_stream {
            DeadLetters::new(event_stream.clone())
        } else {
            DeadLetters::without_event_stream()
        };
        Ok(ActorSystem {
            address: Address::local(self.name.clone()),
            name: self.name,
            inner: Arc::new(ActorSystemInner {
                dispatcher: self.dispatcher,
                mailbox: self.mailbox,
                scheduler: self.scheduler,
                event_stream,
                dead_letters,
                ..ActorSystemInner::default()
            }),
        })
    }
}
