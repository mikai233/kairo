use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;

use crate::error::ActorError;
use crate::path::ActorPath;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeathWatchKind {
    Signal,
    Custom,
}

pub(crate) struct DeathWatchRegistration {
    watcher: ActorPath,
    kind: DeathWatchKind,
    notify: Box<dyn FnOnce() + Send>,
}

impl DeathWatchRegistration {
    pub(crate) fn new(
        watcher: ActorPath,
        kind: DeathWatchKind,
        notify: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            watcher,
            kind,
            notify: Box::new(notify),
        }
    }

    fn watcher(&self) -> &ActorPath {
        &self.watcher
    }

    pub(crate) fn notify(self) {
        (self.notify)();
    }
}

#[derive(Default)]
pub(crate) struct DeathWatchRegistry {
    watchers: Mutex<HashMap<ActorPath, Vec<DeathWatchRegistration>>>,
}

impl fmt::Debug for DeathWatchRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let watched_count = self
            .watchers
            .lock()
            .expect("death watch registry poisoned")
            .len();
        f.debug_struct("DeathWatchRegistry")
            .field("watched_count", &watched_count)
            .finish_non_exhaustive()
    }
}

impl DeathWatchRegistry {
    pub(crate) fn watch(
        &self,
        subject: ActorPath,
        registration: DeathWatchRegistration,
    ) -> Result<(), ActorError> {
        let mut watchers = self.watchers.lock().expect("death watch registry poisoned");
        let subject_watchers = watchers.entry(subject.clone()).or_default();
        if let Some(existing) = subject_watchers
            .iter()
            .find(|existing| existing.watcher == registration.watcher)
        {
            if existing.kind == DeathWatchKind::Signal
                && registration.kind == DeathWatchKind::Signal
            {
                return Ok(());
            }
            return Err(ActorError::AlreadyWatching {
                actor: subject.to_string(),
                watcher: registration.watcher.to_string(),
            });
        }
        subject_watchers.push(registration);
        Ok(())
    }

    pub(crate) fn unwatch(&self, subject: &ActorPath, watcher: &ActorPath) {
        let mut watchers = self.watchers.lock().expect("death watch registry poisoned");
        if let Some(subject_watchers) = watchers.get_mut(subject) {
            subject_watchers.retain(|registration| registration.watcher() != watcher);
            if subject_watchers.is_empty() {
                watchers.remove(subject);
            }
        }
    }

    pub(crate) fn remove_watcher(&self, watcher: &ActorPath) {
        let mut watchers = self.watchers.lock().expect("death watch registry poisoned");
        watchers.retain(|_, subject_watchers| {
            subject_watchers.retain(|registration| registration.watcher() != watcher);
            !subject_watchers.is_empty()
        });
    }

    pub(crate) fn notify(&self, subject: &ActorPath) {
        let registrations = self
            .watchers
            .lock()
            .expect("death watch registry poisoned")
            .remove(subject)
            .unwrap_or_default();
        for registration in registrations {
            registration.notify();
        }
    }
}
