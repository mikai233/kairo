use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Mutex;

use crate::error::ActorError;
use crate::path::ActorPath;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TerminationCause {
    Stopped,
    Failed(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeathWatchKind {
    Signal,
    Custom,
}

pub(crate) struct DeathWatchRegistration {
    watcher: ActorPath,
    kind: DeathWatchKind,
    notify: Box<dyn FnOnce(TerminationCause) + Send>,
}

impl DeathWatchRegistration {
    pub(crate) fn new(
        watcher: ActorPath,
        kind: DeathWatchKind,
        notify: impl FnOnce(TerminationCause) + Send + 'static,
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

    pub(crate) fn notify(self, cause: TerminationCause) {
        (self.notify)(cause);
    }
}

#[derive(Default)]
pub(crate) struct DeathWatchRegistry {
    watchers: Mutex<HashMap<ActorPath, Vec<DeathWatchRegistration>>>,
    queued_signals: Mutex<HashSet<QueuedSignal>>,
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
        if registration.kind == DeathWatchKind::Signal
            && self.is_signal_queued(&subject, registration.watcher())
        {
            return Ok(());
        }

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
        self.clear_queued_signal(subject, watcher);
    }

    pub(crate) fn remove_watcher(&self, watcher: &ActorPath) {
        let mut watchers = self.watchers.lock().expect("death watch registry poisoned");
        watchers.retain(|_, subject_watchers| {
            subject_watchers.retain(|registration| registration.watcher() != watcher);
            !subject_watchers.is_empty()
        });
        self.queued_signals
            .lock()
            .expect("death watch queued signals poisoned")
            .retain(|queued| &queued.watcher != watcher);
    }

    pub(crate) fn notify(&self, subject: &ActorPath, cause: TerminationCause) {
        let registrations = self
            .watchers
            .lock()
            .expect("death watch registry poisoned")
            .remove(subject)
            .unwrap_or_default();
        for registration in registrations {
            if registration.kind == DeathWatchKind::Signal {
                self.queue_signal(subject.clone(), registration.watcher.clone());
            }
            registration.notify(cause.clone());
        }
    }

    pub(crate) fn notify_matching(
        &self,
        mut predicate: impl FnMut(&ActorPath) -> bool,
        cause: TerminationCause,
    ) {
        let registrations = {
            let mut watchers = self.watchers.lock().expect("death watch registry poisoned");
            let subjects = watchers
                .keys()
                .filter(|subject| predicate(subject))
                .cloned()
                .collect::<Vec<_>>();
            let mut registrations = Vec::new();
            for subject in subjects {
                registrations.extend(
                    watchers
                        .remove(&subject)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|registration| (subject.clone(), registration)),
                );
            }
            registrations
        };
        for (subject, registration) in registrations {
            if registration.kind == DeathWatchKind::Signal {
                self.queue_signal(subject, registration.watcher.clone());
            }
            registration.notify(cause.clone());
        }
    }

    pub(crate) fn take_queued_signal(&self, subject: &ActorPath, watcher: &ActorPath) -> bool {
        self.queued_signals
            .lock()
            .expect("death watch queued signals poisoned")
            .remove(&QueuedSignal {
                subject: subject.clone(),
                watcher: watcher.clone(),
            })
    }

    fn clear_queued_signal(&self, subject: &ActorPath, watcher: &ActorPath) {
        self.take_queued_signal(subject, watcher);
    }

    fn is_signal_queued(&self, subject: &ActorPath, watcher: &ActorPath) -> bool {
        self.queued_signals
            .lock()
            .expect("death watch queued signals poisoned")
            .contains(&QueuedSignal {
                subject: subject.clone(),
                watcher: watcher.clone(),
            })
    }

    fn queue_signal(&self, subject: ActorPath, watcher: ActorPath) {
        self.queued_signals
            .lock()
            .expect("death watch queued signals poisoned")
            .insert(QueuedSignal { subject, watcher });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct QueuedSignal {
    subject: ActorPath,
    watcher: ActorPath,
}
