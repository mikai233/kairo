use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::{ActorPath, ActorRef};

#[derive(Clone, Default)]
pub struct EventStream {
    inner: Arc<EventStreamInner>,
}

#[derive(Default)]
struct EventStreamInner {
    subscriptions: Mutex<HashMap<TypeId, Vec<Subscription>>>,
}

type Deliver = dyn Fn(&dyn Any) -> bool + Send + Sync;

struct Subscription {
    path: ActorPath,
    deliver: Box<Deliver>,
}

impl fmt::Debug for EventStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let channel_count = self
            .inner
            .subscriptions
            .lock()
            .expect("event stream subscriptions poisoned")
            .len();
        f.debug_struct("EventStream")
            .field("channel_count", &channel_count)
            .finish_non_exhaustive()
    }
}

impl EventStream {
    pub fn subscribe<M>(&self, subscriber: ActorRef<M>) -> bool
    where
        M: Clone + Send + 'static,
    {
        let mut subscriptions = self
            .inner
            .subscriptions
            .lock()
            .expect("event stream subscriptions poisoned");
        let channel = subscriptions.entry(TypeId::of::<M>()).or_default();
        if channel
            .iter()
            .any(|subscription| subscription.path == *subscriber.path())
        {
            return false;
        }
        let path = subscriber.path().clone();
        channel.push(Subscription {
            path,
            deliver: Box::new(move |event| {
                let Some(event) = event.downcast_ref::<M>() else {
                    return true;
                };
                subscriber.tell(event.clone()).is_ok()
            }),
        });
        true
    }

    pub fn unsubscribe<M>(&self, subscriber: &ActorRef<M>) -> bool
    where
        M: Send + 'static,
    {
        let mut subscriptions = self
            .inner
            .subscriptions
            .lock()
            .expect("event stream subscriptions poisoned");
        let Some(channel) = subscriptions.get_mut(&TypeId::of::<M>()) else {
            return false;
        };
        let before = channel.len();
        channel.retain(|subscription| subscription.path != *subscriber.path());
        let removed = before != channel.len();
        if channel.is_empty() {
            subscriptions.remove(&TypeId::of::<M>());
        }
        removed
    }

    pub fn unsubscribe_all<M>(&self, subscriber: &ActorRef<M>) -> bool
    where
        M: Send + 'static,
    {
        let mut removed = false;
        let mut subscriptions = self
            .inner
            .subscriptions
            .lock()
            .expect("event stream subscriptions poisoned");
        subscriptions.retain(|_, channel| {
            let before = channel.len();
            channel.retain(|subscription| subscription.path != *subscriber.path());
            removed |= before != channel.len();
            !channel.is_empty()
        });
        removed
    }

    pub fn publish<M>(&self, event: M)
    where
        M: Clone + Send + 'static,
    {
        let mut subscriptions = self
            .inner
            .subscriptions
            .lock()
            .expect("event stream subscriptions poisoned");
        let Some(channel) = subscriptions.get_mut(&TypeId::of::<M>()) else {
            return;
        };

        let mut retained = Vec::with_capacity(channel.len());
        for subscription in channel.drain(..) {
            if (subscription.deliver)(&event) {
                retained.push(subscription);
            }
        }
        *channel = retained;
        if channel.is_empty() {
            subscriptions.remove(&TypeId::of::<M>());
        }
    }
}
