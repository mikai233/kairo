use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::error::ActorError;
use crate::path::ActorPath;
use crate::refs::ActorRef;

pub struct ServiceKey<M> {
    id: String,
    type_id: TypeId,
    _message: PhantomData<fn(M)>,
}

impl<M: 'static> ServiceKey<M> {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            type_id: TypeId::of::<M>(),
            _message: PhantomData,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

impl<M> Clone for ServiceKey<M> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            type_id: self.type_id,
            _message: PhantomData,
        }
    }
}

impl<M> fmt::Debug for ServiceKey<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceKey")
            .field("id", &self.id)
            .field("type_id", &self.type_id)
            .finish()
    }
}

impl<M> PartialEq for ServiceKey<M> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.type_id == other.type_id
    }
}

impl<M> Eq for ServiceKey<M> {}

impl<M> Hash for ServiceKey<M> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.type_id.hash(state);
    }
}

pub struct Listing<M> {
    key: ServiceKey<M>,
    service_instances: Vec<ActorRef<M>>,
}

impl<M> Listing<M> {
    pub fn key(&self) -> &ServiceKey<M> {
        &self.key
    }

    pub fn service_instances(&self) -> &[ActorRef<M>] {
        &self.service_instances
    }

    pub fn all_service_instances(&self) -> &[ActorRef<M>] {
        &self.service_instances
    }

    pub fn services_were_added_or_removed(&self) -> bool {
        true
    }
}

impl<M> Clone for Listing<M> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            service_instances: self.service_instances.clone(),
        }
    }
}

impl<M> fmt::Debug for Listing<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Listing")
            .field("key", &self.key)
            .field("service_instances", &self.service_instances)
            .finish()
    }
}

pub struct Registered<M> {
    key: ServiceKey<M>,
    service_instance: ActorRef<M>,
}

impl<M> Registered<M> {
    pub fn key(&self) -> &ServiceKey<M> {
        &self.key
    }

    pub fn service_instance(&self) -> &ActorRef<M> {
        &self.service_instance
    }
}

impl<M> Clone for Registered<M> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            service_instance: self.service_instance.clone(),
        }
    }
}

impl<M> fmt::Debug for Registered<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registered")
            .field("key", &self.key)
            .field("service_instance", &self.service_instance)
            .finish()
    }
}

pub struct Deregistered<M> {
    key: ServiceKey<M>,
    service_instance: ActorRef<M>,
}

impl<M> Deregistered<M> {
    pub fn key(&self) -> &ServiceKey<M> {
        &self.key
    }

    pub fn service_instance(&self) -> &ActorRef<M> {
        &self.service_instance
    }
}

impl<M> Clone for Deregistered<M> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            service_instance: self.service_instance.clone(),
        }
    }
}

impl<M> fmt::Debug for Deregistered<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Deregistered")
            .field("key", &self.key)
            .field("service_instance", &self.service_instance)
            .finish()
    }
}

#[derive(Clone, Default)]
pub struct Receptionist {
    inner: Arc<ReceptionistInner>,
}

impl fmt::Debug for Receptionist {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receptionist").finish_non_exhaustive()
    }
}

#[derive(Default)]
struct ReceptionistInner {
    buckets: Mutex<HashMap<BucketKey, Box<dyn BucketOps>>>,
}

impl Receptionist {
    pub fn register<M>(&self, key: ServiceKey<M>, service: ActorRef<M>) -> bool
    where
        M: Send + 'static,
    {
        let mut buckets = self.inner.buckets.lock().expect("receptionist poisoned");
        let bucket = bucket_mut(&mut buckets, &key);
        let registered = bucket.register(service);
        if registered {
            bucket.publish_listing();
        }
        registered
    }

    pub fn register_with_ack<M>(
        &self,
        key: ServiceKey<M>,
        service: ActorRef<M>,
        reply_to: ActorRef<Registered<M>>,
    ) -> Result<bool, ActorError>
    where
        M: Send + 'static,
    {
        let mut buckets = self.inner.buckets.lock().expect("receptionist poisoned");
        let bucket = bucket_mut(&mut buckets, &key);
        let registered = bucket.register(service.clone());
        let ack = reply_to.tell(Registered {
            key,
            service_instance: service,
        });
        if registered {
            bucket.publish_listing();
        }
        ack.map(|()| registered)
            .map_err(|error| ActorError::Message(error.to_string()))
    }

    pub fn deregister<M>(&self, key: &ServiceKey<M>, service: &ActorRef<M>) -> bool
    where
        M: Send + 'static,
    {
        let mut buckets = self.inner.buckets.lock().expect("receptionist poisoned");
        let Some(bucket) = existing_bucket_mut(&mut buckets, key) else {
            return false;
        };
        let deregistered = bucket.deregister(service.path());
        if deregistered {
            bucket.publish_listing();
        }
        deregistered
    }

    pub fn deregister_with_ack<M>(
        &self,
        key: &ServiceKey<M>,
        service: &ActorRef<M>,
        reply_to: ActorRef<Deregistered<M>>,
    ) -> Result<bool, ActorError>
    where
        M: Send + 'static,
    {
        let mut buckets = self.inner.buckets.lock().expect("receptionist poisoned");
        let Some(bucket) = existing_bucket_mut(&mut buckets, key) else {
            return Ok(false);
        };
        let deregistered = bucket.deregister(service.path());
        if !deregistered {
            return Ok(false);
        }
        let ack = reply_to.tell(Deregistered {
            key: key.clone(),
            service_instance: service.clone(),
        });
        bucket.publish_listing();
        ack.map(|()| true)
            .map_err(|error| ActorError::Message(error.to_string()))
    }

    pub fn find<M>(&self, key: &ServiceKey<M>) -> Listing<M>
    where
        M: Send + 'static,
    {
        let mut buckets = self.inner.buckets.lock().expect("receptionist poisoned");
        let bucket = bucket_mut(&mut buckets, key);
        bucket.listing()
    }

    pub fn subscribe<M>(&self, key: ServiceKey<M>, subscriber: ActorRef<Listing<M>>) -> bool
    where
        M: Send + 'static,
    {
        let mut buckets = self.inner.buckets.lock().expect("receptionist poisoned");
        let bucket = bucket_mut(&mut buckets, &key);
        let subscribed = bucket.subscribe(subscriber.clone());
        let _ = subscriber.tell(bucket.listing());
        subscribed
    }

    pub(crate) fn remove_actor(&self, path: &ActorPath) {
        let mut buckets = self.inner.buckets.lock().expect("receptionist poisoned");
        for bucket in buckets.values_mut() {
            if bucket.remove_path(path) {
                bucket.publish_any_listing();
            }
        }
    }
}

fn bucket_mut<'a, M>(
    buckets: &'a mut HashMap<BucketKey, Box<dyn BucketOps>>,
    key: &ServiceKey<M>,
) -> &'a mut ReceptionistBucket<M>
where
    M: Send + 'static,
{
    let bucket_key = BucketKey::new(key);
    buckets
        .entry(bucket_key)
        .or_insert_with(|| Box::new(ReceptionistBucket::<M>::new(key.clone())))
        .as_any_mut()
        .downcast_mut::<ReceptionistBucket<M>>()
        .expect("receptionist bucket type must match service key")
}

fn existing_bucket_mut<'a, M>(
    buckets: &'a mut HashMap<BucketKey, Box<dyn BucketOps>>,
    key: &ServiceKey<M>,
) -> Option<&'a mut ReceptionistBucket<M>>
where
    M: Send + 'static,
{
    buckets
        .get_mut(&BucketKey::new(key))?
        .as_any_mut()
        .downcast_mut::<ReceptionistBucket<M>>()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BucketKey {
    id: String,
    type_id: TypeId,
}

impl BucketKey {
    fn new<M>(key: &ServiceKey<M>) -> Self {
        Self {
            id: key.id.clone(),
            type_id: key.type_id,
        }
    }
}

trait BucketOps: Any + Send {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn remove_path(&mut self, path: &ActorPath) -> bool;
    fn publish_any_listing(&self);
}

struct ReceptionistBucket<M> {
    key: ServiceKey<M>,
    services: Vec<ActorRef<M>>,
    subscribers: Vec<ActorRef<Listing<M>>>,
}

impl<M> fmt::Debug for ReceptionistBucket<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceptionistBucket")
            .field("key", &self.key)
            .field("services", &self.services.len())
            .field("subscribers", &self.subscribers.len())
            .finish()
    }
}

impl<M> ReceptionistBucket<M>
where
    M: Send + 'static,
{
    fn new(key: ServiceKey<M>) -> Self {
        Self {
            key,
            services: Vec::new(),
            subscribers: Vec::new(),
        }
    }

    fn register(&mut self, service: ActorRef<M>) -> bool {
        if self
            .services
            .iter()
            .any(|existing| existing.path() == service.path())
        {
            return false;
        }
        self.services.push(service);
        true
    }

    fn deregister(&mut self, path: &ActorPath) -> bool {
        remove_path_from(&mut self.services, path)
    }

    fn subscribe(&mut self, subscriber: ActorRef<Listing<M>>) -> bool {
        if self
            .subscribers
            .iter()
            .any(|existing| existing.path() == subscriber.path())
        {
            return false;
        }
        self.subscribers.push(subscriber);
        true
    }

    fn listing(&self) -> Listing<M> {
        Listing {
            key: self.key.clone(),
            service_instances: self.services.clone(),
        }
    }

    fn publish_listing(&self) {
        let listing = self.listing();
        for subscriber in &self.subscribers {
            let _ = subscriber.tell(listing.clone());
        }
    }
}

impl<M> BucketOps for ReceptionistBucket<M>
where
    M: Send + 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn remove_path(&mut self, path: &ActorPath) -> bool {
        let services_changed = remove_path_from(&mut self.services, path);
        remove_path_from(&mut self.subscribers, path);
        services_changed
    }

    fn publish_any_listing(&self) {
        self.publish_listing();
    }
}

fn remove_path_from<M: Send + 'static>(refs: &mut Vec<ActorRef<M>>, path: &ActorPath) -> bool {
    let before = refs.len();
    refs.retain(|actor| actor.path() != path);
    refs.len() != before
}
