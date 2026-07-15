use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use crate::{ActorError, ActorSystem};

/// Marker trait for thread-safe services stored once per actor system.
pub trait Extension: Any + Send + Sync + 'static {}

impl<T> Extension for T where T: Any + Send + Sync + 'static {}

#[derive(Clone, Default)]
/// Type-indexed registry of lazily created actor-system extensions.
pub struct ExtensionRegistry {
    inner: Arc<ExtensionRegistryInner>,
}

#[derive(Default)]
struct ExtensionRegistryInner {
    extensions: Mutex<HashMap<TypeId, Arc<ExtensionSlot>>>,
}

struct ExtensionSlot {
    value: OnceLock<Arc<dyn Any + Send + Sync>>,
}

impl ExtensionSlot {
    fn new() -> Self {
        Self {
            value: OnceLock::new(),
        }
    }
}

impl fmt::Debug for ExtensionRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let extension_count = self
            .inner
            .extensions
            .lock()
            .expect("extension registry lock poisoned")
            .len();
        f.debug_struct("ExtensionRegistry")
            .field("extension_count", &extension_count)
            .finish_non_exhaustive()
    }
}

impl ExtensionRegistry {
    /// Returns the existing extension of type `T` or creates it exactly once.
    pub fn register<T, F>(&self, system: &ActorSystem, create: F) -> Arc<T>
    where
        T: Extension,
        F: FnOnce(&ActorSystem) -> T,
    {
        let slot = self.slot::<T>();
        let extension = slot
            .value
            .get_or_init(|| Arc::new(create(system)) as Arc<dyn Any + Send + Sync>)
            .clone();
        Arc::downcast::<T>(extension)
            .expect("extension registry stored a value under the wrong TypeId")
    }

    /// Looks up a previously registered extension by its concrete type.
    pub fn extension<T>(&self) -> Result<Arc<T>, ActorError>
    where
        T: Extension,
    {
        let Some(slot) = self.existing_slot::<T>() else {
            return Err(ActorError::ExtensionNotRegistered(type_name::<T>()));
        };
        let Some(extension) = slot.value.get() else {
            return Err(ActorError::ExtensionNotRegistered(type_name::<T>()));
        };
        Arc::downcast::<T>(Arc::clone(extension))
            .map_err(|_| ActorError::ExtensionNotRegistered(type_name::<T>()))
    }

    /// Returns whether an extension of type `T` has completed registration.
    pub fn has_extension<T>(&self) -> bool
    where
        T: Extension,
    {
        self.existing_slot::<T>()
            .and_then(|slot| slot.value.get().cloned())
            .is_some()
    }

    fn slot<T>(&self) -> Arc<ExtensionSlot>
    where
        T: Extension,
    {
        let mut extensions = self
            .inner
            .extensions
            .lock()
            .expect("extension registry lock poisoned");
        extensions
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Arc::new(ExtensionSlot::new()))
            .clone()
    }

    fn existing_slot<T>(&self) -> Option<Arc<ExtensionSlot>>
    where
        T: Extension,
    {
        self.inner
            .extensions
            .lock()
            .expect("extension registry lock poisoned")
            .get(&TypeId::of::<T>())
            .cloned()
    }
}
