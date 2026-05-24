mod actor;
mod local_manager;
mod manager;
mod oldest;

pub use actor::{SingletonManagerActor, SingletonManagerMsg, SingletonManagerSnapshot};
pub use local_manager::{
    LocalSingletonManagerActor, LocalSingletonManagerMsg, LocalSingletonManagerSnapshot,
};
pub use manager::{SingletonManagerEffect, SingletonManagerRuntime, SingletonManagerState};
pub use oldest::{
    SingletonOldestChange, SingletonOldestObservation, SingletonOldestTracker, SingletonScope,
};
