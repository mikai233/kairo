mod actor;
mod manager;
mod oldest;

pub use actor::{SingletonManagerActor, SingletonManagerMsg, SingletonManagerSnapshot};
pub use manager::{SingletonManagerEffect, SingletonManagerRuntime, SingletonManagerState};
pub use oldest::{
    SingletonOldestChange, SingletonOldestObservation, SingletonOldestTracker, SingletonScope,
};
