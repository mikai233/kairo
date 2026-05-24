mod manager;
mod oldest;

pub use manager::{SingletonManagerEffect, SingletonManagerRuntime, SingletonManagerState};
pub use oldest::{
    SingletonOldestChange, SingletonOldestObservation, SingletonOldestTracker, SingletonScope,
};
