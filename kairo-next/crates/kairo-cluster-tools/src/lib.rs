//! Higher-level cluster utilities built on top of `kairo-cluster`.

mod singleton;
mod topic;

pub use singleton::{
    SingletonOldestChange, SingletonOldestObservation, SingletonOldestTracker, SingletonScope,
};
pub use topic::TopicName;

#[cfg(test)]
mod tests;
