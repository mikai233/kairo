mod actor;
mod local_manager;
mod manager;
mod oldest;
mod proxy;
mod proxy_routes;
mod proxy_target;
mod remote;

pub use actor::{SingletonManagerActor, SingletonManagerMsg, SingletonManagerSnapshot};
pub use local_manager::{
    LocalSingletonManagerActor, LocalSingletonManagerMsg, LocalSingletonManagerSnapshot,
};
pub use manager::{SingletonManagerEffect, SingletonManagerRuntime, SingletonManagerState};
pub use oldest::{
    SingletonOldestChange, SingletonOldestObservation, SingletonOldestTracker, SingletonScope,
};
pub use proxy::{
    SingletonProxyActor, SingletonProxyMsg, SingletonProxySettings, SingletonProxySettingsError,
    SingletonProxySnapshot,
};
pub use proxy_target::SingletonProxyTarget;
pub use remote::{
    DEFAULT_SINGLETON_MANAGER_REMOTE_PATH, SingletonManagerRemoteError,
    SingletonManagerRemoteInbound, SingletonManagerRemoteOutbound,
};
