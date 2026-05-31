//! Format-neutral Kairo settings and TOML loading.

mod error;
mod runtime;
mod settings;
mod toml_loader;

pub use error::ConfigError;
pub use settings::{
    ActorConfig, ClusterConfig, ClusterDowningConfig, ClusterDowningStrategyConfig,
    ClusterHeartbeatConfig, ClusterSeedConfig, ClusterShardingConfig, ClusterToolsConfig,
    DispatcherConfig, KairoSettings, RemoteConfig, RemoteTransportConfig,
};
pub use toml_loader::{load_toml_file, parse_toml_str};

#[cfg(test)]
mod tests;
