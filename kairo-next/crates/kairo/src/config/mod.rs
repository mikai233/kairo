//! Format-neutral Kairo settings and TOML loading.

mod error;
mod runtime;
mod settings;
mod toml_loader;

pub use error::ConfigError;
#[cfg(feature = "cluster")]
pub use runtime::ConfiguredDowningHook;
pub use settings::{
    ActorConfig, ClusterConfig, ClusterDowningConfig, ClusterDowningStrategyConfig,
    ClusterHeartbeatConfig, ClusterSeedConfig, ClusterShardingAllocationConfig,
    ClusterShardingConfig, ClusterToolsConfig, DiagnosticsConfig, DispatcherConfig, KairoSettings,
    MailboxConfig, ObservabilityConfig, RemoteConfig, RemoteTransportConfig,
};
pub use toml_loader::{load_toml_file, load_toml_files, parse_toml_str};

#[cfg(test)]
mod tests;
