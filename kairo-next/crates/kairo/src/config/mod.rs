//! Format-neutral Kairo settings and TOML loading.
//!
//! The initial file format is TOML, but the public settings structs in this
//! module deliberately avoid TOML-specific concepts. File loaders project TOML
//! documents into [`KairoSettings`], validate the result, and the runtime
//! helpers convert those settings into actor, remote, cluster, sharding, and
//! diagnostics builder values.
//!
//! Use [`load_toml_file`] or [`load_toml_files`] for explicit configuration
//! paths, [`load_standard_toml_files`] for the standard `kairo.toml` plus
//! `kairo.local.toml` discovery path, and [`parse_toml_str`] when tests,
//! embedded defaults, or higher-level discovery already provide configuration
//! text.

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
pub use toml_loader::{
    STANDARD_TOML_FILES, find_standard_toml_files, load_standard_toml_files, load_toml_file,
    load_toml_files, parse_toml_str,
};

#[cfg(test)]
mod tests;
