use std::time::Duration;

use super::error::ConfigError;
use super::settings::{
    ActorConfig, ClusterConfig, ClusterDowningConfig, ClusterHeartbeatConfig,
    ClusterShardingConfig, ClusterToolsConfig, DispatcherConfig, KairoSettings, RemoteConfig,
    RemoteTransportConfig,
};

impl KairoSettings {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.actor.validate()?;
        self.remote.validate()?;
        self.cluster.validate()?;
        Ok(())
    }
}

impl ActorConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.default_dispatcher()?
            .validated_throughput("actor.dispatchers.default.throughput")?;
        for (name, dispatcher) in &self.dispatchers {
            dispatcher.validated_throughput(format!("actor.dispatchers.{name}.throughput"))?;
        }
        Ok(())
    }

    pub fn default_dispatcher(&self) -> Result<&DispatcherConfig, ConfigError> {
        self.dispatchers
            .get("default")
            .ok_or_else(|| ConfigError::InvalidValue {
                path: "actor.dispatchers.default".to_string(),
                reason: "default dispatcher settings are required".to_string(),
            })
    }

    #[cfg(feature = "actor")]
    pub fn actor_system_builder(
        &self,
        name: impl Into<String>,
    ) -> Result<kairo_actor::ActorSystemBuilder, ConfigError> {
        Ok(
            kairo_actor::ActorSystem::builder(name).dispatcher_throughput(
                self.default_dispatcher()?
                    .validated_throughput("actor.dispatchers.default.throughput")?,
            ),
        )
    }
}

impl DispatcherConfig {
    pub fn validated_throughput(&self, path: impl Into<String>) -> Result<usize, ConfigError> {
        if self.throughput == 0 {
            Err(ConfigError::InvalidValue {
                path: path.into(),
                reason: "must be greater than zero".to_string(),
            })
        } else {
            Ok(self.throughput)
        }
    }
}

impl RemoteTransportConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.canonical_hostname.is_empty() {
            return Err(ConfigError::InvalidValue {
                path: "remote.transport.canonical_hostname".to_string(),
                reason: "must not be empty".to_string(),
            });
        }
        Ok(())
    }

    #[cfg(feature = "remote")]
    pub fn to_remote_settings(&self) -> Result<kairo_remote::RemoteSettings, ConfigError> {
        self.validate()?;
        Ok(kairo_remote::RemoteSettings::new(
            self.canonical_hostname.clone(),
            self.canonical_port,
        ))
    }
}

impl RemoteConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.transport.validate()
    }
}

impl ClusterConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.heartbeat.validate()?;
        self.downing.validate()?;
        self.sharding.validated_shard_count()?;
        self.tools.validate()?;
        Ok(())
    }
}

impl ClusterHeartbeatConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        reject_zero(
            self.monitored_by_nr_of_members,
            "cluster.heartbeat.monitored_by_nr_of_members",
        )?;
        reject_zero_duration(self.interval, "cluster.heartbeat.interval")?;
        reject_zero_duration(
            self.expected_response_after,
            "cluster.heartbeat.expected_response_after",
        )?;
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub fn to_failure_detector_settings(
        &self,
    ) -> Result<kairo_cluster::DeadlineFailureDetectorSettings, ConfigError> {
        self.validate()?;
        kairo_cluster::DeadlineFailureDetectorSettings::new(self.interval, self.acceptable_pause)
            .map_err(|_| ConfigError::InvalidValue {
                path: "cluster.heartbeat.interval".to_string(),
                reason: "must be greater than zero".to_string(),
            })
    }

    #[cfg(feature = "cluster")]
    pub fn to_heartbeat_sender_settings(
        &self,
    ) -> Result<kairo_cluster::HeartbeatSenderSettings, ConfigError> {
        let failure_detector = self.to_failure_detector_settings()?;
        Ok(kairo_cluster::HeartbeatSenderSettings::new(
            self.monitored_by_nr_of_members,
            failure_detector,
        )
        .with_heartbeat_expected_response_after(self.expected_response_after))
    }
}

impl ClusterDowningConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.strategy.is_empty() {
            return Err(ConfigError::InvalidValue {
                path: "cluster.downing.strategy".to_string(),
                reason: "must not be empty".to_string(),
            });
        }
        Ok(())
    }
}

impl ClusterShardingConfig {
    pub fn validated_shard_count(&self) -> Result<u64, ConfigError> {
        if self.number_of_shards == 0 {
            Err(ConfigError::InvalidValue {
                path: "cluster.sharding.number_of_shards".to_string(),
                reason: "must be greater than zero".to_string(),
            })
        } else {
            Ok(self.number_of_shards)
        }
    }

    #[cfg(feature = "cluster-sharding")]
    pub fn default_shard_count_matches_runtime(&self) -> bool {
        self.number_of_shards == kairo_cluster_sharding::DEFAULT_SHARD_COUNT
    }
}

impl ClusterToolsConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        reject_zero_duration(
            self.pubsub_gossip_interval,
            "cluster.tools.pubsub.gossip_interval",
        )?;
        reject_zero(
            self.pubsub_max_delta_entries,
            "cluster.tools.pubsub.max_delta_entries",
        )?;
        Ok(())
    }
}

fn reject_zero(value: usize, path: &str) -> Result<(), ConfigError> {
    if value == 0 {
        Err(ConfigError::InvalidValue {
            path: path.to_string(),
            reason: "must be greater than zero".to_string(),
        })
    } else {
        Ok(())
    }
}

fn reject_zero_duration(duration: Duration, path: &str) -> Result<(), ConfigError> {
    if duration.is_zero() {
        Err(ConfigError::InvalidValue {
            path: path.to_string(),
            reason: "must be greater than zero".to_string(),
        })
    } else {
        Ok(())
    }
}
