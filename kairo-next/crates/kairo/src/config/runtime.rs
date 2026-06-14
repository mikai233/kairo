#[cfg(any(feature = "cluster", feature = "remote"))]
use std::sync::Arc;
use std::time::Duration;

use super::error::ConfigError;
use super::settings::{
    ActorConfig, ClusterConfig, ClusterDowningConfig, ClusterDowningStrategyConfig,
    ClusterHeartbeatConfig, ClusterSeedConfig, ClusterShardingAllocationConfig,
    ClusterShardingConfig, ClusterToolsConfig, DiagnosticsConfig, DispatcherConfig, KairoSettings,
    MailboxConfig, ObservabilityConfig, RemoteConfig, RemoteTransportConfig,
};

#[cfg(feature = "cluster")]
#[derive(Debug, Clone)]
pub enum ConfiguredDowningHook {
    None,
    SplitBrain(kairo_cluster::SplitBrainResolverHook),
}

#[cfg(feature = "cluster")]
impl kairo_cluster::DowningHook for ConfiguredDowningHook {
    fn decide(
        &self,
        gossip: &kairo_cluster::Gossip,
        self_node: &kairo_cluster::UniqueAddress,
    ) -> kairo_cluster::DowningDecision {
        match self {
            Self::None => kairo_cluster::NoDowning.decide(gossip, self_node),
            Self::SplitBrain(hook) => hook.decide(gossip, self_node),
        }
    }

    fn decision_delay(
        &self,
        gossip: &kairo_cluster::Gossip,
        self_node: &kairo_cluster::UniqueAddress,
    ) -> Duration {
        match self {
            Self::None => kairo_cluster::NoDowning.decision_delay(gossip, self_node),
            Self::SplitBrain(hook) => hook.decision_delay(gossip, self_node),
        }
    }

    fn plan(
        &self,
        gossip: &kairo_cluster::Gossip,
        self_node: &kairo_cluster::UniqueAddress,
    ) -> kairo_cluster::DowningPlan {
        match self {
            Self::None => kairo_cluster::NoDowning.plan(gossip, self_node),
            Self::SplitBrain(hook) => hook.plan(gossip, self_node),
        }
    }
}

impl KairoSettings {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.actor.validate()?;
        self.remote.validate()?;
        self.cluster.validate()?;
        self.observability.validate()?;
        Ok(())
    }

    #[cfg(feature = "actor")]
    pub fn actor_system_builder(
        &self,
        name: impl Into<String>,
    ) -> Result<kairo_actor::ActorSystemBuilder, ConfigError> {
        Ok(self
            .actor
            .actor_system_builder(name)?
            .publish_dead_letters_to_event_stream(self.observability.diagnostics.dead_letters))
    }
}

impl ActorConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.default_dispatcher()?
            .validated_throughput("actor.dispatchers.default.throughput")?;
        for (name, dispatcher) in &self.dispatchers {
            dispatcher.validated_throughput(format!("actor.dispatchers.{name}.throughput"))?;
        }
        self.default_mailbox()?
            .validate("actor.mailboxes.default")?;
        for (name, mailbox) in &self.mailboxes {
            mailbox.validate(format!("actor.mailboxes.{name}"))?;
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

    pub fn default_mailbox(&self) -> Result<&MailboxConfig, ConfigError> {
        self.mailboxes
            .get("default")
            .ok_or_else(|| ConfigError::InvalidValue {
                path: "actor.mailboxes.default".to_string(),
                reason: "default mailbox settings are required".to_string(),
            })
    }

    #[cfg(feature = "actor")]
    pub fn actor_system_builder(
        &self,
        name: impl Into<String>,
    ) -> Result<kairo_actor::ActorSystemBuilder, ConfigError> {
        let mut builder = kairo_actor::ActorSystem::builder(name).dispatcher_throughput(
            self.default_dispatcher()?
                .validated_throughput("actor.dispatchers.default.throughput")?,
        );
        if let Some(capacity) = self
            .default_mailbox()?
            .validated_capacity("actor.mailboxes.default.capacity")?
        {
            builder = builder.mailbox_capacity(capacity);
        }
        Ok(builder)
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

impl MailboxConfig {
    pub fn validate(&self, path: impl Into<String>) -> Result<(), ConfigError> {
        let path = path.into();
        self.validated_capacity(format!("{path}.capacity"))?;
        Ok(())
    }

    pub fn validated_capacity(
        &self,
        path: impl Into<String>,
    ) -> Result<Option<usize>, ConfigError> {
        match self.capacity {
            Some(0) => Err(ConfigError::InvalidValue {
                path: path.into(),
                reason: "must be greater than zero when set".to_string(),
            }),
            capacity => Ok(capacity),
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
        self.seed.validate()?;
        self.heartbeat.validate()?;
        self.downing.validate()?;
        self.sharding.validate()?;
        self.tools.validate()?;
        Ok(())
    }
}

impl ClusterSeedConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (index, node) in self.nodes.iter().enumerate() {
            if node.trim().is_empty() {
                return Err(ConfigError::InvalidValue {
                    path: format!("cluster.seed.nodes[{index}]"),
                    reason: "must not be empty".to_string(),
                });
            }
        }
        Ok(())
    }

    #[cfg(feature = "remote")]
    pub fn to_remote_association_addresses(
        &self,
    ) -> Result<Vec<kairo_remote::RemoteAssociationAddress>, ConfigError> {
        self.validate()?;
        self.nodes
            .iter()
            .enumerate()
            .map(|(index, node)| {
                node.parse::<kairo_remote::RemoteAssociationAddress>()
                    .map_err(|error| ConfigError::InvalidValue {
                        path: format!("cluster.seed.nodes[{index}]"),
                        reason: error.to_string(),
                    })
            })
            .collect()
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
        reject_zero_duration(self.stable_after, "cluster.downing.stable_after")?;
        if let ClusterDowningStrategyConfig::LeaseMajority {
            lease_name,
            release_after,
            ..
        } = &self.strategy
        {
            if lease_name.trim().is_empty() {
                return Err(ConfigError::InvalidValue {
                    path: "cluster.downing.lease_name".to_string(),
                    reason: "must not be empty for lease-majority".to_string(),
                });
            }
            reject_zero_duration(*release_after, "cluster.downing.release_after")?;
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub fn to_downing_hook(&self) -> Result<ConfiguredDowningHook, ConfigError> {
        self.validate()?;
        match &self.strategy {
            ClusterDowningStrategyConfig::None => Ok(ConfiguredDowningHook::None),
            ClusterDowningStrategyConfig::DownAll => Ok(ConfiguredDowningHook::SplitBrain(
                kairo_cluster::SplitBrainResolverHook::down_all(),
            )),
            ClusterDowningStrategyConfig::KeepMajority { role } => {
                Ok(ConfiguredDowningHook::SplitBrain(
                    kairo_cluster::SplitBrainResolverHook::keep_majority(role.clone()),
                ))
            }
            ClusterDowningStrategyConfig::KeepOldest {
                role,
                down_if_alone,
            } => Ok(ConfiguredDowningHook::SplitBrain(
                kairo_cluster::SplitBrainResolverHook::keep_oldest(role.clone(), *down_if_alone),
            )),
            ClusterDowningStrategyConfig::LeaseMajority { .. } => Err(ConfigError::InvalidValue {
                path: "cluster.downing.strategy".to_string(),
                reason:
                    "lease-majority requires to_lease_majority_hook with an explicit lease implementation"
                        .to_string(),
            }),
        }
    }

    #[cfg(feature = "cluster")]
    pub fn to_lease_majority_settings(
        &self,
    ) -> Result<kairo_cluster::LeaseMajoritySettings, ConfigError> {
        self.validate()?;
        let ClusterDowningStrategyConfig::LeaseMajority {
            lease_name,
            role,
            acquire_lease_delay_for_minority,
            release_after,
        } = &self.strategy
        else {
            return Err(ConfigError::InvalidValue {
                path: "cluster.downing.strategy".to_string(),
                reason: "expected lease-majority".to_string(),
            });
        };
        kairo_cluster::LeaseMajoritySettings::new(
            lease_name.clone(),
            role.clone(),
            *acquire_lease_delay_for_minority,
            *release_after,
        )
        .map_err(|error| ConfigError::InvalidValue {
            path: "cluster.downing.lease_name".to_string(),
            reason: error.to_string(),
        })
    }

    #[cfg(feature = "cluster")]
    pub fn to_lease_majority_hook<L>(
        &self,
        lease: L,
    ) -> Result<kairo_cluster::LeaseMajorityHook<L>, ConfigError>
    where
        L: kairo_cluster::LeaseMajorityLease,
    {
        Ok(kairo_cluster::LeaseMajorityHook::new(
            self.to_lease_majority_settings()?,
            lease,
        ))
    }
}

impl ClusterShardingConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validated_shard_count()?;
        reject_zero_duration(self.retry_interval, "cluster.sharding.retry_interval")?;
        reject_zero_duration(self.handoff_timeout, "cluster.sharding.handoff_timeout")?;
        reject_zero_duration(
            self.shard_failure_backoff,
            "cluster.sharding.shard_failure_backoff",
        )?;
        reject_zero_duration(
            self.rebalance_interval,
            "cluster.sharding.rebalance_interval",
        )?;
        reject_zero_duration(
            self.shard_region_query_timeout,
            "cluster.sharding.shard_region_query_timeout",
        )?;
        self.least_shard_allocation.validate()?;
        Ok(())
    }

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
    pub fn to_shard_count(&self) -> Result<u64, ConfigError> {
        self.validated_shard_count()
    }

    pub fn to_retry_interval(&self) -> Result<Duration, ConfigError> {
        reject_zero_duration(self.retry_interval, "cluster.sharding.retry_interval")?;
        Ok(self.retry_interval)
    }

    pub fn to_handoff_timeout(&self) -> Result<Duration, ConfigError> {
        reject_zero_duration(self.handoff_timeout, "cluster.sharding.handoff_timeout")?;
        Ok(self.handoff_timeout)
    }

    pub fn to_shard_failure_backoff(&self) -> Result<Duration, ConfigError> {
        reject_zero_duration(
            self.shard_failure_backoff,
            "cluster.sharding.shard_failure_backoff",
        )?;
        Ok(self.shard_failure_backoff)
    }

    pub fn to_rebalance_interval(&self) -> Result<Duration, ConfigError> {
        reject_zero_duration(
            self.rebalance_interval,
            "cluster.sharding.rebalance_interval",
        )?;
        Ok(self.rebalance_interval)
    }

    pub fn to_shard_region_query_timeout(&self) -> Result<Duration, ConfigError> {
        reject_zero_duration(
            self.shard_region_query_timeout,
            "cluster.sharding.shard_region_query_timeout",
        )?;
        Ok(self.shard_region_query_timeout)
    }

    pub fn remember_entities_enabled(&self) -> bool {
        self.remember_entities
    }

    #[cfg(feature = "cluster-sharding")]
    pub fn shard_id_for(
        &self,
        entity_id: impl AsRef<str>,
    ) -> Result<kairo_cluster_sharding::ShardId, ConfigError> {
        kairo_cluster_sharding::shard_id_for(entity_id, self.to_shard_count()?).map_err(|error| {
            ConfigError::InvalidValue {
                path: "cluster.sharding.number_of_shards".to_string(),
                reason: error.to_string(),
            }
        })
    }

    #[cfg(feature = "cluster-sharding")]
    pub fn default_shard_count_matches_runtime(&self) -> bool {
        self.number_of_shards == kairo_cluster_sharding::DEFAULT_SHARD_COUNT
    }

    #[cfg(feature = "cluster-sharding")]
    pub fn to_least_shard_allocation_strategy(
        &self,
    ) -> Result<kairo_cluster_sharding::LeastShardAllocationStrategy, ConfigError> {
        self.least_shard_allocation
            .to_least_shard_allocation_strategy()
    }
}

impl ClusterShardingAllocationConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        reject_zero(
            self.rebalance_absolute_limit,
            "cluster.sharding.least_shard_allocation.rebalance_absolute_limit",
        )?;
        if !self.rebalance_relative_limit.is_finite() || self.rebalance_relative_limit <= 0.0 {
            return Err(ConfigError::InvalidValue {
                path: "cluster.sharding.least_shard_allocation.rebalance_relative_limit"
                    .to_string(),
                reason: "must be finite and greater than zero".to_string(),
            });
        }
        Ok(())
    }

    #[cfg(feature = "cluster-sharding")]
    pub fn to_least_shard_allocation_strategy(
        &self,
    ) -> Result<kairo_cluster_sharding::LeastShardAllocationStrategy, ConfigError> {
        self.validate()?;
        kairo_cluster_sharding::LeastShardAllocationStrategy::new(
            self.rebalance_absolute_limit,
            self.rebalance_relative_limit,
        )
        .map_err(|error| ConfigError::InvalidValue {
            path: "cluster.sharding.least_shard_allocation".to_string(),
            reason: error.to_string(),
        })
    }
}

impl ClusterToolsConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self
            .singleton_role
            .as_ref()
            .is_some_and(|role| role.trim().is_empty())
        {
            return Err(ConfigError::InvalidValue {
                path: "cluster.tools.singleton.role".to_string(),
                reason: "must not be empty when set".to_string(),
            });
        }
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

    #[cfg(feature = "cluster-tools")]
    pub fn to_singleton_scope(&self) -> Result<kairo_cluster_tools::SingletonScope, ConfigError> {
        self.validate()?;
        Ok(match &self.singleton_role {
            Some(role) => kairo_cluster_tools::SingletonScope::for_role(role.clone()),
            None => kairo_cluster_tools::SingletonScope::all(),
        })
    }

    pub fn to_pubsub_gossip_interval(&self) -> Result<Duration, ConfigError> {
        reject_zero_duration(
            self.pubsub_gossip_interval,
            "cluster.tools.pubsub.gossip_interval",
        )?;
        Ok(self.pubsub_gossip_interval)
    }

    pub fn to_pubsub_max_delta_entries(&self) -> Result<usize, ConfigError> {
        reject_zero(
            self.pubsub_max_delta_entries,
            "cluster.tools.pubsub.max_delta_entries",
        )?;
        Ok(self.pubsub_max_delta_entries)
    }

    #[cfg(feature = "cluster-tools")]
    pub fn to_pubsub_gossip_actor(
        &self,
        self_node: kairo_cluster::UniqueAddress,
    ) -> Result<kairo_cluster_tools::PubSubGossipActor, ConfigError> {
        self.validate()?;
        Ok(kairo_cluster_tools::PubSubGossipActor::new(self_node)
            .with_max_delta_entries(self.to_pubsub_max_delta_entries()?))
    }
}

impl ObservabilityConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.diagnostics.validate()
    }
}

impl DiagnosticsConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }

    pub fn publishes_runtime_failures(&self) -> bool {
        self.remote_delivery_failures
            || self.serialization_failures
            || self.quarantine_events
            || self.gossip_state_changes
    }

    #[cfg(feature = "remote")]
    pub fn remote_inbound_diagnostic_filter(&self) -> kairo_remote::RemoteInboundDiagnosticFilter {
        kairo_remote::RemoteInboundDiagnosticFilter::new(
            self.serialization_failures,
            self.remote_delivery_failures,
        )
    }

    #[cfg(feature = "remote")]
    pub fn remote_inbound_diagnostics(
        &self,
        diagnostics: Arc<dyn kairo_remote::RemoteInboundDiagnostics>,
    ) -> Option<Arc<dyn kairo_remote::RemoteInboundDiagnostics>> {
        self.remote_inbound_diagnostic_filter().wrap(diagnostics)
    }

    #[cfg(feature = "remote")]
    pub fn remote_association_diagnostic_filter(
        &self,
    ) -> kairo_remote::RemoteAssociationDiagnosticFilter {
        kairo_remote::RemoteAssociationDiagnosticFilter::new(self.quarantine_events)
    }

    #[cfg(feature = "remote")]
    pub fn remote_association_diagnostics(
        &self,
        diagnostics: Arc<dyn kairo_remote::RemoteAssociationDiagnostics>,
    ) -> Option<Arc<dyn kairo_remote::RemoteAssociationDiagnostics>> {
        self.remote_association_diagnostic_filter()
            .wrap(diagnostics)
    }

    #[cfg(feature = "cluster")]
    pub fn cluster_diagnostic_filter(&self) -> kairo_cluster::ClusterDiagnosticFilter {
        kairo_cluster::ClusterDiagnosticFilter::new(self.gossip_state_changes)
    }

    #[cfg(feature = "cluster")]
    pub fn cluster_diagnostics(
        &self,
        diagnostics: Arc<dyn kairo_cluster::ClusterDiagnostics>,
    ) -> Option<Arc<dyn kairo_cluster::ClusterDiagnostics>> {
        self.cluster_diagnostic_filter().wrap(diagnostics)
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
