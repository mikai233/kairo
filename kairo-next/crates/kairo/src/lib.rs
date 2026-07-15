//! Facade crate for Kairo Next.
//!
//! `kairo` is the user-facing entry point for the Rust-first rewrite. It keeps
//! the implementation in focused crates and re-exports them behind feature
//! flags, so applications can depend on one facade without collapsing actor,
//! serialization, remote, cluster, distributed-data, sharding, tools, and
//! testkit logic into one crate.
//!
//! The default feature set enables typed local actors, macros, and the
//! format-neutral configuration model with a TOML loader. Distributed features
//! are opt-in and preserve the lower-level crate boundaries: `remote` enables
//! stable remote-message metadata and remoting, `cluster` builds on remoting
//! with gossip membership, `distributed-data` builds on cluster routes,
//! `cluster-sharding` builds on cluster and distributed data, and
//! `cluster-tools` builds cluster singleton and pubsub utilities on top of
//! cluster state.
//! `load_standard_toml_files` discovers `kairo.toml` and `kairo.local.toml`
//! in standard load order, while `load_toml_files` can layer explicit file
//! paths. Both paths recursively merge tables and let later files override
//! scalar values before the result is projected into `KairoSettings`.
//!
//! Local-only actor messages do not require serialization. Remote-capable
//! messages still use stable manifests, versions, serializer ids, and
//! registered codecs from `kairo-serialization`; wire compatibility must not
//! depend on Rust type names, enum discriminants, or memory layout.
//!
//! ```
//! use std::time::Duration;
//!
//! use kairo::prelude::*;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let settings = parse_toml_str(
//!     r#"
//! [actor.dispatchers.default]
//! throughput = 16
//!
//! [actor.mailboxes.default]
//! capacity = 1024
//!
//! [remote.transport]
//! canonical_hostname = "127.0.0.1"
//! canonical_port = 25521
//! connect_timeout = "1s"
//!
//! [cluster.seed]
//! nodes = ["kairo://worker@127.0.0.1:25521"]
//!
//! [cluster.heartbeat]
//! monitored_by_nr_of_members = 3
//! interval = "500ms"
//! acceptable_pause = "2s"
//! expected_response_after = "1s"
//!
//! [cluster.sharding]
//! number_of_shards = 100
//! remember_entities = false
//! retry_interval = "2s"
//! handoff_timeout = "60s"
//! shard_failure_backoff = "10s"
//! rebalance_interval = "10s"
//! shard_region_query_timeout = "3s"
//!
//! [cluster.sharding.least_shard_allocation]
//! rebalance_absolute_limit = 10
//! rebalance_relative_limit = 0.1
//!
//! [observability.diagnostics]
//! dead_letters = true
//! remote_delivery_failures = true
//! association_close_events = true
//! "#,
//! )?;
//!
//! assert_eq!(settings.actor.default_dispatcher()?.throughput, 16);
//! assert_eq!(settings.actor.default_mailbox()?.capacity, Some(1024));
//! assert_eq!(settings.remote.transport.canonical_port, 25521);
//! assert_eq!(settings.remote.transport.connect_timeout, Some(Duration::from_secs(1)));
//! assert_eq!(settings.cluster.seed.nodes.len(), 1);
//! assert_eq!(settings.cluster.heartbeat.interval, Duration::from_millis(500));
//! assert_eq!(settings.cluster.sharding.number_of_shards, 100);
//! assert_eq!(settings.cluster.sharding.handoff_timeout, Duration::from_secs(60));
//! assert_eq!(
//!     settings
//!         .cluster
//!         .sharding
//!         .least_shard_allocation
//!         .rebalance_absolute_limit,
//!     10
//! );
//! assert!(settings.observability.diagnostics.dead_letters);
//! # Ok(())
//! # }
//! ```
//!
//! Observability settings stay backend-neutral. The dead-letter flag maps into
//! the actor-system builder, while remote inbound diagnostics can be filtered
//! with `DiagnosticsConfig::remote_inbound_diagnostics` before installing a
//! caller-provided observer. Remote association and cluster gossip diagnostics
//! use the same pattern through
//! `DiagnosticsConfig::remote_association_diagnostics` and
//! `DiagnosticsConfig::cluster_diagnostics`.
//! When a dependency-free observability bridge is enough,
//! [`observability::DiagnosticCounters`] and
//! [`observability::DiagnosticTextSink`] implement the enabled
//! remote and cluster diagnostic observer traits, exposing point-in-time
//! [`observability::DiagnosticCounterSnapshot`] values or stable text lines for
//! export.
//!
//! `kairo::prelude` exports the common typed actor API plus enabled facade
//! features. For subsystem-specific protocols and lower-level test fixtures,
//! import the focused crates or feature-gated modules directly.

#[cfg(feature = "actor")]
pub use kairo_actor as actor;
#[cfg(feature = "config")]
pub mod config;
#[cfg(feature = "macros")]
pub use kairo_actor_macros as macros;
pub mod observability;
#[cfg(feature = "cluster")]
pub use kairo_cluster as cluster;
#[cfg(feature = "cluster-sharding")]
pub use kairo_cluster_sharding as cluster_sharding;
#[cfg(feature = "cluster-tools")]
pub use kairo_cluster_tools as cluster_tools;
#[cfg(feature = "distributed-data")]
pub use kairo_distributed_data as distributed_data;
#[cfg(feature = "remote")]
pub use kairo_remote as remote;
#[cfg(feature = "serialization")]
pub use kairo_serialization as serialization;
#[cfg(feature = "testkit")]
pub use kairo_testkit as testkit;

pub mod prelude {
    #[cfg(all(feature = "config", feature = "cluster"))]
    pub use crate::config::ConfiguredDowningHook;
    #[cfg(feature = "config")]
    pub use crate::config::{
        ActorConfig, ClusterConfig, ClusterDowningConfig, ClusterDowningStrategyConfig,
        ClusterHeartbeatConfig, ClusterSeedConfig, ClusterShardingAllocationConfig,
        ClusterShardingConfig, ClusterToolsConfig, ConfigError, DiagnosticsConfig,
        DispatcherConfig, KairoSettings, MailboxConfig, ObservabilityConfig, RemoteConfig,
        RemoteTransportConfig, STANDARD_TOML_FILES, find_standard_toml_files,
        load_standard_toml_files, load_toml_file, load_toml_files, parse_toml_str,
    };
    pub use crate::observability::{
        DiagnosticCounterSnapshot, DiagnosticCounters, DiagnosticTextSink,
    };
    #[cfg(feature = "actor")]
    pub use kairo_actor::prelude::*;
    #[cfg(feature = "macros")]
    pub use kairo_actor_macros::*;
    #[cfg(feature = "cluster")]
    pub use kairo_cluster::{
        CLUSTER_SYSTEM_MANIFESTS, Cluster, ClusterDiagnostic, ClusterDiagnosticFilter,
        ClusterDiagnostics, ClusterError, ClusterEvent, ClusterExtension, ClusterSubscriptionEvent,
        ClusterSubscriptionInitialState, CurrentClusterState, Member, MemberEvent, MemberStatus,
        ReachabilityEvent, UniqueAddress, register_cluster_system_inbound,
    };
    #[cfg(feature = "cluster-sharding")]
    pub use kairo_cluster_sharding::{
        ClusterSharding, ClusterShardingBootstrapError, ClusterShardingRegistration,
        ClusterShardingSettings, DEFAULT_SHARD_COUNT, Entity, EntityRef, EntityTypeKey,
        ShardRegionActor, ShardRegionMsg, ShardingEnvelope, ShardingEnvelopeRouter, ShardingError,
        default_shard_id_for, register_cluster_sharding, shard_id_for, stable_hash_entity_id,
    };
    #[cfg(feature = "cluster-tools")]
    pub use kairo_cluster_tools::{
        DistributedPubSubMediatorActor, DistributedPubSubMediatorMsg, LocalPubSub,
        LocalSingletonManagerActor, LocalSingletonManagerMsg, SingletonManagerSettings,
        SingletonProxyActor, SingletonProxyMsg, SingletonScope, TopicName, TopicPublishMode,
    };
    #[cfg(feature = "distributed-data")]
    pub use kairo_distributed_data::{
        DDATA_SYSTEM_MANIFESTS, DeltaReplicatedData, DistributedDataBootstrapError,
        DistributedDataExtension, DistributedDataHandle, DistributedDataRegistration,
        DistributedDataSettings, GCounter, GSet, GetResponse, ORSet, PNCounter, ReadConsistency,
        ReplicaId, ReplicatedData, ReplicatedDelta, ReplicatorActor, ReplicatorActorMsg,
        ReplicatorKey, ReplicatorState, UpdateResponse, WriteConsistency,
        register_distributed_data,
    };
    #[cfg(feature = "remote")]
    pub use kairo_remote::{
        ReliableSystemAck, ReliableSystemDeliveryFailure, ReliableSystemDeliveryObserver,
        ReliableSystemDeliverySettings, ReliableSystemDeliveryStats, ReliableSystemEnvelope,
        ReliableSystemNack, ReliableSystemReceiveOutcome, ReliableSystemReceiver,
        ReliableSystemSender, RemoteActorRef, RemoteActorRefProvider, RemoteActorRefResolver,
        RemoteAssociationDiagnostic, RemoteAssociationDiagnosticFilter,
        RemoteAssociationDiagnostics, RemoteError, RemoteInboundDiagnostic,
        RemoteInboundDiagnosticFilter, RemoteInboundDiagnostics, RemoteOutbound,
        RemoteOutboundQueueSettings, RemoteSettings, ResolvedActorRef,
        TcpAssociationAssemblySettings, TcpHandshakeReadSettings, TcpRemoteActorRuntime,
        TcpRemoteActorRuntimeBuilder, TcpRemoteActorRuntimeContext, TcpRemoteActorSystem,
    };
    #[cfg(feature = "serialization")]
    pub use kairo_serialization::{
        DynCodec, Manifest, MessageCodec, RemoteMessage, SerializationError, SerializationRegistry,
        SerializedMessage, SerializerId,
    };
    #[cfg(feature = "testkit")]
    pub use kairo_testkit::{
        ActorSystemTestKit, FishingOutcome, ManualTime, ManualTimeHandle, MultiNode,
        MultiNodeError, MultiNodeResult, MultiNodeTestKit, TestProbe, await_assert,
    };
}

#[cfg(test)]
mod tests;
