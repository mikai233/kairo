//! Facade crate for Kairo Next.

#[cfg(feature = "actor")]
pub use kairo_actor as actor;
#[cfg(feature = "macros")]
pub use kairo_actor_macros as macros;
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
    #[cfg(feature = "actor")]
    pub use kairo_actor::prelude::*;
    #[cfg(feature = "macros")]
    pub use kairo_actor_macros::*;
    #[cfg(feature = "serialization")]
    pub use kairo_serialization::{
        DynCodec, Manifest, MessageCodec, RemoteMessage, SerializationError, SerializationRegistry,
        SerializedMessage, SerializerId,
    };
}
