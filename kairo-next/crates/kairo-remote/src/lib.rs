//! Remote actor references, associations, transports, and remote death watch.

mod association;
mod error;
mod outbound;
mod protocol;
mod provider;
mod remote_ref;
mod settings;

pub use association::{AssociationState, RemoteAssociation};
pub use error::{RemoteError, Result};
pub use kairo_actor::ActorPath;
pub use kairo_serialization::{RemoteEnvelope, SerializedMessage};
pub use outbound::RemoteOutbound;
pub use protocol::{
    AddressTerminated, RemoteHeartbeat, RemoteHeartbeatAck, UnwatchRemote, WatchRemote,
};
pub use provider::RemoteActorRefProvider;
pub use remote_ref::RemoteActorRef;
pub use settings::RemoteSettings;
