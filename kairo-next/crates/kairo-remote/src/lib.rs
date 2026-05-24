//! Remote actor references, associations, transports, and remote death watch.

mod protocol;

pub use kairo_actor::{ActorPath, ActorRef};
pub use kairo_serialization::SerializedMessage;
pub use protocol::{
    AddressTerminated, RemoteHeartbeat, RemoteHeartbeatAck, UnwatchRemote, WatchRemote,
};

#[derive(Debug, Clone)]
pub struct RemoteSettings {
    pub canonical_hostname: String,
    pub canonical_port: u16,
}
