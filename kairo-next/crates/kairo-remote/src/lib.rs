//! Remote actor references, associations, transports, and remote death watch.

pub use kairo_actor::{ActorPath, ActorRef};
pub use kairo_serialization::SerializedMessage;

#[derive(Debug, Clone)]
pub struct RemoteSettings {
    pub canonical_hostname: String,
    pub canonical_port: u16,
}
