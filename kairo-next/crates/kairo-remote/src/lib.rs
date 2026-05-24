//! Remote actor references, associations, transports, and remote death watch.

mod association;
mod codec;
mod error;
mod frame;
mod inbound;
mod outbound;
mod protocol;
mod provider;
mod remote_ref;
mod settings;
mod transport;

pub use association::{AssociationState, RemoteAssociation};
pub use codec::{
    ADDRESS_TERMINATED_SERIALIZER_ID, AddressTerminatedCodec, REMOTE_HEARTBEAT_ACK_SERIALIZER_ID,
    REMOTE_HEARTBEAT_SERIALIZER_ID, RemoteHeartbeatAckCodec, RemoteHeartbeatCodec,
    UNWATCH_REMOTE_SERIALIZER_ID, UnwatchRemoteCodec, WATCH_REMOTE_SERIALIZER_ID, WatchRemoteCodec,
    register_remote_protocol_codecs,
};
pub use error::{RemoteError, Result};
pub use frame::{decode_remote_envelope_frame, encode_remote_envelope_frame};
pub use inbound::{InboundMessage, RemoteInbound, RemoteInboundDelivery};
pub use kairo_actor::ActorPath;
pub use kairo_serialization::{RemoteEnvelope, SerializedMessage};
pub use outbound::RemoteOutbound;
pub use protocol::{
    AddressTerminated, RemoteHeartbeat, RemoteHeartbeatAck, UnwatchRemote, WatchRemote,
};
pub use provider::RemoteActorRefProvider;
pub use remote_ref::RemoteActorRef;
pub use settings::RemoteSettings;
pub use transport::{FramedRemoteInbound, FramedRemoteOutbound, RemoteFrameSink};
