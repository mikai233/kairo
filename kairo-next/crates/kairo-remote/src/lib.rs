//! Remote actor references, associations, transports, and remote death watch.

mod association;
mod association_inbound;
mod association_outbound;
mod association_pipeline;
mod codec;
mod error;
mod frame;
mod inbound;
mod lanes;
mod local_delivery;
mod outbound;
mod protocol;
mod provider;
mod remote_ref;
mod remote_watch;
mod settings;
mod stream;
mod stream_inbound;
mod stream_sink;
mod transport;

pub use association::{AssociationState, RemoteAssociation};
pub use association_inbound::AssociationRemoteInbound;
pub use association_outbound::AssociationRemoteOutbound;
pub use association_pipeline::AssociationOutboundPipeline;
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
pub use lanes::{LaneRemoteOutbound, RemoteLaneClassifier, RemoteLaneSink, lane_send_failure};
pub use local_delivery::LocalActorInboundDelivery;
pub use outbound::RemoteOutbound;
pub use protocol::{
    AddressTerminated, RemoteHeartbeat, RemoteHeartbeatAck, UnwatchRemote, WatchRemote,
};
pub use provider::RemoteActorRefProvider;
pub use remote_ref::RemoteActorRef;
pub use remote_watch::{RemoteDeathWatchEffect, RemoteDeathWatchState};
pub use settings::RemoteSettings;
pub use stream::{
    RemoteStreamDecoder, RemoteStreamEncoder, RemoteStreamFrame, RemoteStreamId,
    decode_remote_stream_header, encode_remote_stream_frame, encode_remote_stream_header,
};
pub use stream_inbound::{RemoteFrameHandler, StreamFrameInbound};
pub use stream_sink::{RemoteByteSink, RemoteStreamWriter, StreamLaneSink, stream_send_failure};
pub use transport::{FramedRemoteInbound, FramedRemoteOutbound, RemoteFrameSink};
