//! Remote actor references, associations, transports, and remote death watch.
//!
//! `kairo-remote` is the typed boundary between local actors and transport
//! frames. It keeps local `ActorRef<M>` semantics in `kairo-actor`, stable wire
//! metadata in `kairo-serialization`, and remote delivery in focused modules:
//! [`RemoteActorRef`] serializes typed messages, [`RemoteActorRefProvider`]
//! resolves addressed paths, association and lane types model outbound link
//! state, stream/frame helpers provide transport-neutral bytes, and remote
//! death-watch state keeps the heartbeat/watch protocol explicit.
//!
//! A remote-capable message must implement
//! [`RemoteMessage`](kairo_serialization::RemoteMessage) and have a registered
//! [`MessageCodec`](kairo_serialization::MessageCodec). The wire contract is
//! the registered serializer id, manifest, version, recipient actor-ref wire
//! data, optional sender actor-ref wire data, and payload bytes. Do not rely on
//! Rust enum discriminants, type names, or memory layout for remote
//! compatibility.
//!
//! ```
//! use std::sync::{Arc, Mutex};
//!
//! use bytes::Bytes;
//! use kairo_remote::{RemoteActorRef, RemoteError, RemoteOutbound};
//! use kairo_serialization::{
//!     ActorRefWireData, MessageCodec, Registry, RemoteEnvelope, RemoteMessage,
//!     SerializationError, SerializationRegistry,
//! };
//!
//! #[derive(Debug, PartialEq, Eq)]
//! struct Ping {
//!     value: u8,
//! }
//!
//! impl RemoteMessage for Ping {
//!     const MANIFEST: &'static str = "kairo.example.Ping";
//!     const VERSION: u16 = 1;
//! }
//!
//! struct PingCodec;
//!
//! impl MessageCodec<Ping> for PingCodec {
//!     fn serializer_id(&self) -> u32 {
//!         1201
//!     }
//!
//!     fn encode(&self, message: &Ping) -> kairo_serialization::Result<Bytes> {
//!         Ok(Bytes::from(vec![message.value]))
//!     }
//!
//!     fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Ping> {
//!         if version != Ping::VERSION {
//!             return Err(SerializationError::Message(format!(
//!                 "unsupported Ping version {version}"
//!             )));
//!         }
//!         Ok(Ping { value: payload[0] })
//!     }
//! }
//!
//! #[derive(Default)]
//! struct RecordingOutbound {
//!     sent: Mutex<Vec<RemoteEnvelope>>,
//! }
//!
//! impl RemoteOutbound for RecordingOutbound {
//!     fn send(&self, envelope: RemoteEnvelope) -> kairo_remote::Result<()> {
//!         self.sent
//!             .lock()
//!             .map_err(|error| RemoteError::Outbound(error.to_string()))?
//!             .push(envelope);
//!         Ok(())
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut registry = Registry::new();
//! registry.register::<Ping, _>(PingCodec)?;
//! let registry = Arc::new(registry);
//! let outbound = Arc::new(RecordingOutbound::default());
//!
//! let recipient =
//!     ActorRefWireData::new("kairo://worker-system@127.0.0.1:25520/user/pinger#4")?;
//! let pinger = RemoteActorRef::<Ping>::new(
//!     recipient,
//!     registry,
//!     outbound.clone() as Arc<dyn RemoteOutbound>,
//! );
//!
//! pinger.tell(Ping { value: 7 })?;
//!
//! let sent = outbound.sent.lock().unwrap();
//! assert_eq!(sent.len(), 1);
//! assert_eq!(sent[0].message.serializer_id, 1201);
//! assert_eq!(sent[0].message.manifest.as_str(), Ping::MANIFEST);
//! assert_eq!(sent[0].message.version, Ping::VERSION);
//! assert_eq!(sent[0].message.payload, Bytes::from_static(&[7]));
//! # Ok(())
//! # }
//! ```
//!
//! Remote actor refs are location-transparent at the typed send boundary, but
//! they are not local actors. Local-only messages still need no serialization,
//! and `RemoteActorRefProvider::resolve` rejects local-only paths so callers do
//! not accidentally treat local refs as remoting endpoints. Remote death watch
//! follows Pekko's observable model of explicit watch/unwatch messages,
//! heartbeat acknowledgements per watched address, re-watch when a peer UID is
//! first learned or changes, and address termination when the watched address is
//! deemed unreachable.
//!
//! `ResolvedActorRef<M>` can wrap a local `ActorRef<M>` without making `M`
//! remote-capable. This keeps local inspection and local sends independent of
//! serialization:
//!
//! ```
//! use kairo_actor::{Actor, ActorResult, ActorSystem, Context, Props};
//! use kairo_remote::ResolvedActorRef;
//!
//! struct LocalOnly;
//! struct Sink;
//!
//! impl Actor for Sink {
//!     type Msg = LocalOnly;
//!
//!     fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
//!         Ok(())
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let system = ActorSystem::builder("local-only-doc").build()?;
//! let local = system.spawn("sink", Props::new(|| Sink))?;
//! let resolved = ResolvedActorRef::Local(local.clone());
//!
//! assert!(resolved.is_local());
//! assert_eq!(resolved.path(), local.path());
//! # Ok(())
//! # }
//! ```
//!
//! Remote resolution remains explicitly gated by `RemoteMessage` metadata:
//!
//! ```compile_fail
//! use std::sync::Arc;
//!
//! use kairo_remote::{RemoteActorRefProvider, RemoteOutbound, RemoteSettings};
//! use kairo_serialization::{Registry, RemoteEnvelope};
//!
//! struct LocalOnly;
//!
//! struct DropOutbound;
//!
//! impl RemoteOutbound for DropOutbound {
//!     fn send(&self, _envelope: RemoteEnvelope) -> kairo_remote::Result<()> {
//!         Ok(())
//!     }
//! }
//!
//! let provider = RemoteActorRefProvider::new(
//!     "local",
//!     RemoteSettings::new("127.0.0.1", 25520),
//!     Arc::new(Registry::new()),
//!     Arc::new(DropOutbound),
//! );
//!
//! let _ = provider.resolve::<LocalOnly>("kairo://remote@127.0.0.1:25521/user/sink");
//! ```
//!
//! [`RemoteInbound::with_diagnostics`] can attach a backend-neutral
//! [`RemoteInboundDiagnostics`] sink to record structured serialization and
//! delivery failures without choosing a logging or metrics dependency.
//! [`RemoteInboundDiagnosticFilter`] can wrap that observer when configuration
//! enables only a subset of remote inbound diagnostic categories.
//! [`ActorSystemRemoteInbound::with_diagnostics`] and
//! [`ActorSystemRemoteInbound::with_remote_settings_and_diagnostics`] carry the
//! same observer through actor-system inbound frame routing.
//! [`RemoteAssociation::with_diagnostics`] can attach the same style of
//! backend-neutral observer for quarantine transitions.

mod association;
mod association_cache;
mod association_inbound;
mod association_outbound;
mod association_pipeline;
mod association_registry;
mod association_routes;
mod codec;
mod error;
mod frame;
mod inbound;
mod inbound_router;
mod lanes;
mod local_address;
mod local_delivery;
mod outbound;
mod protocol;
mod provider;
mod reliable_delivery;
mod reliable_runtime;
mod remote_ref;
mod remote_watch;
mod remote_watch_actor;
mod remote_watch_effects;
mod remote_watch_inbound;
mod remote_watch_system_inbound;
mod resolved_ref;
mod settings;
mod stream;
mod stream_inbound;
mod stream_sink;
mod system_inbound;
mod tcp;
mod tcp_runtime;
mod transport;

pub use association::{
    AssociationState, RemoteAssociation, RemoteAssociationDiagnostic,
    RemoteAssociationDiagnosticFilter, RemoteAssociationDiagnostics,
};
pub use association_cache::{RemoteAssociationAddress, RemoteAssociationCache};
pub use association_inbound::AssociationRemoteInbound;
pub use association_outbound::AssociationRemoteOutbound;
pub use association_pipeline::AssociationOutboundPipeline;
pub use association_registry::{RemoteAssociationHandle, RemoteAssociationRegistry};
pub use association_routes::{RemoteAssociationRouteInstaller, RemoteAssociationRouteRegistration};
pub use codec::{
    ADDRESS_TERMINATED_SERIALIZER_ID, AddressTerminatedCodec, REMOTE_HEARTBEAT_ACK_SERIALIZER_ID,
    REMOTE_HEARTBEAT_SERIALIZER_ID, REMOTE_TERMINATED_SERIALIZER_ID, RemoteHeartbeatAckCodec,
    RemoteHeartbeatCodec, RemoteTerminatedCodec, UNWATCH_REMOTE_SERIALIZER_ID, UnwatchRemoteCodec,
    WATCH_REMOTE_SERIALIZER_ID, WatchRemoteCodec, register_remote_protocol_codecs,
};
pub use error::{RemoteError, Result};
pub use frame::{decode_remote_envelope_frame, encode_remote_envelope_frame};
pub use inbound::{
    InboundMessage, RemoteInbound, RemoteInboundDelivery, RemoteInboundDiagnostic,
    RemoteInboundDiagnosticFilter, RemoteInboundDiagnostics,
};
pub use inbound_router::{
    ManifestRemoteInboundRouter, RemoteEnvelopeHandler, RemoteInboundFrameRouter,
    is_remote_death_watch_manifest,
};
pub use kairo_actor::ActorPath;
pub use kairo_serialization::{RemoteEnvelope, SerializedMessage};
pub use lanes::{LaneRemoteOutbound, RemoteLaneClassifier, RemoteLaneSink, lane_send_failure};
pub use local_address::CanonicalLocalAddress;
pub use local_delivery::LocalActorInboundDelivery;
pub use outbound::{RemoteOutbound, RemoteOutboundRecipient};
pub use protocol::{
    AddressTerminated, RemoteHeartbeat, RemoteHeartbeatAck, RemoteTerminated, UnwatchRemote,
    WatchRemote,
};
pub use provider::{RemoteActorRefProvider, RemoteActorRefResolver};
pub use reliable_delivery::{
    RELIABLE_SYSTEM_ACK_SERIALIZER_ID, RELIABLE_SYSTEM_ENVELOPE_SERIALIZER_ID,
    RELIABLE_SYSTEM_NACK_SERIALIZER_ID, ReliableSystemAck, ReliableSystemAckCodec,
    ReliableSystemEnvelope, ReliableSystemEnvelopeCodec, ReliableSystemNack,
    ReliableSystemNackCodec, ReliableSystemReceiveOutcome, ReliableSystemReceiver,
    ReliableSystemSender, register_reliable_system_codecs,
};
pub use reliable_runtime::{
    ReliableSystemDeliveryFailure, ReliableSystemDeliveryObserver, ReliableSystemDeliverySettings,
    ReliableSystemDeliveryStats,
};
pub use remote_ref::RemoteActorRef;
pub use remote_watch::{RemoteDeathWatchEffect, RemoteDeathWatchState};
pub use remote_watch_actor::{
    RemoteDeathWatchActor, RemoteDeathWatchCommand, RemoteDeathWatchEffectSink,
    RemoteDeathWatchStats,
};
pub use remote_watch_effects::{
    IgnoreRemoteDeathWatchEffects, RemoteDeathWatchEffectObserver, RemoteDeathWatchOutboundSink,
    watcher_recipient_for_actor, watcher_recipient_for_address,
};
pub use remote_watch_inbound::RemoteDeathWatchProtocolDelivery;
pub use remote_watch_system_inbound::RemoteDeathWatchSystemInbound;
pub use resolved_ref::ResolvedActorRef;
pub use settings::RemoteSettings;
pub use stream::{
    RemoteStreamDecoder, RemoteStreamEncoder, RemoteStreamFrame, RemoteStreamId,
    decode_remote_stream_header, encode_remote_stream_frame, encode_remote_stream_header,
};
pub use stream_inbound::{RemoteFrameHandler, StreamFrameInbound};
pub use stream_sink::{
    QueuedRemoteByteSink, RemoteByteSink, RemoteOutboundQueueSettings, RemoteStreamWriter,
    StreamLaneSink, stream_send_failure,
};
pub use system_inbound::{ActorSystemRemoteInbound, ActorSystemRemoteInboundRegistry};
pub use tcp::{
    TcpAcceptedAssociation, TcpAssociationDialer, TcpAssociationFrameHandlerFactory,
    TcpAssociationHandshake, TcpAssociationIdentity, TcpAssociationListener,
    TcpAssociationListenerHandle, TcpAssociationListenerReport, TcpAssociationReadReport,
    TcpAssociationReaderFailure, TcpAssociationReaderHandle, TcpAssociationReaderRestartSettings,
    TcpAssociationReaderSupervisionDecision, TcpAssociationReaderSupervisor,
    TcpAssociationStreamReader, TcpAssociationSupervisedReadReport, TcpRemoteByteSink,
};
pub use tcp_runtime::{
    TcpRemoteActorRuntime, TcpRemoteActorRuntimeBuilder, TcpRemoteActorRuntimeContext,
    TcpRemoteActorSystem,
};
pub use transport::{FramedRemoteInbound, FramedRemoteOutbound, RemoteFrameSink};
