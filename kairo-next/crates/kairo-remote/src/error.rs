#![deny(missing_docs)]

use std::time::Duration;

use kairo_serialization::SerializationError;

/// Result type used by remote APIs.
pub type Result<T> = std::result::Result<T, RemoteError>;

/// Errors returned by remote refs, inbound routing, associations, and transports.
#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    /// Stable serialization metadata or codec failure.
    #[error(transparent)]
    Serialization(#[from] SerializationError),
    /// The supplied actor-ref path names the local system rather than a remote
    /// address.
    #[error("actor ref `{0}` does not name a remote address")]
    LocalAddress(String),
    /// The supplied remote actor-ref path or address was malformed.
    #[error("remote actor ref `{0}` is invalid: {1}")]
    InvalidRemoteRef(String, String),
    /// Outbound delivery failed before the frame was accepted by the remote
    /// association.
    #[error("remote outbound delivery failed: {0}")]
    Outbound(String),
    /// A bounded association lane could not accept another frame immediately.
    #[error("remote {lane} lane queue is full at capacity {capacity}")]
    OutboundLaneQueueFull {
        /// Lowercase transport lane name.
        lane: String,
        /// Configured bounded queue capacity.
        capacity: usize,
    },
    /// A lane writer has closed and rejects further frames.
    #[error("remote {lane} lane writer is closed: {reason}")]
    OutboundLaneClosed {
        /// Lowercase transport lane name.
        lane: String,
        /// Diagnostic reason the writer closed.
        reason: String,
    },
    /// The bounded reliable-system retention buffer is full.
    #[error("reliable system delivery buffer is full at capacity {capacity}")]
    ReliableSystemBufferFull {
        /// Configured maximum number of retained system messages.
        capacity: usize,
    },
    /// Reliable delivery metadata did not match the active association state.
    #[error("invalid reliable system delivery transition: {0}")]
    InvalidReliableSystemDelivery(String),
    /// Inbound delivery failed after a frame was decoded.
    #[error("remote inbound delivery failed: {0}")]
    Inbound(String),
    /// A protocol manifest was registered more than once before remoting bind.
    #[error("remote protocol manifest `{0}` is already registered")]
    DuplicateProtocolManifest(String),
    /// An inbound envelope named no protocol registered by this runtime.
    #[error("remote protocol manifest `{0}` is not registered")]
    UnknownProtocolManifest(String),
    /// A transport frame or stream payload did not match Kairo's remote wire
    /// format.
    #[error("invalid remote frame: {0}")]
    InvalidFrame(String),
    /// TCP handshake resource settings were zero or otherwise unusable.
    #[error("invalid tcp handshake settings: {0}")]
    InvalidTcpHandshakeSettings(String),
    /// TCP lane-assembly resource settings were zero or otherwise unusable.
    #[error("invalid tcp association assembly settings: {0}")]
    InvalidTcpAssociationAssemblySettings(String),
    /// TCP route reconnect settings were zero or otherwise unusable.
    #[error("invalid tcp reconnect settings: {0}")]
    InvalidTcpReconnectSettings(String),
    /// A TCP remoting runtime could not stop its owned workers within the supplied budget.
    #[error("remote tcp shutdown timed out after {timeout:?}")]
    ShutdownTimeout {
        /// Total shutdown budget supplied by the caller.
        timeout: Duration,
    },
    /// The association is closed and cannot accept more outbound traffic.
    #[error("remote association with `{remote}` is closed: {reason}")]
    AssociationClosed {
        /// Canonical remote actor-system address.
        remote: String,
        /// First terminal close reason.
        reason: String,
    },
    /// The association is quarantined and cannot accept more outbound traffic.
    #[error("remote association with `{remote}` is quarantined: {reason}")]
    AssociationQuarantined {
        /// Canonical remote actor-system address.
        remote: String,
        /// Quarantine reason.
        reason: String,
    },
    /// No outbound association route is installed for the remote address.
    #[error("no remote association route for `{remote}`")]
    AssociationUnavailable {
        /// Canonical remote actor-system address.
        remote: String,
    },
    /// A remote UID is already bound to a different association address.
    #[error(
        "remote association UID collision for `{uid}`: existing `{existing}`, attempted `{attempted}`"
    )]
    AssociationUidCollision {
        /// Remote actor-system incarnation claimed by both addresses.
        uid: u64,
        /// Canonical address already indexed for the UID.
        existing: String,
        /// Canonical address attempting to claim the UID.
        attempted: String,
    },
}
