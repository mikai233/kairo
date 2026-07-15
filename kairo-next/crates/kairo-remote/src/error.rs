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
    OutboundLaneQueueFull { lane: String, capacity: usize },
    /// A lane writer has closed and rejects further frames.
    #[error("remote {lane} lane writer is closed: {reason}")]
    OutboundLaneClosed { lane: String, reason: String },
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
    /// The association is closed and cannot accept more outbound traffic.
    #[error("remote association with `{remote}` is closed: {reason}")]
    AssociationClosed { remote: String, reason: String },
    /// The association is quarantined and cannot accept more outbound traffic.
    #[error("remote association with `{remote}` is quarantined: {reason}")]
    AssociationQuarantined { remote: String, reason: String },
    /// No outbound association route is installed for the remote address.
    #[error("no remote association route for `{remote}`")]
    AssociationUnavailable { remote: String },
    /// A remote UID is already bound to a different association address.
    #[error(
        "remote association UID collision for `{uid}`: existing `{existing}`, attempted `{attempted}`"
    )]
    AssociationUidCollision {
        uid: u64,
        existing: String,
        attempted: String,
    },
}
