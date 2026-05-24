use kairo_serialization::SerializationError;

pub type Result<T> = std::result::Result<T, RemoteError>;

#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    #[error(transparent)]
    Serialization(#[from] SerializationError),
    #[error("actor ref `{0}` does not name a remote address")]
    LocalAddress(String),
    #[error("remote actor ref `{0}` is invalid: {1}")]
    InvalidRemoteRef(String, String),
    #[error("remote outbound delivery failed: {0}")]
    Outbound(String),
    #[error("remote inbound delivery failed: {0}")]
    Inbound(String),
    #[error("invalid remote frame: {0}")]
    InvalidFrame(String),
    #[error("remote association with `{remote}` is closed: {reason}")]
    AssociationClosed { remote: String, reason: String },
    #[error("remote association with `{remote}` is quarantined: {reason}")]
    AssociationQuarantined { remote: String, reason: String },
}
