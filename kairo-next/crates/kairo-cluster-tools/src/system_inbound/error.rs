use std::fmt::{self, Display, Formatter};

use crate::{PubSubGossipWireError, PubSubRemoteDeliveryError, SingletonManagerRemoteError};

#[derive(Debug)]
pub enum ClusterToolsSystemInboundError {
    InvalidRecipientPath(String),
    MissingRemoteHost { node: String },
    MissingHandler(&'static str),
    PubSubDelivery(PubSubRemoteDeliveryError),
    PubSubGossip(PubSubGossipWireError),
    Serialization(kairo_serialization::SerializationError),
    SingletonManager(SingletonManagerRemoteError),
    UnsupportedManifest(String),
    WrongRecipient { expected: String, actual: String },
}

impl Display for ClusterToolsSystemInboundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRecipientPath(path) => {
                write!(
                    f,
                    "cluster-tools recipient path `{path}` must start with `/`"
                )
            }
            Self::MissingRemoteHost { node } => {
                write!(f, "cluster-tools self node {node} has no remote host")
            }
            Self::MissingHandler(handler) => {
                write!(f, "cluster-tools system inbound has no {handler} handler")
            }
            Self::PubSubDelivery(error) => write!(f, "{error}"),
            Self::PubSubGossip(error) => write!(f, "{error}"),
            Self::Serialization(error) => write!(f, "{error}"),
            Self::SingletonManager(error) => write!(f, "{error}"),
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported cluster-tools system manifest `{manifest}`")
            }
            Self::WrongRecipient { expected, actual } => {
                write!(
                    f,
                    "cluster-tools system envelope addressed to {actual}, expected {expected}"
                )
            }
        }
    }
}

impl std::error::Error for ClusterToolsSystemInboundError {}

impl From<PubSubRemoteDeliveryError> for ClusterToolsSystemInboundError {
    fn from(error: PubSubRemoteDeliveryError) -> Self {
        Self::PubSubDelivery(error)
    }
}

impl From<PubSubGossipWireError> for ClusterToolsSystemInboundError {
    fn from(error: PubSubGossipWireError) -> Self {
        Self::PubSubGossip(error)
    }
}

impl From<kairo_serialization::SerializationError> for ClusterToolsSystemInboundError {
    fn from(error: kairo_serialization::SerializationError) -> Self {
        Self::Serialization(error)
    }
}

impl From<SingletonManagerRemoteError> for ClusterToolsSystemInboundError {
    fn from(error: SingletonManagerRemoteError) -> Self {
        Self::SingletonManager(error)
    }
}
