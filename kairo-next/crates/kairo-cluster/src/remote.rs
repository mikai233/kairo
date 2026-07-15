#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_remote::RemoteOutbound;
use kairo_serialization::{ActorRefWireData, RemoteEnvelope, SerializationError};

use crate::{ClusterSerializedMembership, UniqueAddress};

/// Default remote actor path for the cluster membership daemon.
pub const DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH: &str = "/system/cluster/core/daemon";

#[derive(Debug)]
/// Failure while adapting serialized membership traffic to a remote envelope.
pub enum ClusterMembershipRemoteEnvelopeError {
    /// The configured recipient path was not an absolute actor path.
    InvalidRecipientPath(String),
    /// The target node has a local-only address and cannot be reached remotely.
    MissingRemoteHost {
        /// Stable display key for the rejected node incarnation.
        node: String,
    },
    /// The recipient actor reference could not be represented on the wire.
    Serialization(SerializationError),
    /// The remoting layer rejected the completed envelope.
    Send {
        /// Stable display key for the target node incarnation.
        node: String,
        /// Error reported by the remoting layer.
        reason: String,
    },
}

impl Display for ClusterMembershipRemoteEnvelopeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRecipientPath(path) => {
                write!(f, "cluster remote target path `{path}` must start with `/`")
            }
            Self::MissingRemoteHost { node } => {
                write!(f, "cluster remote target {node} has no remote host")
            }
            Self::Serialization(error) => write!(f, "{error}"),
            Self::Send { node, reason } => {
                write!(f, "cluster remote envelope send to {node} failed: {reason}")
            }
        }
    }
}

impl std::error::Error for ClusterMembershipRemoteEnvelopeError {}

impl From<SerializationError> for ClusterMembershipRemoteEnvelopeError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
/// Converts serialized membership messages into remoting envelopes.
///
/// The destination actor path is resolved under the target node's canonical
/// address. The target must therefore include a host; local-only addresses are
/// rejected before the remoting layer is called.
pub struct ClusterMembershipRemoteEnvelopeOutbound {
    recipient_path: String,
    sender: Option<ActorRefWireData>,
    outbound: Arc<dyn RemoteOutbound>,
}

impl ClusterMembershipRemoteEnvelopeOutbound {
    /// Creates an adapter backed by an owned remoting outbound.
    pub fn new(outbound: impl RemoteOutbound + 'static) -> Self {
        Self::from_arc(Arc::new(outbound))
    }

    /// Creates an adapter backed by a shared remoting outbound.
    pub fn from_arc(outbound: Arc<dyn RemoteOutbound>) -> Self {
        Self {
            recipient_path: DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH.to_string(),
            sender: None,
            outbound,
        }
    }

    /// Overrides the absolute recipient path appended to each target address.
    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    /// Sets the optional actor identity recorded as the remote sender.
    pub fn with_sender(mut self, sender: Option<ActorRefWireData>) -> Self {
        self.sender = sender;
        self
    }

    /// Resolves the membership daemon recipient for `node`.
    ///
    /// This validates both the configured absolute path and the presence of a
    /// remote host without sending an envelope.
    pub fn recipient_for_node(
        &self,
        node: &UniqueAddress,
    ) -> Result<ActorRefWireData, ClusterMembershipRemoteEnvelopeError> {
        if !self.recipient_path.starts_with('/') {
            return Err(ClusterMembershipRemoteEnvelopeError::InvalidRecipientPath(
                self.recipient_path.clone(),
            ));
        }

        if node.address.host().is_none() {
            return Err(ClusterMembershipRemoteEnvelopeError::MissingRemoteHost {
                node: node.ordering_key(),
            });
        }

        Ok(ActorRefWireData::new(format!(
            "{}{}",
            node.address, self.recipient_path
        ))?)
    }

    /// Wraps and sends one already serialized membership message.
    pub fn send_serialized(
        &self,
        membership: ClusterSerializedMembership,
    ) -> Result<(), ClusterMembershipRemoteEnvelopeError> {
        let target = membership.target.clone();
        let recipient = self.recipient_for_node(&target)?;
        let envelope = RemoteEnvelope::new(recipient, self.sender.clone(), membership.message);
        self.outbound
            .send(envelope)
            .map_err(|error| ClusterMembershipRemoteEnvelopeError::Send {
                node: target.ordering_key(),
                reason: error.to_string(),
            })
    }
}

impl Recipient<ClusterSerializedMembership> for ClusterMembershipRemoteEnvelopeOutbound {
    fn tell(
        &self,
        message: ClusterSerializedMembership,
    ) -> Result<(), SendError<ClusterSerializedMembership>> {
        let rejected = message.clone();
        self.send_serialized(message)
            .map_err(|error| SendError::new(rejected, error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use kairo_actor::{Address, Recipient};
    use kairo_remote::{RemoteAssociationAddress, RemoteAssociationCache, Result};
    use kairo_serialization::{Manifest, RemoteEnvelope, RemoteMessage, SerializedMessage};

    use super::*;
    use crate::{JOIN_SERIALIZER_ID, Join, register_cluster_protocol_codecs};

    #[derive(Default)]
    struct CollectingRemoteOutbound {
        sent: Mutex<Vec<RemoteEnvelope>>,
    }

    impl CollectingRemoteOutbound {
        fn sent(&self) -> Vec<RemoteEnvelope> {
            self.sent
                .lock()
                .expect("collecting remote outbound poisoned")
                .clone()
        }
    }

    impl kairo_remote::RemoteOutbound for CollectingRemoteOutbound {
        fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
            self.sent
                .lock()
                .expect("collecting remote outbound poisoned")
                .push(envelope);
            Ok(())
        }
    }

    fn node(name: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new(
                "kairo",
                "cluster",
                Some(format!("{name}.example.test")),
                Some(2552),
            ),
            uid,
        )
    }

    fn serialized_for(target: UniqueAddress, value: u8) -> ClusterSerializedMembership {
        ClusterSerializedMembership::new(
            target,
            SerializedMessage::new(
                JOIN_SERIALIZER_ID,
                Manifest::new(Join::MANIFEST),
                Join::VERSION,
                Bytes::from(vec![value]),
            ),
        )
    }

    #[test]
    fn remote_envelope_outbound_wraps_serialized_membership_for_cluster_core() {
        let collecting = Arc::new(CollectingRemoteOutbound::default());
        let outbound = ClusterMembershipRemoteEnvelopeOutbound::from_arc(
            collecting.clone() as Arc<dyn kairo_remote::RemoteOutbound>
        );
        let target = node("seed", 7);

        outbound.tell(serialized_for(target, 1)).unwrap();

        let sent = collecting.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].recipient.path(),
            "kairo://cluster@seed.example.test:2552/system/cluster/core/daemon"
        );
        assert_eq!(sent[0].message.payload, Bytes::from_static(&[1]));
    }

    #[test]
    fn remote_envelope_outbound_rejects_local_only_target() {
        let outbound =
            ClusterMembershipRemoteEnvelopeOutbound::new(CollectingRemoteOutbound::default());
        let local = UniqueAddress::new(Address::local("cluster"), 1);
        let message = serialized_for(local, 2);

        let error = outbound
            .tell(message)
            .expect_err("local-only target should be rejected");

        assert!(error.reason().contains("has no remote host"));
        assert_eq!(error.into_message().target.address.host(), None);
    }

    #[test]
    fn remote_envelope_outbound_can_use_association_cache() {
        let cache = RemoteAssociationCache::new();
        let collecting = Arc::new(CollectingRemoteOutbound::default());
        cache.insert_route(
            RemoteAssociationAddress::new("kairo", "cluster", "seed.example.test", Some(2552))
                .unwrap(),
            collecting.clone() as Arc<dyn kairo_remote::RemoteOutbound>,
        );
        let outbound = ClusterMembershipRemoteEnvelopeOutbound::new(cache);

        outbound.tell(serialized_for(node("seed", 9), 3)).unwrap();

        let sent = collecting.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].recipient.path(),
            "kairo://cluster@seed.example.test:2552/system/cluster/core/daemon"
        );
        assert_eq!(sent[0].message.manifest.as_str(), "kairo.cluster.join");
    }

    #[test]
    fn membership_wire_outbound_can_route_through_remote_envelopes() {
        let mut registry = kairo_serialization::Registry::new();
        register_cluster_protocol_codecs(&mut registry).unwrap();
        let registry = Arc::new(registry);
        let collecting = Arc::new(CollectingRemoteOutbound::default());
        let remote = ClusterMembershipRemoteEnvelopeOutbound::from_arc(
            collecting.clone() as Arc<dyn kairo_remote::RemoteOutbound>
        );
        let target = node("seed", 11);
        let outbound = crate::ClusterMembershipWireOutbound::new(target.clone(), registry, remote);

        outbound
            .send_membership(crate::ClusterMembershipMsg::Join {
                join: Join {
                    node: node("joining", 12),
                    roles: vec!["backend".to_string()],
                },
                reply_to: None,
            })
            .unwrap();

        let sent = collecting.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].recipient.path(),
            "kairo://cluster@seed.example.test:2552/system/cluster/core/daemon"
        );
        assert_eq!(sent[0].message.serializer_id, JOIN_SERIALIZER_ID);
    }
}
