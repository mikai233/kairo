use std::marker::PhantomData;

use bytes::Bytes;
use kairo_cluster::UniqueAddress;
use kairo_remote::{RemoteError, RemoteFrameHandler, RemoteStreamId, decode_remote_envelope_frame};
use kairo_serialization::{ActorRefWireData, RemoteEnvelope, RemoteMessage};

use crate::{
    CLUSTER_TOOLS_SYSTEM_MANIFESTS, DEFAULT_PUBSUB_REMOTE_PATH, PubSubDelta,
    PubSubGossipWireInbound, PubSubPathEnvelope, PubSubPublishEnvelope,
    PubSubRemoteDeliveryInbound, PubSubStatus, SingletonHandOverDone, SingletonHandOverInProgress,
    SingletonHandOverToMe, SingletonManagerRemoteInbound, SingletonTakeOverFromMe,
};

use super::ClusterToolsSystemInboundError;

/// Shared control-plane ingress for cluster-tools remote protocols.
///
/// The router classifies stable manifests before invoking an optional typed
/// adapter. Gossip, pubsub business delivery, and singleton handover remain
/// separate handlers so applications can compose only the facilities they
/// enable. Recognized traffic without a handler fails explicitly.
#[derive(Clone)]
pub struct ClusterToolsSystemInbound<M>
where
    M: Send + 'static,
{
    self_node: UniqueAddress,
    pubsub_gossip: Option<PubSubGossipWireInbound>,
    pubsub_delivery: Option<PubSubRemoteDeliveryInbound<M>>,
    singleton_manager: Option<SingletonManagerRemoteInbound>,
    _message: PhantomData<fn(M)>,
}

impl<M> ClusterToolsSystemInbound<M>
where
    M: Send + 'static,
{
    /// Creates an ingress with no protocol handlers installed.
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            self_node,
            pubsub_gossip: None,
            pubsub_delivery: None,
            singleton_manager: None,
            _message: PhantomData,
        }
    }

    /// Installs the pubsub status/delta gossip adapter.
    pub fn with_pubsub_gossip(mut self, inbound: PubSubGossipWireInbound) -> Self {
        self.pubsub_gossip = Some(inbound);
        self
    }

    /// Installs the typed pubsub publish/path delivery adapter.
    pub fn with_pubsub_delivery(mut self, inbound: PubSubRemoteDeliveryInbound<M>) -> Self {
        self.pubsub_delivery = Some(inbound);
        self
    }

    /// Installs the singleton handover adapter.
    pub fn with_singleton_manager(mut self, inbound: SingletonManagerRemoteInbound) -> Self {
        self.singleton_manager = Some(inbound);
        self
    }

    /// Validates and dispatches one decoded remote envelope by stable manifest.
    ///
    /// Recipient validation is delegated to the selected protocol adapter,
    /// except gossip messages whose already-demultiplexed entry point requires
    /// this router to validate the canonical pubsub path first.
    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), ClusterToolsSystemInboundError>
    where
        M: RemoteMessage,
    {
        match envelope.message.manifest.as_str() {
            PubSubStatus::MANIFEST | PubSubDelta::MANIFEST => {
                validate_recipient(
                    &self.self_node,
                    DEFAULT_PUBSUB_REMOTE_PATH,
                    &envelope.recipient,
                )?;
                self.pubsub_gossip
                    .as_ref()
                    .ok_or(ClusterToolsSystemInboundError::MissingHandler(
                        "pubsub gossip",
                    ))?
                    .receive_message(envelope.message)?;
                Ok(())
            }
            PubSubPublishEnvelope::MANIFEST | PubSubPathEnvelope::MANIFEST => {
                self.pubsub_delivery
                    .as_ref()
                    .ok_or(ClusterToolsSystemInboundError::MissingHandler(
                        "pubsub delivery",
                    ))?
                    .receive(envelope)?;
                Ok(())
            }
            SingletonHandOverToMe::MANIFEST
            | SingletonHandOverInProgress::MANIFEST
            | SingletonHandOverDone::MANIFEST
            | SingletonTakeOverFromMe::MANIFEST => {
                self.singleton_manager
                    .as_ref()
                    .ok_or(ClusterToolsSystemInboundError::MissingHandler(
                        "singleton manager",
                    ))?
                    .receive(envelope)?;
                Ok(())
            }
            manifest => Err(ClusterToolsSystemInboundError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }
}

impl<M> RemoteFrameHandler for ClusterToolsSystemInbound<M>
where
    M: RemoteMessage + Send + 'static,
{
    fn handle_frame(&self, _stream_id: RemoteStreamId, frame: Bytes) -> kairo_remote::Result<()> {
        self.receive(decode_remote_envelope_frame(frame)?)
            .map_err(|error| RemoteError::Inbound(error.to_string()))
    }
}

/// Returns whether `manifest` belongs to the shared control-plane protocol.
pub fn is_cluster_tools_system_manifest(manifest: &str) -> bool {
    CLUSTER_TOOLS_SYSTEM_MANIFESTS.contains(&manifest)
}

fn validate_recipient(
    node: &UniqueAddress,
    recipient_path: &str,
    actual: &ActorRefWireData,
) -> Result<(), ClusterToolsSystemInboundError> {
    let expected = recipient_for_node(node, recipient_path)?;
    if actual != &expected {
        return Err(ClusterToolsSystemInboundError::WrongRecipient {
            expected: expected.path().to_string(),
            actual: actual.path().to_string(),
        });
    }
    Ok(())
}

fn recipient_for_node(
    node: &UniqueAddress,
    recipient_path: &str,
) -> Result<ActorRefWireData, ClusterToolsSystemInboundError> {
    if !recipient_path.starts_with('/') {
        return Err(ClusterToolsSystemInboundError::InvalidRecipientPath(
            recipient_path.to_string(),
        ));
    }
    if node.address.host().is_none() {
        return Err(ClusterToolsSystemInboundError::MissingRemoteHost {
            node: node.ordering_key(),
        });
    }
    Ok(ActorRefWireData::new(format!(
        "{}{}",
        node.address, recipient_path
    ))?)
}
