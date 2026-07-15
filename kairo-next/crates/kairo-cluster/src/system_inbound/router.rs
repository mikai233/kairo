use bytes::Bytes;
use kairo_remote::{RemoteError, RemoteFrameHandler, RemoteStreamId, decode_remote_envelope_frame};
use kairo_serialization::{ActorRefWireData, RemoteEnvelope, RemoteMessage};

use crate::{
    ClusterGossipWireInbound, ClusterMembershipWireInbound, ClusterSeedJoinWireInbound,
    DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH, DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH,
    DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH, Down, ExitingConfirmed, GossipEnvelope, GossipStatus,
    Heartbeat, HeartbeatRemoteReceiverInbound, HeartbeatRemoteResponseInbound, HeartbeatRsp,
    InitJoin, InitJoinAck, InitJoinNack, Join, Leave, UniqueAddress, Welcome,
};

use super::ClusterSystemInboundError;

#[derive(Clone)]
pub struct ClusterSystemInbound {
    self_node: UniqueAddress,
    gossip: Option<ClusterGossipWireInbound>,
    membership: Option<ClusterMembershipWireInbound>,
    heartbeat_receiver: Option<HeartbeatRemoteReceiverInbound>,
    heartbeat_response: Option<HeartbeatRemoteResponseInbound>,
    seed_join: Option<ClusterSeedJoinWireInbound>,
}

impl ClusterSystemInbound {
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            self_node,
            gossip: None,
            membership: None,
            heartbeat_receiver: None,
            heartbeat_response: None,
            seed_join: None,
        }
    }

    pub fn with_membership(mut self, inbound: ClusterMembershipWireInbound) -> Self {
        self.membership = Some(inbound);
        self
    }

    pub fn with_gossip(mut self, inbound: ClusterGossipWireInbound) -> Self {
        self.gossip = Some(inbound);
        self
    }

    pub fn with_heartbeat_receiver(mut self, inbound: HeartbeatRemoteReceiverInbound) -> Self {
        self.heartbeat_receiver = Some(inbound);
        self
    }

    pub fn with_heartbeat_response(mut self, inbound: HeartbeatRemoteResponseInbound) -> Self {
        self.heartbeat_response = Some(inbound);
        self
    }

    pub fn with_seed_join(mut self, inbound: ClusterSeedJoinWireInbound) -> Self {
        self.seed_join = Some(inbound);
        self
    }

    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), ClusterSystemInboundError> {
        match envelope.message.manifest.as_str() {
            InitJoin::MANIFEST | InitJoinAck::MANIFEST | InitJoinNack::MANIFEST => {
                validate_recipient(
                    &self.self_node,
                    DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH,
                    &envelope.recipient,
                )?;
                self.seed_join
                    .as_ref()
                    .ok_or(ClusterSystemInboundError::MissingHandler("seed join"))?
                    .receive(envelope)?;
                Ok(())
            }
            Join::MANIFEST
            | Welcome::MANIFEST
            | GossipEnvelope::MANIFEST
            | Leave::MANIFEST
            | Down::MANIFEST
            | ExitingConfirmed::MANIFEST => {
                validate_recipient(
                    &self.self_node,
                    DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH,
                    &envelope.recipient,
                )?;
                self.membership
                    .as_ref()
                    .ok_or(ClusterSystemInboundError::MissingHandler("membership"))?
                    .receive_message(envelope.message)?;
                Ok(())
            }
            GossipStatus::MANIFEST => {
                validate_recipient(
                    &self.self_node,
                    DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH,
                    &envelope.recipient,
                )?;
                self.gossip
                    .as_ref()
                    .ok_or(ClusterSystemInboundError::MissingHandler("gossip"))?
                    .receive_message(envelope.message)?;
                Ok(())
            }
            Heartbeat::MANIFEST => {
                validate_recipient(
                    &self.self_node,
                    DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
                    &envelope.recipient,
                )?;
                self.heartbeat_receiver
                    .as_ref()
                    .ok_or(ClusterSystemInboundError::MissingHandler(
                        "heartbeat receiver",
                    ))?
                    .receive(envelope)?;
                Ok(())
            }
            HeartbeatRsp::MANIFEST => {
                validate_recipient(
                    &self.self_node,
                    DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH,
                    &envelope.recipient,
                )?;
                self.heartbeat_response
                    .as_ref()
                    .ok_or(ClusterSystemInboundError::MissingHandler(
                        "heartbeat response",
                    ))?
                    .receive(envelope)?;
                Ok(())
            }
            manifest => Err(ClusterSystemInboundError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }
}

impl RemoteFrameHandler for ClusterSystemInbound {
    fn handle_frame(&self, _stream_id: RemoteStreamId, frame: Bytes) -> kairo_remote::Result<()> {
        self.receive(decode_remote_envelope_frame(frame)?)
            .map_err(|error| RemoteError::Inbound(error.to_string()))
    }
}

pub fn is_cluster_system_manifest(manifest: &str) -> bool {
    matches!(
        manifest,
        Join::MANIFEST
            | InitJoin::MANIFEST
            | InitJoinAck::MANIFEST
            | InitJoinNack::MANIFEST
            | Welcome::MANIFEST
            | GossipEnvelope::MANIFEST
            | GossipStatus::MANIFEST
            | Heartbeat::MANIFEST
            | HeartbeatRsp::MANIFEST
            | Leave::MANIFEST
            | Down::MANIFEST
            | ExitingConfirmed::MANIFEST
    )
}

fn validate_recipient(
    node: &UniqueAddress,
    recipient_path: &str,
    actual: &ActorRefWireData,
) -> Result<(), ClusterSystemInboundError> {
    let expected = recipient_for_node(node, recipient_path)?;
    if actual != &expected {
        return Err(ClusterSystemInboundError::WrongRecipient {
            expected: expected.path().to_string(),
            actual: actual.path().to_string(),
        });
    }
    Ok(())
}

fn recipient_for_node(
    node: &UniqueAddress,
    recipient_path: &str,
) -> Result<ActorRefWireData, ClusterSystemInboundError> {
    if !recipient_path.starts_with('/') {
        return Err(ClusterSystemInboundError::InvalidRecipientPath(
            recipient_path.to_string(),
        ));
    }
    if node.address.host().is_none() {
        return Err(ClusterSystemInboundError::MissingRemoteHost {
            node: node.ordering_key(),
        });
    }
    Ok(ActorRefWireData::new(format!(
        "{}{}",
        node.address, recipient_path
    ))?)
}
