use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::Recipient;
use kairo_cluster::UniqueAddress;
use kairo_serialization::{ActorRefWireData, Registry, SerializationError};

use crate::{
    AggregationTarget, AggregationTargetRegistry, AggregationTransport, DeltaPropagationTarget,
    DeltaPropagationTargetRegistry, DeltaPropagationTransport, ReplicaId, ReplicatorClusterRoutes,
    ReplicatorGossipTarget, ReplicatorGossipTargetRegistry, ReplicatorGossipTransport,
    ReplicatorRemoteEnvelope, ReplicatorRemoteEnvelopeOutbound, ReplicatorRemoteTarget,
};

pub const DEFAULT_REPLICATOR_REMOTE_PATH: &str = "/system/ddata";

#[derive(Debug)]
pub enum ReplicatorRemoteTargetError {
    InvalidRecipientPath(String),
    MissingRemoteHost { node: ReplicaId },
    Serialization(SerializationError),
}

impl Display for ReplicatorRemoteTargetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRecipientPath(path) => {
                write!(
                    f,
                    "replicator remote target path `{path}` must start with `/`"
                )
            }
            Self::MissingRemoteHost { node } => {
                write!(
                    f,
                    "replicator remote target {} has no remote host",
                    node.as_str()
                )
            }
            Self::Serialization(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ReplicatorRemoteTargetError {}

impl From<SerializationError> for ReplicatorRemoteTargetError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRemoteTargetRegistrationReport {
    registered: Vec<ReplicaId>,
}

impl ReplicatorRemoteTargetRegistrationReport {
    pub fn registered(&self) -> &[ReplicaId] {
        &self.registered
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRemoteRouteRegistrationReport {
    delta_registered: Vec<ReplicaId>,
    aggregation_registered: Vec<ReplicaId>,
    gossip_registered: Vec<ReplicaId>,
}

impl ReplicatorRemoteRouteRegistrationReport {
    pub fn new(
        delta_registered: impl IntoIterator<Item = ReplicaId>,
        aggregation_registered: impl IntoIterator<Item = ReplicaId>,
        gossip_registered: impl IntoIterator<Item = ReplicaId>,
    ) -> Self {
        Self {
            delta_registered: delta_registered.into_iter().collect(),
            aggregation_registered: aggregation_registered.into_iter().collect(),
            gossip_registered: gossip_registered.into_iter().collect(),
        }
    }

    pub fn delta_registered(&self) -> &[ReplicaId] {
        &self.delta_registered
    }

    pub fn aggregation_registered(&self) -> &[ReplicaId] {
        &self.aggregation_registered
    }

    pub fn gossip_registered(&self) -> &[ReplicaId] {
        &self.gossip_registered
    }
}

#[derive(Clone)]
pub struct ReplicatorRemoteRouteTargets {
    recipient_path: String,
    sender: Option<ActorRefWireData>,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<ReplicatorRemoteEnvelope> + Send + Sync>,
}

impl ReplicatorRemoteRouteTargets {
    pub fn new(
        registry: Arc<Registry>,
        outbound: impl Recipient<ReplicatorRemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::from_arc(registry, Arc::new(outbound))
    }

    pub fn from_arc(
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<ReplicatorRemoteEnvelope> + Send + Sync>,
    ) -> Self {
        Self {
            recipient_path: DEFAULT_REPLICATOR_REMOTE_PATH.to_string(),
            sender: None,
            registry,
            outbound,
        }
    }

    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    pub fn with_sender(mut self, sender: Option<ActorRefWireData>) -> Self {
        self.sender = sender;
        self
    }

    pub fn target_for_node(
        &self,
        node: &UniqueAddress,
    ) -> Result<ReplicatorRemoteTarget, ReplicatorRemoteTargetError> {
        if !self.recipient_path.starts_with('/') {
            return Err(ReplicatorRemoteTargetError::InvalidRecipientPath(
                self.recipient_path.clone(),
            ));
        }

        let replica = ReplicaId::from(node);
        if node.address.host().is_none() {
            return Err(ReplicatorRemoteTargetError::MissingRemoteHost {
                node: replica.clone(),
            });
        }

        let recipient = ActorRefWireData::new(format!("{}{}", node.address, self.recipient_path))?;
        Ok(ReplicatorRemoteTarget::new(replica, recipient))
    }

    pub fn set_delta_targets<Codec>(
        &self,
        routes: &ReplicatorClusterRoutes,
        transport: &mut DeltaPropagationTransport<Codec>,
    ) -> Result<ReplicatorRemoteTargetRegistrationReport, ReplicatorRemoteTargetError> {
        self.set_delta_target_registry(routes, &transport.target_registry())
    }

    pub fn set_delta_target_registry(
        &self,
        routes: &ReplicatorClusterRoutes,
        registry: &DeltaPropagationTargetRegistry,
    ) -> Result<ReplicatorRemoteTargetRegistrationReport, ReplicatorRemoteTargetError> {
        let targets = routes
            .remote_nodes()
            .iter()
            .map(|node| self.delta_target_for_node(node))
            .collect::<Result<Vec<_>, _>>()?;
        let registered = targets
            .iter()
            .map(|target| target.replica().clone())
            .collect();
        registry.set_targets(targets);
        Ok(ReplicatorRemoteTargetRegistrationReport { registered })
    }

    pub fn set_aggregation_targets<Codec>(
        &self,
        routes: &ReplicatorClusterRoutes,
        transport: &mut AggregationTransport<Codec>,
    ) -> Result<ReplicatorRemoteTargetRegistrationReport, ReplicatorRemoteTargetError> {
        self.set_aggregation_target_registry(routes, &transport.target_registry())
    }

    pub fn set_aggregation_target_registry(
        &self,
        routes: &ReplicatorClusterRoutes,
        registry: &AggregationTargetRegistry,
    ) -> Result<ReplicatorRemoteTargetRegistrationReport, ReplicatorRemoteTargetError> {
        let targets = routes
            .remote_nodes()
            .iter()
            .map(|node| self.aggregation_target_for_node(node))
            .collect::<Result<Vec<_>, _>>()?;
        let registered = targets
            .iter()
            .map(|target| target.replica().clone())
            .collect();
        registry.set_targets(targets);
        Ok(ReplicatorRemoteTargetRegistrationReport { registered })
    }

    pub fn set_gossip_targets(
        &self,
        routes: &ReplicatorClusterRoutes,
        transport: &mut ReplicatorGossipTransport,
    ) -> Result<ReplicatorRemoteTargetRegistrationReport, ReplicatorRemoteTargetError> {
        self.set_gossip_target_registry(routes, &transport.target_registry())
    }

    pub fn set_gossip_target_registry(
        &self,
        routes: &ReplicatorClusterRoutes,
        registry: &ReplicatorGossipTargetRegistry,
    ) -> Result<ReplicatorRemoteTargetRegistrationReport, ReplicatorRemoteTargetError> {
        let targets = routes
            .remote_nodes()
            .iter()
            .map(|node| self.gossip_target_for_node(node))
            .collect::<Result<Vec<_>, _>>()?;
        let registered = targets
            .iter()
            .map(|target| target.replica().clone())
            .collect();
        registry.set_targets(targets);
        Ok(ReplicatorRemoteTargetRegistrationReport { registered })
    }

    fn delta_target_for_node(
        &self,
        node: &UniqueAddress,
    ) -> Result<DeltaPropagationTarget, ReplicatorRemoteTargetError> {
        let target = self.target_for_node(node)?;
        Ok(DeltaPropagationTarget::new(
            target.replica().clone(),
            self.outbound_for(target),
        ))
    }

    fn aggregation_target_for_node(
        &self,
        node: &UniqueAddress,
    ) -> Result<AggregationTarget, ReplicatorRemoteTargetError> {
        let target = self.target_for_node(node)?;
        Ok(AggregationTarget::remote_envelope(
            target.replica().clone(),
            self.outbound_for(target.clone()),
            self.outbound_for(target),
        ))
    }

    fn gossip_target_for_node(
        &self,
        node: &UniqueAddress,
    ) -> Result<ReplicatorGossipTarget, ReplicatorRemoteTargetError> {
        let target = self.target_for_node(node)?;
        Ok(ReplicatorGossipTarget::new(
            target.replica().clone(),
            self.outbound_for(target.clone()),
            self.outbound_for(target),
        ))
    }

    fn outbound_for(&self, target: ReplicatorRemoteTarget) -> ReplicatorRemoteEnvelopeOutbound {
        ReplicatorRemoteEnvelopeOutbound::from_arc(
            target,
            self.sender.clone(),
            Arc::clone(&self.registry),
            Arc::clone(&self.outbound),
        )
    }
}

#[cfg(test)]
mod tests;
