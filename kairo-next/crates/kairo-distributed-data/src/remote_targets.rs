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
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    use kairo_actor::{Address, Recipient, SendError};
    use kairo_cluster::{CurrentClusterState, Member, MemberStatus};
    use kairo_serialization::{Registry, RemoteMessage};

    use super::*;
    use crate::{
        AggregationTransport, DataEnvelope, DeltaPropagationLog, DeltaPropagationTransport,
        DeltaReplicatedData, GCounter, GCounterCodec, ReadAggregationPlan, ReadAggregatorState,
        ReadConsistency, ReplicatorGossipTransport, ReplicatorKey, WriteAggregationPlan,
        WriteAggregatorState, WriteConsistency, decode_delta_propagation,
        register_ddata_protocol_codecs,
    };

    #[test]
    fn route_targets_register_remote_envelope_ddata_targets() {
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let weak = node("weak", 3);
        let joining = node("joining", 4);
        let routes = ReplicatorClusterRoutes::from_current_state(
            self_node.clone(),
            &CurrentClusterState {
                members: vec![
                    member(self_node.clone(), MemberStatus::Up),
                    member(peer.clone(), MemberStatus::Up),
                    member(weak.clone(), MemberStatus::WeaklyUp),
                    member(joining, MemberStatus::Joining),
                ],
                unreachable: Vec::new(),
                seen_by: HashSet::new(),
                leader: Some(self_node.clone()),
                role_leaders: HashMap::new(),
                member_tombstones: HashSet::new(),
            },
            ["ddata"],
        );
        let (outbound, rx) = channel_outbound();
        let route_targets = ReplicatorRemoteRouteTargets::new(registry(), outbound);
        let mut delta_transport =
            DeltaPropagationTransport::new(ReplicaId::from(&self_node), GCounterCodec);
        let mut aggregation_transport =
            AggregationTransport::new(ReplicaId::from(&self_node), GCounterCodec);
        let mut gossip_transport = ReplicatorGossipTransport::new();

        let delta_report = route_targets
            .set_delta_targets(&routes, &mut delta_transport)
            .unwrap();
        let aggregation_report = route_targets
            .set_aggregation_targets(&routes, &mut aggregation_transport)
            .unwrap();
        let gossip_report = route_targets
            .set_gossip_targets(&routes, &mut gossip_transport)
            .unwrap();

        assert_eq!(
            delta_report.registered(),
            &[ReplicaId::from(&peer), ReplicaId::from(&weak)]
        );
        assert_eq!(aggregation_report.registered(), delta_report.registered());
        assert_eq!(gossip_report.registered(), delta_report.registered());
        assert_eq!(delta_transport.target_count(), 2);
        assert_eq!(aggregation_transport.target_count(), 2);
        assert_eq!(gossip_transport.target_count(), 2);

        let key = ReplicatorKey::new("counter");
        let mut log = DeltaPropagationLog::new([ReplicaId::from(&peer)]);
        log.record_delta(key.clone(), Some(counter_delta("self", 7)));
        let delta_report = delta_transport.publish(log.collect_propagations());
        assert!(delta_report.is_success());

        let delta_envelope = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(delta_envelope.target, ReplicaId::from(&peer));
        assert_eq!(
            delta_envelope.envelope.recipient.path(),
            "kairo://ddata@peer.example.test:2552/system/ddata"
        );
        let propagation = delta_envelope.envelope.message;
        assert_eq!(
            propagation.manifest.as_str(),
            crate::ReplicatorDeltaPropagation::MANIFEST
        );
        let decoded = decode_delta_propagation(
            &registry()
                .deserialize::<crate::ReplicatorDeltaPropagation>(propagation)
                .unwrap(),
            &GCounterCodec,
        )
        .unwrap();
        assert_eq!(decoded[0].key(), &key);

        let remote_nodes = vec![ReplicaId::from(&peer), ReplicaId::from(&weak)];
        let write_state = WriteAggregatorState::new(
            key.clone(),
            &WriteConsistency::all(Duration::from_secs(1)),
            remote_nodes.clone(),
        )
        .unwrap();
        let write_plan = WriteAggregationPlan::new(
            write_state.clone(),
            write_state.select_replicas(&Default::default()),
        );
        let read_state = ReadAggregatorState::<GCounter>::new(
            key.clone(),
            &ReadConsistency::all(Duration::from_secs(1)),
            remote_nodes,
            None,
        )
        .unwrap();
        let read_plan = ReadAggregationPlan::new(
            read_state.clone(),
            read_state.select_replicas(&Default::default()),
        );
        let envelope = DataEnvelope::new(counter_delta("self", 9).reset_delta());

        assert!(
            aggregation_transport
                .publish_write(&write_plan, &envelope)
                .is_success()
        );
        assert!(aggregation_transport.publish_read(&read_plan).is_success());
        let write_peer_envelope = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let write_weak_envelope = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let read_peer_envelope = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let read_weak_envelope = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            write_peer_envelope.envelope.message.manifest.as_str(),
            crate::ReplicatorWrite::MANIFEST
        );
        assert_eq!(
            write_weak_envelope.envelope.message.manifest.as_str(),
            crate::ReplicatorWrite::MANIFEST
        );
        assert_eq!(
            read_peer_envelope.envelope.message.manifest.as_str(),
            crate::ReplicatorRead::MANIFEST
        );
        assert_eq!(
            read_weak_envelope.envelope.message.manifest.as_str(),
            crate::ReplicatorRead::MANIFEST
        );
    }

    #[test]
    fn route_targets_reject_local_only_cluster_addresses() {
        let route_targets = ReplicatorRemoteRouteTargets::new(registry(), channel_outbound().0);
        let error = route_targets
            .target_for_node(&UniqueAddress::new(Address::local("ddata"), 1))
            .expect_err("local-only addresses are not remote targets");

        assert!(matches!(
            error,
            ReplicatorRemoteTargetError::MissingRemoteHost { .. }
        ));
    }

    #[test]
    fn cloned_transports_share_later_target_registration() {
        let (target, rx) = channel_outbound();
        let mut transport = DeltaPropagationTransport::new(ReplicaId::new("self"), GCounterCodec);
        let cloned = transport.clone();
        transport.insert_target(DeltaPropagationTarget::new(
            ReplicaId::new("peer"),
            ReplicatorRemoteEnvelopeOutbound::new(
                ReplicatorRemoteTarget::new(
                    ReplicaId::new("peer"),
                    ActorRefWireData::new("kairo://ddata@peer.example.test:2552/system/ddata")
                        .unwrap(),
                ),
                None,
                registry(),
                target,
            ),
        ));
        let mut log = DeltaPropagationLog::new([ReplicaId::new("peer")]);
        log.record_delta(
            ReplicatorKey::new("counter"),
            Some(counter_delta("self", 1)),
        );

        let report = cloned.publish(log.collect_propagations());

        assert!(report.is_success());
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap().target,
            ReplicaId::new("peer")
        );
    }

    #[derive(Clone)]
    struct ChannelOutbound {
        tx: mpsc::Sender<ReplicatorRemoteEnvelope>,
    }

    impl Recipient<ReplicatorRemoteEnvelope> for ChannelOutbound {
        fn tell(
            &self,
            message: ReplicatorRemoteEnvelope,
        ) -> Result<(), SendError<ReplicatorRemoteEnvelope>> {
            self.tx
                .send(message)
                .map_err(|error| SendError::new(error.0, "channel closed".to_string()))
        }
    }

    fn channel_outbound() -> (ChannelOutbound, mpsc::Receiver<ReplicatorRemoteEnvelope>) {
        let (tx, rx) = mpsc::channel();
        (ChannelOutbound { tx }, rx)
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_ddata_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn node(name: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new(
                "kairo",
                "ddata",
                Some(format!("{name}.example.test")),
                Some(2552),
            ),
            uid,
        )
    }

    fn member(node: UniqueAddress, status: MemberStatus) -> Member {
        Member::new(node, vec!["ddata".to_string()]).with_status(status)
    }

    fn counter_delta(replica: &str, value: u128) -> GCounter {
        GCounter::new()
            .increment(ReplicaId::new(replica), value)
            .unwrap()
    }
}
