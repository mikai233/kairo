use std::collections::{HashMap, HashSet};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use kairo_actor::{Address, Recipient, SendError};
use kairo_cluster::{CurrentClusterState, Member, MemberStatus};
use kairo_remote::{RemoteAssociationAddress, RemoteAssociationCache, RemoteOutbound};
use kairo_serialization::{Registry, RemoteEnvelope, RemoteMessage};

use super::*;

#[test]
fn family_path_uses_documented_fnv1a_manifest_token() {
    assert_eq!(
        replicator_remote_path_for_manifest("kairo.ddata.gcounter").unwrap(),
        "/system/ddata-12852b5274ed2a86"
    );
    assert!(matches!(
        replicator_remote_path_for_manifest("  "),
        Err(ReplicatorRemoteTargetError::InvalidDataManifest)
    ));
}
use crate::{
    AggregationTransport, DataEnvelope, DeltaPropagationLog, DeltaPropagationTransport,
    DeltaReplicatedData, GCounter, GCounterCodec, ReadAggregationPlan, ReadAggregatorState,
    ReadConsistency, ReplicatorGossipTransport, ReplicatorKey,
    ReplicatorRemoteAssociationCacheOutbound, WriteAggregationPlan, WriteAggregatorState,
    WriteConsistency, decode_delta_propagation, register_ddata_protocol_codecs,
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
    let transport = DeltaPropagationTransport::new(ReplicaId::new("self"), GCounterCodec);
    let cloned = transport.clone();
    transport.insert_target(DeltaPropagationTarget::new(
        ReplicaId::new("peer"),
        ReplicatorRemoteEnvelopeOutbound::new(
            ReplicatorRemoteTarget::new(
                ReplicaId::new("peer"),
                ActorRefWireData::new("kairo://ddata@peer.example.test:2552/system/ddata").unwrap(),
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

#[test]
fn route_targets_can_use_association_cache_outbound() {
    let self_node = node("self", 1);
    let peer = node("peer", 2);
    let routes = ReplicatorClusterRoutes::from_current_state(
        self_node.clone(),
        &CurrentClusterState {
            members: vec![
                member(self_node.clone(), MemberStatus::Up),
                member(peer.clone(), MemberStatus::Up),
            ],
            unreachable: Vec::new(),
            seen_by: HashSet::new(),
            leader: Some(self_node.clone()),
            role_leaders: HashMap::new(),
            member_tombstones: HashSet::new(),
        },
        ["ddata"],
    );
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    let cache = RemoteAssociationCache::new();
    cache.insert_route(
        RemoteAssociationAddress::new("kairo", "ddata", "peer.example.test", Some(2552)).unwrap(),
        collecting.clone() as Arc<dyn RemoteOutbound>,
    );
    let outbound = ReplicatorRemoteAssociationCacheOutbound::new(cache);
    let route_targets = ReplicatorRemoteRouteTargets::new(registry(), outbound);
    let mut delta_transport =
        DeltaPropagationTransport::new(ReplicaId::from(&self_node), GCounterCodec);

    let report = route_targets
        .set_delta_targets(&routes, &mut delta_transport)
        .unwrap();
    assert_eq!(report.registered(), &[ReplicaId::from(&peer)]);

    let key = ReplicatorKey::new("counter");
    let mut log = DeltaPropagationLog::new([ReplicaId::from(&peer)]);
    log.record_delta(key, Some(counter_delta("self", 41)));
    let publish_report = delta_transport.publish(log.collect_propagations());

    assert!(publish_report.is_success());
    let sent = collecting.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].recipient.path(),
        "kairo://ddata@peer.example.test:2552/system/ddata"
    );
    assert_eq!(
        sent[0].message.manifest.as_str(),
        crate::ReplicatorDeltaPropagation::MANIFEST
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

#[derive(Default)]
struct CollectingRemoteOutbound {
    sent: std::sync::Mutex<Vec<RemoteEnvelope>>,
}

impl CollectingRemoteOutbound {
    fn sent(&self) -> Vec<RemoteEnvelope> {
        self.sent
            .lock()
            .expect("collecting remote outbound poisoned")
            .clone()
    }
}

impl RemoteOutbound for CollectingRemoteOutbound {
    fn send(&self, envelope: RemoteEnvelope) -> kairo_remote::Result<()> {
        self.sent
            .lock()
            .expect("collecting remote outbound poisoned")
            .push(envelope);
        Ok(())
    }
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
