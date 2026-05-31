use std::sync::Arc;
use std::time::{Duration, Instant};

use kairo_actor::{ActorRef, Props};
use kairo_remote::{RemoteOutbound, RemoteSettings};
use kairo_serialization::{ActorRefWireData, Registry};
use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

use crate::{
    ClusterEventPublisher, ClusterMembership, ClusterMembershipMsg,
    ClusterMembershipRemoteEnvelopeOutbound, ClusterMembershipWireInbound,
    ClusterMembershipWireOutbound, ClusterSystemInbound, ClusterTcpAssociationRuntime,
    DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH, DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH,
    DowningDecision, DowningProviderActor, DowningProviderMsg, DowningProviderSnapshot, Gossip,
    HeartbeatRemoteReceiverInbound, HeartbeatRemoteResponseInbound, HeartbeatSenderMsg, Join,
    MemberStatus, StaticDowningHook, UniqueAddress, register_cluster_protocol_codecs,
};

#[test]
fn tcp_membership_socket_updates_downing_provider_state() {
    let sender_kit = ActorSystemTestKit::new("cluster-live-downing-sender").unwrap();
    let (receiver_kit, receiver_time) =
        ActorSystemTestKit::with_manual_time("cluster-live-downing-receiver").unwrap();
    let registry = registry();
    let sender = bind_probe_runtime("sender", 1, 11, &sender_kit, registry.clone());
    let receiver_membership = SharedMembership::default();
    let receiver_snapshots = SharedSnapshotProbe::default();
    let receiver = bind_membership_runtime(
        "receiver",
        2,
        22,
        &receiver_kit,
        registry.clone(),
        receiver_membership.clone(),
        receiver_snapshots.clone(),
    );
    let receiver_membership = receiver_membership.take();
    let receiver_snapshots = receiver_snapshots.take();

    let registration = sender.dial(receiver.local_address().clone()).unwrap();
    wait_for_route(&receiver);

    assert_member_status(
        &receiver_membership,
        &receiver_kit
            .create_probe::<Gossip>("receiver-initial-gossip")
            .unwrap(),
        receiver.self_node(),
        MemberStatus::Up,
    );

    let outbound = ClusterMembershipWireOutbound::new(
        receiver.self_node().clone(),
        registry,
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(Arc::new(
            sender.association_cache().clone(),
        ) as Arc<dyn RemoteOutbound>),
    );
    outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: sender.self_node().clone(),
                roles: Vec::new(),
            },
            reply_to: None,
        })
        .unwrap();
    assert_member_status(
        &receiver_membership,
        &receiver_kit
            .create_probe::<Gossip>("receiver-joined-gossip")
            .unwrap(),
        sender.self_node(),
        MemberStatus::Joining,
    );

    receiver_membership
        .tell(ClusterMembershipMsg::MarkUnreachable {
            observer: receiver.self_node().clone(),
            subject: sender.self_node().clone(),
        })
        .unwrap();
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            receiver_snapshots
                .provider
                .tell(DowningProviderMsg::Snapshot {
                    reply_to: receiver_snapshots.snapshots.actor_ref(),
                })
                .map_err(|error| error.reason().to_string())?;
            let snapshot = receiver_snapshots
                .snapshots
                .expect_msg(Duration::from_millis(100))
                .map_err(|error| error.to_string())?;
            if snapshot.responsible
                && snapshot.stable_timer_active
                && snapshot.relevant_unreachable == vec![sender.self_node().clone()]
            {
                Ok(())
            } else {
                Err(format!("unexpected downing snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap();

    receiver_time.advance(Duration::from_millis(10));
    assert_member_status(
        &receiver_membership,
        &receiver_kit
            .create_probe::<Gossip>("receiver-downed-gossip")
            .unwrap(),
        sender.self_node(),
        MemberStatus::Down,
    );

    assert!(sender.remove_route(receiver.local_address()));
    drop(registration);
    sender.shutdown().unwrap();
    receiver.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_cluster_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

fn bind_probe_runtime(
    name: &str,
    node_uid: u64,
    system_uid: u64,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> ClusterTcpAssociationRuntime {
    ClusterTcpAssociationRuntime::bind(
        name,
        node_uid,
        system_uid,
        RemoteSettings::new("127.0.0.1", 0),
        move |self_node, cache| probe_inbound(name, kit, registry, self_node, cache),
    )
    .unwrap()
}

fn bind_membership_runtime(
    name: &str,
    node_uid: u64,
    system_uid: u64,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
    membership_slot: SharedMembership,
    snapshot_slot: SharedSnapshotProbe,
) -> ClusterTcpAssociationRuntime {
    ClusterTcpAssociationRuntime::bind(
        name,
        node_uid,
        system_uid,
        RemoteSettings::new("127.0.0.1", 0),
        move |self_node, cache| {
            membership_inbound(
                name,
                kit,
                registry,
                self_node,
                cache,
                membership_slot,
                snapshot_slot,
            )
        },
    )
    .unwrap()
}

fn probe_inbound(
    name: &str,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
    self_node: UniqueAddress,
    cache: kairo_remote::RemoteAssociationCache,
) -> ClusterSystemInbound {
    let membership = kit
        .create_probe::<ClusterMembershipMsg>(format!("{name}-membership"))
        .unwrap();
    let heartbeat_sender = kit
        .create_probe::<HeartbeatSenderMsg>(format!("{name}-heartbeat-sender"))
        .unwrap();
    base_inbound(
        self_node,
        registry,
        cache,
        membership.actor_ref(),
        heartbeat_sender.actor_ref(),
    )
}

fn membership_inbound(
    name: &str,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
    self_node: UniqueAddress,
    cache: kairo_remote::RemoteAssociationCache,
    membership_slot: SharedMembership,
    snapshot_slot: SharedSnapshotProbe,
) -> ClusterSystemInbound {
    let publisher = kit
        .system()
        .spawn(
            format!("{name}-publisher"),
            Props::new({
                let self_node = self_node.clone();
                move || ClusterEventPublisher::new(self_node.clone())
            }),
        )
        .unwrap();
    let membership = kit
        .system()
        .spawn(
            format!("{name}-membership"),
            Props::new({
                let self_node = self_node.clone();
                move || ClusterMembership::new(self_node.clone(), Vec::new(), publisher.clone())
            }),
        )
        .unwrap();
    let snapshots = kit
        .create_probe::<DowningProviderSnapshot>(format!("{name}-downing-snapshots"))
        .unwrap();
    let provider = kit
        .system()
        .spawn(
            format!("{name}-downing-provider"),
            DowningProviderActor::props(
                self_node.clone(),
                StaticDowningHook::new(DowningDecision::DownUnreachable),
                membership.clone(),
                Duration::from_millis(10),
            ),
        )
        .unwrap();
    membership
        .tell(ClusterMembershipMsg::RegisterDowningProvider {
            provider: provider.clone(),
        })
        .unwrap();
    membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
    membership_slot.store(membership.clone());
    snapshot_slot.store(DowningProviderProbe {
        provider,
        snapshots,
    });

    let heartbeat_sender = kit
        .create_probe::<HeartbeatSenderMsg>(format!("{name}-heartbeat-sender"))
        .unwrap();
    base_inbound(
        self_node,
        registry,
        cache,
        membership,
        heartbeat_sender.actor_ref(),
    )
}

fn base_inbound(
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    cache: kairo_remote::RemoteAssociationCache,
    membership: ActorRef<ClusterMembershipMsg>,
    heartbeat_sender: ActorRef<HeartbeatSenderMsg>,
) -> ClusterSystemInbound {
    ClusterSystemInbound::new(self_node.clone())
        .with_membership(ClusterMembershipWireInbound::new(
            self_node.clone(),
            registry.clone(),
            membership,
        ))
        .with_heartbeat_receiver(
            HeartbeatRemoteReceiverInbound::from_arc(
                self_node.clone(),
                registry.clone(),
                Arc::new(cache) as Arc<dyn RemoteOutbound>,
            )
            .with_sender(Some(wire_for(
                &self_node,
                DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
            ))),
        )
        .with_heartbeat_response(HeartbeatRemoteResponseInbound::new(
            wire_for(&self_node, DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH),
            registry,
            heartbeat_sender,
        ))
}

fn assert_member_status(
    membership: &ActorRef<ClusterMembershipMsg>,
    probe: &TestProbe<Gossip>,
    node: &UniqueAddress,
    status: MemberStatus,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            membership
                .tell(ClusterMembershipMsg::SendCurrentGossip {
                    reply_to: probe.actor_ref(),
                })
                .map_err(|error| error.reason().to_string())?;
            let gossip = probe
                .expect_msg(Duration::from_millis(100))
                .map_err(|error| error.to_string())?;
            match gossip.member(node).map(|member| member.status) {
                Some(actual) if actual == status => Ok(()),
                actual => Err(format!(
                    "expected {} to be {status:?}, got {actual:?}",
                    node.ordering_key()
                )),
            }
        },
    )
    .unwrap();
}

fn wait_for_route(runtime: &ClusterTcpAssociationRuntime) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while runtime.association_cache().route_count() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(runtime.association_cache().route_count(), 1);
}

fn wire_for(node: &UniqueAddress, path: &str) -> ActorRefWireData {
    ActorRefWireData::new(format!("{}{}", node.address, path)).unwrap()
}

#[derive(Clone, Default)]
struct SharedMembership {
    inner: Arc<std::sync::Mutex<Option<ActorRef<ClusterMembershipMsg>>>>,
}

impl SharedMembership {
    fn store(&self, membership: ActorRef<ClusterMembershipMsg>) {
        *self.inner.lock().expect("membership slot lock poisoned") = Some(membership);
    }

    fn take(&self) -> ActorRef<ClusterMembershipMsg> {
        self.inner
            .lock()
            .expect("membership slot lock poisoned")
            .clone()
            .expect("membership actor should be installed")
    }
}

#[derive(Clone, Default)]
struct SharedSnapshotProbe {
    inner: Arc<std::sync::Mutex<Option<DowningProviderProbe>>>,
}

impl SharedSnapshotProbe {
    fn store(&self, probe: DowningProviderProbe) {
        *self.inner.lock().expect("snapshot slot lock poisoned") = Some(probe);
    }

    fn take(&self) -> DowningProviderProbe {
        self.inner
            .lock()
            .expect("snapshot slot lock poisoned")
            .take()
            .expect("downing provider probe should be installed")
    }
}

struct DowningProviderProbe {
    provider: ActorRef<DowningProviderMsg>,
    snapshots: TestProbe<DowningProviderSnapshot>,
}
