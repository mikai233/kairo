use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use kairo_actor::{Address, Props};
use kairo_remote::{RemoteAssociationAddress, RemoteAssociationCache, Result};
use kairo_serialization::{
    ActorRefWireData, Manifest, Registry, RemoteEnvelope, RemoteMessage, SerializedMessage,
};
use kairo_testkit::ActorSystemTestKit;

use super::*;
use crate::{
    CurrentClusterState, DeadlineFailureDetectorSettings, HEARTBEAT_SERIALIZER_ID, Heartbeat,
    HeartbeatReceiverMsg, HeartbeatSender, HeartbeatSenderMsg, HeartbeatSenderSettings, Member,
    MemberStatus, UniqueAddress, register_cluster_control_codecs,
};

#[derive(Default)]
struct CollectingRemoteOutbound {
    sent: Mutex<Vec<RemoteEnvelope>>,
    changed: Condvar,
}

impl CollectingRemoteOutbound {
    fn sent(&self) -> Vec<RemoteEnvelope> {
        self.sent
            .lock()
            .expect("collecting remote outbound poisoned")
            .clone()
    }

    fn wait_for_len(&self, len: usize, timeout: Duration) -> Vec<RemoteEnvelope> {
        let deadline = Instant::now() + timeout;
        let mut sent = self
            .sent
            .lock()
            .expect("collecting remote outbound poisoned");
        while sent.len() < len {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            let (next_sent, wait) = self
                .changed
                .wait_timeout(sent, remaining)
                .expect("collecting remote outbound poisoned");
            sent = next_sent;
            if wait.timed_out() {
                break;
            }
        }
        sent.clone()
    }
}

impl kairo_remote::RemoteOutbound for CollectingRemoteOutbound {
    fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
        self.sent
            .lock()
            .expect("collecting remote outbound poisoned")
            .push(envelope);
        self.changed.notify_all();
        Ok(())
    }
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_cluster_control_codecs(&mut registry).unwrap();
    Arc::new(registry)
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

fn sender_wire() -> ActorRefWireData {
    ActorRefWireData::new(format!(
        "kairo://cluster@sender.example.test:2552{}",
        DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH
    ))
    .unwrap()
}

fn receiver_wire(node: &UniqueAddress) -> ActorRefWireData {
    ActorRefWireData::new(format!(
        "{}{}",
        node.address, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH
    ))
    .unwrap()
}

fn settings() -> HeartbeatSenderSettings {
    HeartbeatSenderSettings::new(
        3,
        DeadlineFailureDetectorSettings::new(
            Duration::from_millis(1_000),
            Duration::from_millis(3_000),
        )
        .unwrap(),
    )
    .with_automatic_ticks(false)
}

fn member(unique_address: UniqueAddress) -> Member {
    Member::new(unique_address, Vec::new())
        .with_status(MemberStatus::Up)
        .with_up_number(1)
}

fn cluster_state(self_node: UniqueAddress, peer: UniqueAddress) -> CurrentClusterState {
    CurrentClusterState {
        members: vec![member(self_node), member(peer)],
        unreachable: Vec::new(),
        seen_by: std::collections::HashSet::new(),
        leader: None,
        role_leaders: std::collections::HashMap::new(),
        member_tombstones: std::collections::HashSet::new(),
    }
}

#[test]
fn outbound_actor_wraps_heartbeat_for_remote_receiver_path() {
    let kit = ActorSystemTestKit::new("cluster-heartbeat-remote-out").unwrap();
    let registry = registry();
    let target = node("receiver", 2);
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    let outbound = kit
        .system()
        .spawn(
            "remote-heartbeat",
            Props::new({
                let target = target.clone();
                let registry = registry.clone();
                let collecting = collecting.clone();
                move || {
                    HeartbeatRemoteReceiverOutbound::from_arc(
                        target.clone(),
                        registry.clone(),
                        sender_wire(),
                        collecting.clone() as Arc<dyn kairo_remote::RemoteOutbound>,
                    )
                }
            }),
        )
        .unwrap();
    let reply_probe = kit.create_probe::<HeartbeatSenderMsg>("reply").unwrap();

    outbound
        .tell(HeartbeatReceiverMsg::Heartbeat {
            heartbeat: Heartbeat {
                from: node("sender", 1),
                sequence_nr: 7,
                creation_time_nanos: 42,
            },
            reply_to: reply_probe.actor_ref(),
        })
        .unwrap();

    let sent = collecting.wait_for_len(1, Duration::from_secs(1));
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].recipient, receiver_wire(&target));
    assert_eq!(sent[0].sender, Some(sender_wire()));
    assert_eq!(sent[0].message.serializer_id, HEARTBEAT_SERIALIZER_ID);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn receiver_inbound_replies_to_remote_sender_metadata() {
    let registry = registry();
    let receiver = node("receiver", 2);
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    let inbound = HeartbeatRemoteReceiverInbound::from_arc(
        receiver.clone(),
        registry.clone(),
        collecting.clone() as Arc<dyn kairo_remote::RemoteOutbound>,
    )
    .with_sender(Some(receiver_wire(&receiver)));
    let request = RemoteEnvelope::new(
        receiver_wire(&receiver),
        Some(sender_wire()),
        registry
            .serialize(&Heartbeat {
                from: node("sender", 1),
                sequence_nr: 9,
                creation_time_nanos: 123,
            })
            .unwrap(),
    );

    inbound.receive(request).unwrap();

    let sent = collecting.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].recipient, sender_wire());
    assert_eq!(sent[0].sender, Some(receiver_wire(&receiver)));
    let response = registry
        .deserialize::<crate::HeartbeatRsp>(sent[0].message.clone())
        .unwrap();
    assert_eq!(response.from, receiver);
    assert_eq!(response.sequence_nr, 9);
    assert_eq!(response.creation_time_nanos, 123);
}

#[test]
fn remote_heartbeat_round_trip_updates_sender_failure_detector() {
    let kit = ActorSystemTestKit::new("cluster-heartbeat-remote-roundtrip").unwrap();
    let registry = registry();
    let sender_node = node("sender", 1);
    let receiver_node = node("receiver", 2);
    let sender = kit
        .system()
        .spawn(
            "sender",
            Props::new({
                let sender_node = sender_node.clone();
                move || HeartbeatSender::new(sender_node.clone(), settings()).unwrap()
            }),
        )
        .unwrap();
    let outbound_messages = Arc::new(CollectingRemoteOutbound::default());
    let remote_receiver = kit
        .system()
        .spawn(
            "remote-receiver",
            Props::new({
                let receiver_node = receiver_node.clone();
                let registry = registry.clone();
                let outbound_messages = outbound_messages.clone();
                move || {
                    HeartbeatRemoteReceiverOutbound::from_arc(
                        receiver_node.clone(),
                        registry.clone(),
                        sender_wire(),
                        outbound_messages.clone() as Arc<dyn kairo_remote::RemoteOutbound>,
                    )
                }
            }),
        )
        .unwrap();
    sender
        .tell(HeartbeatSenderMsg::RegisterReceiver {
            node: receiver_node.clone(),
            receiver: remote_receiver,
        })
        .unwrap();
    sender
        .tell(HeartbeatSenderMsg::Init(cluster_state(
            sender_node.clone(),
            receiver_node.clone(),
        )))
        .unwrap();
    sender.tell(HeartbeatSenderMsg::HeartbeatTick).unwrap();

    let heartbeat_envelope = outbound_messages
        .wait_for_len(1, Duration::from_secs(1))
        .remove(0);
    let response_messages = Arc::new(CollectingRemoteOutbound::default());
    let receiver_inbound = HeartbeatRemoteReceiverInbound::from_arc(
        receiver_node.clone(),
        registry.clone(),
        response_messages.clone() as Arc<dyn kairo_remote::RemoteOutbound>,
    )
    .with_sender(Some(receiver_wire(&receiver_node)));
    receiver_inbound.receive(heartbeat_envelope).unwrap();
    let response_envelope = response_messages.sent().remove(0);
    let response_inbound =
        HeartbeatRemoteResponseInbound::new(sender_wire(), registry.clone(), sender.clone());
    response_inbound.receive(response_envelope).unwrap();

    let probe = kit
        .create_probe::<crate::HeartbeatSenderSnapshot>("snapshot")
        .unwrap();
    sender
        .tell(HeartbeatSenderMsg::SendSnapshot {
            reply_to: probe.actor_ref(),
        })
        .unwrap();
    let snapshot = probe.expect_msg(Duration::from_secs(1)).unwrap();
    assert!(snapshot.monitored_receivers.contains(&receiver_node));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn outbound_can_use_shared_association_cache() {
    let registry = registry();
    let cache = RemoteAssociationCache::new();
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    cache.insert_route(
        RemoteAssociationAddress::new("kairo", "cluster", "receiver.example.test", Some(2552))
            .unwrap(),
        collecting.clone() as Arc<dyn kairo_remote::RemoteOutbound>,
    );
    let outbound =
        HeartbeatRemoteReceiverOutbound::new(node("receiver", 2), registry, sender_wire(), cache);

    outbound
        .send_heartbeat(Heartbeat {
            from: node("sender", 1),
            sequence_nr: 1,
            creation_time_nanos: 2,
        })
        .unwrap();

    let sent = collecting.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].recipient.path(),
        "kairo://cluster@receiver.example.test:2552/system/cluster/heartbeatReceiver"
    );
}

#[test]
fn receiver_inbound_rejects_missing_sender_and_wrong_recipient() {
    let registry = registry();
    let receiver = node("receiver", 2);
    let inbound = HeartbeatRemoteReceiverInbound::new(
        receiver.clone(),
        registry.clone(),
        CollectingRemoteOutbound::default(),
    );
    let missing_sender = RemoteEnvelope::new(
        receiver_wire(&receiver),
        None,
        registry
            .serialize(&Heartbeat {
                from: node("sender", 1),
                sequence_nr: 1,
                creation_time_nanos: 2,
            })
            .unwrap(),
    );
    assert!(matches!(
        inbound.receive(missing_sender).unwrap_err(),
        ClusterHeartbeatRemoteError::MissingSender
    ));

    let wrong_recipient = RemoteEnvelope::new(
        ActorRefWireData::new(
            "kairo://cluster@other.example.test:2552/system/cluster/heartbeatReceiver",
        )
        .unwrap(),
        Some(sender_wire()),
        SerializedMessage::new(
            HEARTBEAT_SERIALIZER_ID,
            Manifest::new(Heartbeat::MANIFEST),
            Heartbeat::VERSION,
            bytes::Bytes::new(),
        ),
    );
    assert!(matches!(
        inbound.receive(wrong_recipient).unwrap_err(),
        ClusterHeartbeatRemoteError::WrongRecipient { .. }
    ));
}
