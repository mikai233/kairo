use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};
use kairo_remote::RemoteOutbound;
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
};

use crate::{Heartbeat, HeartbeatReceiverMsg, HeartbeatRsp, HeartbeatSenderMsg, UniqueAddress};

pub const DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH: &str = "/system/cluster/heartbeatReceiver";
pub const DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH: &str = "/system/cluster/heartbeatSender";

#[derive(Debug)]
pub enum ClusterHeartbeatRemoteError {
    InvalidRecipientPath(String),
    MissingRemoteHost { node: String },
    MissingSender,
    Serialization(SerializationError),
    Send { target: String, reason: String },
    UnsupportedManifest(String),
    WrongRecipient { expected: String, actual: String },
}

impl Display for ClusterHeartbeatRemoteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRecipientPath(path) => {
                write!(
                    f,
                    "cluster heartbeat remote path `{path}` must start with `/`"
                )
            }
            Self::MissingRemoteHost { node } => {
                write!(f, "cluster heartbeat target {node} has no remote host")
            }
            Self::MissingSender => {
                write!(
                    f,
                    "cluster heartbeat request is missing remote sender metadata"
                )
            }
            Self::Serialization(error) => write!(f, "{error}"),
            Self::Send { target, reason } => {
                write!(
                    f,
                    "cluster heartbeat remote send to {target} failed: {reason}"
                )
            }
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported cluster heartbeat manifest `{manifest}`")
            }
            Self::WrongRecipient { expected, actual } => {
                write!(
                    f,
                    "cluster heartbeat envelope was addressed to {actual}, expected {expected}"
                )
            }
        }
    }
}

impl std::error::Error for ClusterHeartbeatRemoteError {}

impl From<SerializationError> for ClusterHeartbeatRemoteError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
pub struct HeartbeatRemoteReceiverOutbound {
    target: UniqueAddress,
    registry: Arc<Registry>,
    sender: ActorRefWireData,
    recipient_path: String,
    outbound: Arc<dyn RemoteOutbound>,
}

impl HeartbeatRemoteReceiverOutbound {
    pub fn new(
        target: UniqueAddress,
        registry: Arc<Registry>,
        sender: ActorRefWireData,
        outbound: impl RemoteOutbound + 'static,
    ) -> Self {
        Self::from_arc(target, registry, sender, Arc::new(outbound))
    }

    pub fn from_arc(
        target: UniqueAddress,
        registry: Arc<Registry>,
        sender: ActorRefWireData,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        Self {
            target,
            registry,
            sender,
            recipient_path: DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH.to_string(),
            outbound,
        }
    }

    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    pub fn target(&self) -> &UniqueAddress {
        &self.target
    }

    pub fn sender(&self) -> &ActorRefWireData {
        &self.sender
    }

    pub fn recipient_for_target(&self) -> Result<ActorRefWireData, ClusterHeartbeatRemoteError> {
        recipient_for_node(&self.target, &self.recipient_path)
    }

    pub fn send_heartbeat(&self, heartbeat: Heartbeat) -> Result<(), ClusterHeartbeatRemoteError> {
        let recipient = self.recipient_for_target()?;
        let target = self.target.ordering_key();
        let message = self.registry.serialize(&heartbeat)?;
        let envelope = RemoteEnvelope::new(recipient, Some(self.sender.clone()), message);
        self.outbound
            .send(envelope)
            .map_err(|error| ClusterHeartbeatRemoteError::Send {
                target,
                reason: error.to_string(),
            })
    }
}

impl Actor for HeartbeatRemoteReceiverOutbound {
    type Msg = HeartbeatReceiverMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            HeartbeatReceiverMsg::Heartbeat { heartbeat, .. } => {
                self.send_heartbeat(heartbeat)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct HeartbeatRemoteReceiverInbound {
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    sender: Option<ActorRefWireData>,
    recipient_path: String,
    outbound: Arc<dyn RemoteOutbound>,
}

impl HeartbeatRemoteReceiverInbound {
    pub fn new(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        outbound: impl RemoteOutbound + 'static,
    ) -> Self {
        Self::from_arc(self_node, registry, Arc::new(outbound))
    }

    pub fn from_arc(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        Self {
            self_node,
            registry,
            sender: None,
            recipient_path: DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH.to_string(),
            outbound,
        }
    }

    pub fn with_sender(mut self, sender: Option<ActorRefWireData>) -> Self {
        self.sender = sender;
        self
    }

    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), ClusterHeartbeatRemoteError> {
        validate_recipient(&self.self_node, &self.recipient_path, &envelope.recipient)?;
        if envelope.message.manifest.as_str() != Heartbeat::MANIFEST {
            return Err(ClusterHeartbeatRemoteError::UnsupportedManifest(
                envelope.message.manifest.as_str().to_string(),
            ));
        }
        let response_recipient = envelope
            .sender
            .clone()
            .ok_or(ClusterHeartbeatRemoteError::MissingSender)?;
        let heartbeat = self.registry.deserialize::<Heartbeat>(envelope.message)?;
        let response = HeartbeatRsp {
            from: self.self_node.clone(),
            sequence_nr: heartbeat.sequence_nr,
            creation_time_nanos: heartbeat.creation_time_nanos,
        };
        let target = response_recipient.path().to_string();
        let envelope = RemoteEnvelope::new(
            response_recipient,
            self.sender.clone(),
            self.registry.serialize(&response)?,
        );
        self.outbound
            .send(envelope)
            .map_err(|error| ClusterHeartbeatRemoteError::Send {
                target,
                reason: error.to_string(),
            })
    }
}

#[derive(Clone)]
pub struct HeartbeatRemoteResponseInbound {
    recipient: ActorRefWireData,
    registry: Arc<Registry>,
    sender: ActorRef<HeartbeatSenderMsg>,
}

impl HeartbeatRemoteResponseInbound {
    pub fn new(
        recipient: ActorRefWireData,
        registry: Arc<Registry>,
        sender: ActorRef<HeartbeatSenderMsg>,
    ) -> Self {
        Self {
            recipient,
            registry,
            sender,
        }
    }

    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), ClusterHeartbeatRemoteError> {
        if envelope.recipient != self.recipient {
            return Err(ClusterHeartbeatRemoteError::WrongRecipient {
                expected: self.recipient.path().to_string(),
                actual: envelope.recipient.path().to_string(),
            });
        }
        if envelope.message.manifest.as_str() != HeartbeatRsp::MANIFEST {
            return Err(ClusterHeartbeatRemoteError::UnsupportedManifest(
                envelope.message.manifest.as_str().to_string(),
            ));
        }
        let response = self
            .registry
            .deserialize::<HeartbeatRsp>(envelope.message)?;
        self.sender
            .tell(HeartbeatSenderMsg::HeartbeatResponse(response))
            .map_err(|error| ClusterHeartbeatRemoteError::Send {
                target: self.sender.path().to_string(),
                reason: error.reason().to_string(),
            })
    }
}

fn recipient_for_node(
    node: &UniqueAddress,
    recipient_path: &str,
) -> Result<ActorRefWireData, ClusterHeartbeatRemoteError> {
    if !recipient_path.starts_with('/') {
        return Err(ClusterHeartbeatRemoteError::InvalidRecipientPath(
            recipient_path.to_string(),
        ));
    }
    if node.address.host().is_none() {
        return Err(ClusterHeartbeatRemoteError::MissingRemoteHost {
            node: node.ordering_key(),
        });
    }
    Ok(ActorRefWireData::new(format!(
        "{}{}",
        node.address, recipient_path
    ))?)
}

fn validate_recipient(
    node: &UniqueAddress,
    recipient_path: &str,
    actual: &ActorRefWireData,
) -> Result<(), ClusterHeartbeatRemoteError> {
    let expected = recipient_for_node(node, recipient_path)?;
    if actual != &expected {
        return Err(ClusterHeartbeatRemoteError::WrongRecipient {
            expected: expected.path().to_string(),
            actual: actual.path().to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    use kairo_actor::{Address, Props};
    use kairo_remote::{RemoteAssociationAddress, RemoteAssociationCache, Result};
    use kairo_serialization::{Manifest, RemoteMessage, SerializedMessage};
    use kairo_testkit::ActorSystemTestKit;

    use super::*;
    use crate::{
        CurrentClusterState, DeadlineFailureDetectorSettings, HEARTBEAT_SERIALIZER_ID,
        HeartbeatSender, HeartbeatSenderSettings, Member, MemberStatus,
        register_cluster_control_codecs,
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
            .deserialize::<HeartbeatRsp>(sent[0].message.clone())
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
        let outbound = HeartbeatRemoteReceiverOutbound::new(
            node("receiver", 2),
            registry,
            sender_wire(),
            cache,
        );

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
}
