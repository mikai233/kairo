use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
    SerializedMessage,
};

use crate::{
    ReplicaId, ReplicatorDeltaPropagation, ReplicatorGossip, ReplicatorGossipStatus,
    ReplicatorRead, ReplicatorReadResult, ReplicatorWrite, ReplicatorWriteAck, ReplicatorWriteNack,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRemoteTarget {
    replica: ReplicaId,
    recipient: ActorRefWireData,
}

impl ReplicatorRemoteTarget {
    pub fn new(replica: ReplicaId, recipient: ActorRefWireData) -> Self {
        Self { replica, recipient }
    }

    pub fn replica(&self) -> &ReplicaId {
        &self.replica
    }

    pub fn recipient(&self) -> &ActorRefWireData {
        &self.recipient
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRemoteEnvelope {
    pub target: ReplicaId,
    pub envelope: RemoteEnvelope,
}

impl ReplicatorRemoteEnvelope {
    pub fn new(target: ReplicaId, envelope: RemoteEnvelope) -> Self {
        Self { target, envelope }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRemoteInboundMessage {
    pub sender: Option<ActorRefWireData>,
    pub message: SerializedMessage,
}

impl ReplicatorRemoteInboundMessage {
    pub fn new(sender: Option<ActorRefWireData>, message: SerializedMessage) -> Self {
        Self { sender, message }
    }
}

#[derive(Debug)]
pub enum ReplicatorRemoteEnvelopeError {
    Serialization(SerializationError),
    Send(String),
    WrongRecipient { expected: String, actual: String },
}

impl Display for ReplicatorRemoteEnvelopeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => {
                write!(
                    f,
                    "replicator remote envelope serialization failed: {error}"
                )
            }
            Self::Send(reason) => {
                write!(f, "replicator remote envelope delivery failed: {reason}")
            }
            Self::WrongRecipient { expected, actual } => {
                write!(
                    f,
                    "replicator remote envelope was addressed to {actual}, expected {expected}"
                )
            }
        }
    }
}

impl std::error::Error for ReplicatorRemoteEnvelopeError {}

impl From<SerializationError> for ReplicatorRemoteEnvelopeError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
pub struct ReplicatorRemoteEnvelopeOutbound {
    target: ReplicatorRemoteTarget,
    sender: Option<ActorRefWireData>,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<ReplicatorRemoteEnvelope> + Send + Sync>,
}

impl ReplicatorRemoteEnvelopeOutbound {
    pub fn new(
        target: ReplicatorRemoteTarget,
        sender: Option<ActorRefWireData>,
        registry: Arc<Registry>,
        outbound: impl Recipient<ReplicatorRemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self {
            target,
            sender,
            registry,
            outbound: Arc::new(outbound),
        }
    }

    pub fn from_arc(
        target: ReplicatorRemoteTarget,
        sender: Option<ActorRefWireData>,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<ReplicatorRemoteEnvelope> + Send + Sync>,
    ) -> Self {
        Self {
            target,
            sender,
            registry,
            outbound,
        }
    }

    pub fn target(&self) -> &ReplicatorRemoteTarget {
        &self.target
    }

    pub fn sender(&self) -> Option<&ActorRefWireData> {
        self.sender.as_ref()
    }

    pub fn with_sender(mut self, sender: Option<ActorRefWireData>) -> Self {
        self.sender = sender;
        self
    }

    pub fn send<M>(&self, message: &M) -> Result<(), ReplicatorRemoteEnvelopeError>
    where
        M: RemoteMessage,
    {
        let serialized = self.registry.serialize(message)?;
        let envelope = RemoteEnvelope::new(
            self.target.recipient.clone(),
            self.sender.clone(),
            serialized,
        );
        self.outbound
            .tell(ReplicatorRemoteEnvelope::new(
                self.target.replica.clone(),
                envelope,
            ))
            .map_err(|error| ReplicatorRemoteEnvelopeError::Send(error.reason().to_string()))
    }
}

impl Recipient<ReplicatorDeltaPropagation> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(
        &self,
        message: ReplicatorDeltaPropagation,
    ) -> Result<(), SendError<ReplicatorDeltaPropagation>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorWrite> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(&self, message: ReplicatorWrite) -> Result<(), SendError<ReplicatorWrite>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorRead> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(&self, message: ReplicatorRead) -> Result<(), SendError<ReplicatorRead>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorGossipStatus> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(
        &self,
        message: ReplicatorGossipStatus,
    ) -> Result<(), SendError<ReplicatorGossipStatus>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorGossip> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(&self, message: ReplicatorGossip) -> Result<(), SendError<ReplicatorGossip>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorWriteAck> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(&self, message: ReplicatorWriteAck) -> Result<(), SendError<ReplicatorWriteAck>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorWriteNack> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(&self, message: ReplicatorWriteNack) -> Result<(), SendError<ReplicatorWriteNack>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorReadResult> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(&self, message: ReplicatorReadResult) -> Result<(), SendError<ReplicatorReadResult>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

#[derive(Clone)]
pub struct ReplicatorRemoteEnvelopeInbound {
    recipient: ActorRefWireData,
}

impl ReplicatorRemoteEnvelopeInbound {
    pub fn new(recipient: ActorRefWireData) -> Self {
        Self { recipient }
    }

    pub fn recipient(&self) -> &ActorRefWireData {
        &self.recipient
    }

    pub fn receive(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<ReplicatorRemoteInboundMessage, ReplicatorRemoteEnvelopeError> {
        if envelope.recipient != self.recipient {
            return Err(ReplicatorRemoteEnvelopeError::WrongRecipient {
                expected: self.recipient.path().to_string(),
                actual: envelope.recipient.path().to_string(),
            });
        }
        Ok(ReplicatorRemoteInboundMessage::new(
            envelope.sender,
            envelope.message,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::mpsc::{self, Receiver};
    use std::time::Duration;

    use kairo_actor::{Actor, ActorRef, ActorResult, ActorSystem, Context, Props, Recipient};
    use kairo_serialization::{ActorRefWireData, Manifest, RemoteMessage};

    use super::*;
    use crate::{
        AggregationTarget, AggregationTransport, CrdtDataCodec, DataEnvelope, DeltaReplicatedData,
        GCounter, GCounterCodec, REPLICATOR_READ_RESULT_SERIALIZER_ID,
        REPLICATOR_READ_SERIALIZER_ID, REPLICATOR_WRITE_ACK_SERIALIZER_ID,
        REPLICATOR_WRITE_SERIALIZER_ID, ReadAggregationPlan, ReadAggregatorState, ReadConsistency,
        ReplicatorDataEnvelope, ReplicatorKey, WriteAggregationPlan, WriteAggregatorState,
        WriteConsistency, register_ddata_protocol_codecs,
    };

    struct Forward<M> {
        tx: mpsc::Sender<M>,
    }

    impl<M> Actor for Forward<M>
    where
        M: Send + 'static,
    {
        type Msg = M;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            self.tx
                .send(msg)
                .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
        }
    }

    fn probe<M>(system: &ActorSystem, name: &str) -> (ActorRef<M>, Receiver<M>)
    where
        M: Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let actor = system
            .spawn(name, Props::new(move || Forward { tx }))
            .unwrap();
        (actor, rx)
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_ddata_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn replica(id: &str) -> ReplicaId {
        ReplicaId::new(id)
    }

    fn actor_ref(path: &str) -> ActorRefWireData {
        ActorRefWireData::new(path).unwrap()
    }

    fn target() -> ReplicatorRemoteTarget {
        ReplicatorRemoteTarget::new(
            replica("remote"),
            actor_ref("kairo://remote@127.0.0.1:25521/system/ddata#1"),
        )
    }

    fn sender() -> ActorRefWireData {
        actor_ref("kairo://local@127.0.0.1:25520/system/ddata-agg-1#7")
    }

    fn counter(replica_id: &str, value: u128) -> GCounter {
        GCounter::new()
            .increment(replica(replica_id), value)
            .unwrap()
            .reset_delta()
    }

    #[test]
    fn remote_outbound_wraps_replicator_requests_with_sender_actor_ref() {
        let system = ActorSystem::builder("ddata-remote-envelope-out")
            .build()
            .unwrap();
        let registry = registry();
        let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
        let outbound = ReplicatorRemoteEnvelopeOutbound::new(
            target(),
            Some(sender()),
            registry.clone(),
            outbound_ref,
        );
        let key = ReplicatorKey::new("counter");
        let write = crate::encode_write(
            &key,
            Some(replica("local")),
            &DataEnvelope::new(counter("local", 5)),
            &GCounterCodec,
        )
        .unwrap();

        outbound.tell(write.clone()).unwrap();
        let sent = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(sent.target, replica("remote"));
        assert_eq!(sent.envelope.recipient, target().recipient().clone());
        assert_eq!(sent.envelope.sender, Some(sender()));
        assert_eq!(
            sent.envelope.message.serializer_id,
            REPLICATOR_WRITE_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorWrite>(sent.envelope.message)
                .unwrap(),
            write
        );

        let read = crate::encode_read(&key, Some(replica("local")));
        outbound.tell(read.clone()).unwrap();
        let sent = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            sent.envelope.message.serializer_id,
            REPLICATOR_READ_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorRead>(sent.envelope.message)
                .unwrap(),
            read
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn remote_outbound_wraps_replies_to_aggregator_actor_ref() {
        let system = ActorSystem::builder("ddata-remote-envelope-reply")
            .build()
            .unwrap();
        let registry = registry();
        let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
        let reply_target = ReplicatorRemoteTarget::new(replica("local"), sender());
        let outbound = ReplicatorRemoteEnvelopeOutbound::new(
            reply_target,
            None,
            registry.clone(),
            outbound_ref,
        );

        outbound.tell(ReplicatorWriteAck).unwrap();
        let sent = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(sent.target, replica("local"));
        assert_eq!(sent.envelope.recipient, sender());
        assert_eq!(sent.envelope.sender, None);
        assert_eq!(
            sent.envelope.message.serializer_id,
            REPLICATOR_WRITE_ACK_SERIALIZER_ID
        );

        let read_result = ReplicatorReadResult {
            envelope: Some(ReplicatorDataEnvelope::new(
                GCounterCodec.serialize(&counter("remote", 9)).unwrap(),
            )),
        };
        outbound.tell(read_result.clone()).unwrap();
        let sent = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            sent.envelope.message.serializer_id,
            REPLICATOR_READ_RESULT_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorReadResult>(sent.envelope.message)
                .unwrap(),
            read_result
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn aggregation_remote_target_preserves_session_sender_actor_ref() {
        let system = ActorSystem::builder("ddata-aggregation-remote-envelope")
            .build()
            .unwrap();
        let registry = registry();
        let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
        let outbound =
            ReplicatorRemoteEnvelopeOutbound::new(target(), None, registry.clone(), outbound_ref);
        let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
        transport.insert_target(AggregationTarget::remote_envelope(
            replica("remote"),
            outbound.clone(),
            outbound,
        ));
        let key = ReplicatorKey::new("counter");
        let write_state = WriteAggregatorState::new(
            key.clone(),
            &WriteConsistency::to(2, Duration::from_secs(1)).unwrap(),
            vec![replica("remote")],
        )
        .unwrap();
        let write_plan = WriteAggregationPlan::new(
            write_state.clone(),
            write_state.select_replicas(&BTreeSet::new()),
        );

        let report = transport.publish_write_with_sender(
            &write_plan,
            &DataEnvelope::new(counter("local", 5)),
            &sender(),
        );
        assert_eq!(report.sent_to(), &[replica("remote")]);
        let sent = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(sent.envelope.sender, Some(sender()));
        assert_eq!(
            sent.envelope.message.serializer_id,
            REPLICATOR_WRITE_SERIALIZER_ID
        );

        let read_state = ReadAggregatorState::<GCounter>::new(
            key,
            &ReadConsistency::from(2, Duration::from_secs(1)).unwrap(),
            vec![replica("remote")],
            None,
        )
        .unwrap();
        let read_plan = ReadAggregationPlan::new(
            read_state.clone(),
            read_state.select_replicas(&BTreeSet::new()),
        );
        let report = transport.publish_read_with_sender(&read_plan, &sender());
        assert_eq!(report.sent_to(), &[replica("remote")]);
        let sent = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(sent.envelope.sender, Some(sender()));
        assert_eq!(
            sent.envelope.message.serializer_id,
            REPLICATOR_READ_SERIALIZER_ID
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn remote_inbound_validates_recipient_and_preserves_sender() {
        let registry = registry();
        let recipient = target().recipient().clone();
        let inbound = ReplicatorRemoteEnvelopeInbound::new(recipient.clone());
        let read = crate::encode_read(&ReplicatorKey::new("counter"), Some(replica("local")));
        let envelope = RemoteEnvelope::new(
            recipient,
            Some(sender()),
            registry.serialize(&read).unwrap(),
        );

        let inbound_message = inbound.receive(envelope).unwrap();
        assert_eq!(inbound_message.sender, Some(sender()));
        assert_eq!(
            registry
                .deserialize::<ReplicatorRead>(inbound_message.message)
                .unwrap(),
            read
        );

        let wrong = inbound
            .receive(RemoteEnvelope::new(
                actor_ref("kairo://remote@127.0.0.1:25521/system/other#2"),
                None,
                SerializedMessage::new(
                    REPLICATOR_READ_SERIALIZER_ID,
                    Manifest::new(ReplicatorRead::MANIFEST),
                    ReplicatorRead::VERSION,
                    bytes::Bytes::new(),
                ),
            ))
            .expect_err("wrong recipient should fail");
        assert!(matches!(
            wrong,
            ReplicatorRemoteEnvelopeError::WrongRecipient { .. }
        ));
    }
}
