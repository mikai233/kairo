use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{ActorSystem, Recipient, SendError};
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
    SerializedMessage,
};

use crate::{
    DeltaPropagationReceiveReport, DeltaReceiveReply, DirectReadResult, DirectWriteResult,
    ReadAggregationActorMsg, ReplicaId, ReplicatorDeltaAck, ReplicatorDeltaNack,
    ReplicatorReadResult, ReplicatorRemoteEnvelope, ReplicatorRemoteEnvelopeError,
    ReplicatorRemoteTarget, ReplicatorWireReply, ReplicatorWriteAck, ReplicatorWriteNack,
    WriteAggregationActorMsg,
};

#[derive(Debug)]
pub enum ReplicatorRemoteReplyError {
    Serialization(SerializationError),
    Send { recipient: String, reason: String },
    UnsupportedManifest(String),
}

impl Display for ReplicatorRemoteReplyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => write!(f, "replicator reply decode failed: {error}"),
            Self::Send { recipient, reason } => {
                write!(
                    f,
                    "replicator reply delivery to `{recipient}` failed: {reason}"
                )
            }
            Self::UnsupportedManifest(manifest) => {
                write!(
                    f,
                    "unsupported remote replicator reply manifest `{manifest}`"
                )
            }
        }
    }
}

impl std::error::Error for ReplicatorRemoteReplyError {}

impl From<SerializationError> for ReplicatorRemoteReplyError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
pub struct ReplicatorRemoteReplyOutbound {
    target: ReplicatorRemoteTarget,
    sender: Option<ActorRefWireData>,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<ReplicatorRemoteEnvelope> + Send + Sync>,
}

impl ReplicatorRemoteReplyOutbound {
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

    pub fn send_delta_report(
        &self,
        report: &DeltaPropagationReceiveReport,
    ) -> Result<bool, ReplicatorRemoteEnvelopeError> {
        match report.reply() {
            Some(DeltaReceiveReply::Ack(message)) => {
                self.send_remote_message(&message)?;
                Ok(true)
            }
            Some(DeltaReceiveReply::Nack(message)) => {
                self.send_remote_message(&message)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub fn send_write_result(
        &self,
        result: &DirectWriteResult,
    ) -> Result<(), ReplicatorRemoteEnvelopeError> {
        match result {
            DirectWriteResult::Ack { message, .. } => self.send_remote_message(message),
            DirectWriteResult::Nack { message, .. } => self.send_remote_message(message),
        }
    }

    pub fn send_read_result(
        &self,
        result: &DirectReadResult,
    ) -> Result<(), ReplicatorRemoteEnvelopeError> {
        self.send_remote_message(result.message())
    }

    fn send_remote_message<M>(&self, message: &M) -> Result<(), ReplicatorRemoteEnvelopeError>
    where
        M: RemoteMessage,
    {
        let serialized = self.registry.serialize(message)?;
        let envelope = RemoteEnvelope::new(
            self.target.recipient().clone(),
            self.sender.clone(),
            serialized,
        );
        self.outbound
            .tell(ReplicatorRemoteEnvelope::new(
                self.target.replica().clone(),
                envelope,
            ))
            .map_err(|error| ReplicatorRemoteEnvelopeError::Send(error.reason().to_string()))
    }
}

impl Recipient<DeltaPropagationReceiveReport> for ReplicatorRemoteReplyOutbound {
    fn tell(
        &self,
        message: DeltaPropagationReceiveReport,
    ) -> Result<(), SendError<DeltaPropagationReceiveReport>> {
        self.send_delta_report(&message)
            .map(|_| ())
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<DirectWriteResult> for ReplicatorRemoteReplyOutbound {
    fn tell(&self, message: DirectWriteResult) -> Result<(), SendError<DirectWriteResult>> {
        self.send_write_result(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<Result<DirectReadResult, String>> for ReplicatorRemoteReplyOutbound {
    fn tell(
        &self,
        message: Result<DirectReadResult, String>,
    ) -> Result<(), SendError<Result<DirectReadResult, String>>> {
        match message {
            Ok(result) => self
                .send_read_result(&result)
                .map_err(|error| SendError::new(Ok(result), error.to_string())),
            Err(reason) => Err(SendError::new(Err(reason.clone()), reason)),
        }
    }
}

#[derive(Clone)]
pub struct ReplicatorRemoteReplyInbound {
    system: ActorSystem,
    registry: Arc<Registry>,
}

impl ReplicatorRemoteReplyInbound {
    pub fn new(system: ActorSystem, registry: Arc<Registry>) -> Self {
        Self { system, registry }
    }

    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    pub fn receive_from(
        &self,
        from: ReplicaId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        self.receive_message(from, envelope.recipient, envelope.message)
    }

    pub fn receive_message(
        &self,
        from: ReplicaId,
        recipient: ActorRefWireData,
        message: SerializedMessage,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        match message.manifest.as_str() {
            ReplicatorDeltaAck::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorDeltaAck>(message)?;
                self.deliver_write(recipient, ReplicatorWireReply::DeltaAck { from, message })
            }
            ReplicatorDeltaNack::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorDeltaNack>(message)?;
                self.deliver_write(recipient, ReplicatorWireReply::DeltaNack { from, message })
            }
            ReplicatorWriteAck::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorWriteAck>(message)?;
                self.deliver_write(recipient, ReplicatorWireReply::WriteAck { from, message })
            }
            ReplicatorWriteNack::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorWriteNack>(message)?;
                self.deliver_write(recipient, ReplicatorWireReply::WriteNack { from, message })
            }
            ReplicatorReadResult::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorReadResult>(message)?;
                self.deliver_read(recipient, ReplicatorWireReply::ReadResult { from, message })
            }
            manifest => Err(ReplicatorRemoteReplyError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }

    fn deliver_write(
        &self,
        recipient: ActorRefWireData,
        reply: ReplicatorWireReply,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        let recipient_path = recipient.path().to_string();
        let actor = self
            .system
            .resolve_local_or_missing::<WriteAggregationActorMsg>(&recipient_path);
        actor
            .tell(WriteAggregationActorMsg::Reply(reply))
            .map_err(|error| ReplicatorRemoteReplyError::Send {
                recipient: recipient_path,
                reason: error.reason().to_string(),
            })
    }

    fn deliver_read(
        &self,
        recipient: ActorRefWireData,
        reply: ReplicatorWireReply,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        let recipient_path = recipient.path().to_string();
        let actor = self
            .system
            .resolve_local_or_missing::<ReadAggregationActorMsg>(&recipient_path);
        actor
            .tell(ReadAggregationActorMsg::Reply(reply))
            .map_err(|error| ReplicatorRemoteReplyError::Send {
                recipient: recipient_path,
                reason: error.reason().to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{self, Receiver};
    use std::time::Duration;

    use bytes::Bytes;
    use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};
    use kairo_serialization::{Manifest, RemoteEnvelope, SerializedMessage};

    use super::*;
    use crate::{
        DataEnvelope, DeltaReplicatedData, GCounter, GCounterCodec,
        REPLICATOR_DELTA_ACK_SERIALIZER_ID, REPLICATOR_READ_RESULT_SERIALIZER_ID,
        REPLICATOR_WRITE_ACK_SERIALIZER_ID, ReplicatorKey, ReplicatorRead, ReplicatorRemoteTarget,
        register_ddata_protocol_codecs,
    };

    struct WriteReplyProbe {
        tx: mpsc::Sender<ReplicatorWireReply>,
    }

    impl Actor for WriteReplyProbe {
        type Msg = WriteAggregationActorMsg;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            if let WriteAggregationActorMsg::Reply(reply) = msg {
                self.tx
                    .send(reply)
                    .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))?;
            }
            Ok(())
        }
    }

    struct ReadReplyProbe {
        tx: mpsc::Sender<ReplicatorWireReply>,
    }

    impl Actor for ReadReplyProbe {
        type Msg = ReadAggregationActorMsg;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            if let ReadAggregationActorMsg::Reply(reply) = msg {
                self.tx
                    .send(reply)
                    .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))?;
            }
            Ok(())
        }
    }

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

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_ddata_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn replica(id: &str) -> ReplicaId {
        ReplicaId::new(id)
    }

    fn counter(replica_id: &str, value: u128) -> GCounter {
        GCounter::new()
            .increment(replica(replica_id), value)
            .unwrap()
            .reset_delta()
    }

    fn actor_ref<M>(actor: &ActorRef<M>) -> ActorRefWireData
    where
        M: Send + 'static,
    {
        ActorRefWireData::new(actor.path().to_string()).unwrap()
    }

    fn wire_ref(path: &str) -> ActorRefWireData {
        ActorRefWireData::new(path).unwrap()
    }

    fn reply_target() -> ReplicatorRemoteTarget {
        ReplicatorRemoteTarget::new(
            replica("remote"),
            wire_ref("kairo://remote@127.0.0.1:25520/user/agg#9"),
        )
    }

    fn write_probe(
        system: &ActorSystem,
    ) -> (
        ActorRef<WriteAggregationActorMsg>,
        Receiver<ReplicatorWireReply>,
    ) {
        let (tx, rx) = mpsc::channel();
        let actor = system
            .spawn("write-agg", Props::new(move || WriteReplyProbe { tx }))
            .unwrap();
        (actor, rx)
    }

    fn read_probe(
        system: &ActorSystem,
    ) -> (
        ActorRef<ReadAggregationActorMsg>,
        Receiver<ReplicatorWireReply>,
    ) {
        let (tx, rx) = mpsc::channel();
        let actor = system
            .spawn("read-agg", Props::new(move || ReadReplyProbe { tx }))
            .unwrap();
        (actor, rx)
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

    #[test]
    fn remote_reply_outbound_sends_direct_results_to_original_sender_ref() {
        let system = ActorSystem::builder("ddata-remote-reply-out")
            .build()
            .unwrap();
        let registry = registry();
        let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
        let sender = wire_ref("kairo://local@127.0.0.1:25521/system/ddata#1");
        let outbound = ReplicatorRemoteReplyOutbound::new(
            reply_target(),
            Some(sender.clone()),
            registry.clone(),
            outbound_ref,
        );

        outbound
            .send_write_result(&DirectWriteResult::Ack {
                key: ReplicatorKey::new("counter"),
                from: Some(replica("remote")),
                changed: true,
                message: ReplicatorWriteAck,
            })
            .unwrap();
        let write = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(write.target, replica("remote"));
        assert_eq!(write.envelope.recipient, reply_target().recipient().clone());
        assert_eq!(write.envelope.sender, Some(sender.clone()));
        assert_eq!(
            write.envelope.message.serializer_id,
            REPLICATOR_WRITE_ACK_SERIALIZER_ID
        );

        let read_result = DirectReadResult::new(
            ReplicatorKey::new("counter"),
            Some(replica("remote")),
            crate::encode_read_result(
                Some(&DataEnvelope::new(counter("local", 4))),
                &GCounterCodec,
            )
            .unwrap(),
        );
        outbound.send_read_result(&read_result).unwrap();
        let read = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(read.envelope.recipient, reply_target().recipient().clone());
        assert_eq!(read.envelope.sender, Some(sender));
        assert_eq!(
            read.envelope.message.serializer_id,
            REPLICATOR_READ_RESULT_SERIALIZER_ID
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn remote_reply_outbound_sends_delta_ack_only_when_requested() {
        let system = ActorSystem::builder("ddata-remote-reply-delta-out")
            .build()
            .unwrap();
        let registry = registry();
        let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
        let outbound =
            ReplicatorRemoteReplyOutbound::new(reply_target(), None, registry, outbound_ref);
        let mut log = crate::DeltaPropagationLog::new([replica("local")]);
        log.record_delta(ReplicatorKey::new("counter"), Some(counter("remote", 3)));
        let propagation = log
            .collect_propagations()
            .remove(&replica("local"))
            .unwrap();
        let message =
            crate::encode_delta_propagation(replica("remote"), false, &propagation, &GCounterCodec)
                .unwrap();
        let mut state = crate::ReplicatorState::<GCounter>::new();
        let mut tracker = crate::DeltaReceiveTracker::new();
        let report = tracker.apply_propagation(&mut state, &message, &GCounterCodec);

        assert!(!outbound.send_delta_report(&report).unwrap());
        assert!(outbound_rx.recv_timeout(Duration::from_millis(50)).is_err());

        let message =
            crate::encode_delta_propagation(replica("remote"), true, &propagation, &GCounterCodec)
                .unwrap();
        let report = tracker.apply_propagation(&mut state, &message, &GCounterCodec);
        assert!(outbound.send_delta_report(&report).unwrap());
        let ack = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(ack.target, replica("remote"));
        assert_eq!(ack.envelope.recipient, reply_target().recipient().clone());
        assert_eq!(
            ack.envelope.message.serializer_id,
            REPLICATOR_DELTA_ACK_SERIALIZER_ID
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn remote_reply_inbound_delivers_write_replies_to_addressed_aggregator() {
        let system = ActorSystem::builder("ddata-remote-reply-write")
            .build()
            .unwrap();
        let registry = registry();
        let inbound = ReplicatorRemoteReplyInbound::new(system.clone(), registry.clone());
        let (recipient, replies) = write_probe(&system);

        inbound
            .receive_from(
                replica("remote"),
                RemoteEnvelope::new(
                    actor_ref(&recipient),
                    None,
                    registry.serialize(&ReplicatorWriteAck).unwrap(),
                ),
            )
            .unwrap();

        assert!(matches!(
            replies.recv_timeout(Duration::from_secs(1)).unwrap(),
            ReplicatorWireReply::WriteAck { from, message: ReplicatorWriteAck }
                if from == replica("remote")
        ));
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn remote_reply_inbound_delivers_read_results_to_addressed_aggregator() {
        let system = ActorSystem::builder("ddata-remote-reply-read")
            .build()
            .unwrap();
        let registry = registry();
        let inbound = ReplicatorRemoteReplyInbound::new(system.clone(), registry.clone());
        let (recipient, replies) = read_probe(&system);

        inbound
            .receive_message(
                replica("remote"),
                actor_ref(&recipient),
                registry
                    .serialize(&ReplicatorReadResult { envelope: None })
                    .unwrap(),
            )
            .unwrap();

        assert!(matches!(
            replies.recv_timeout(Duration::from_secs(1)).unwrap(),
            ReplicatorWireReply::ReadResult {
                from,
                message: ReplicatorReadResult { envelope: None },
            } if from == replica("remote")
        ));
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn remote_reply_inbound_reports_unknown_manifest_and_missing_actor() {
        let system = ActorSystem::builder("ddata-remote-reply-errors")
            .build()
            .unwrap();
        let registry = registry();
        let inbound = ReplicatorRemoteReplyInbound::new(system.clone(), registry.clone());
        let missing =
            ActorRefWireData::new("kairo://ddata-remote-reply-errors/user/missing#9").unwrap();

        let delivery_error = inbound
            .receive_message(
                replica("remote"),
                missing.clone(),
                registry.serialize(&ReplicatorWriteAck).unwrap(),
            )
            .expect_err("missing local aggregation actor should fail");
        assert!(matches!(
            delivery_error,
            ReplicatorRemoteReplyError::Send { .. }
        ));
        assert!(
            system
                .dead_letters()
                .wait_for_len(1, Duration::from_secs(1))
        );
        assert_eq!(
            system.dead_letters().records()[0].recipient().as_str(),
            missing.path()
        );

        let unsupported = inbound
            .receive_message(
                replica("remote"),
                missing,
                SerializedMessage::new(
                    REPLICATOR_READ_RESULT_SERIALIZER_ID,
                    Manifest::new(ReplicatorRead::MANIFEST),
                    ReplicatorRead::VERSION,
                    Bytes::new(),
                ),
            )
            .expect_err("request manifest is not a reply manifest");
        assert!(matches!(
            unsupported,
            ReplicatorRemoteReplyError::UnsupportedManifest(_)
        ));
        system.terminate(Duration::from_secs(1)).unwrap();
    }
}
