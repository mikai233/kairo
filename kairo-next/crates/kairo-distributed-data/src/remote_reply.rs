use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::ActorSystem;
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
    SerializedMessage,
};

use crate::{
    ReadAggregationActorMsg, ReplicaId, ReplicatorDeltaAck, ReplicatorDeltaNack,
    ReplicatorReadResult, ReplicatorWireReply, ReplicatorWriteAck, ReplicatorWriteNack,
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
        REPLICATOR_READ_RESULT_SERIALIZER_ID, ReplicatorRead, register_ddata_protocol_codecs,
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

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_ddata_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn replica(id: &str) -> ReplicaId {
        ReplicaId::new(id)
    }

    fn actor_ref<M>(actor: &ActorRef<M>) -> ActorRefWireData
    where
        M: Send + 'static,
    {
        ActorRefWireData::new(actor.path().to_string()).unwrap()
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
