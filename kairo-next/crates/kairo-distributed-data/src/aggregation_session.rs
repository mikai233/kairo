#![deny(missing_docs)]
//! Actor sessions that publish quorum operations and map completion to clients.
//!
//! A session spawns one typed aggregation actor, publishes to primary replicas
//! immediately, and schedules its secondary replicas after one fifth of the
//! consistency timeout. The child aggregation actor owns the final deadline;
//! the session owns transport publication, stable sender identity, response
//! mapping, and the read-repair handshake with its parent replicator.

use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, Props};
use kairo_remote::{CanonicalLocalAddress, RemoteSettings};
use kairo_serialization::ActorRefWireData;

use crate::{
    AggregationTransport, AggregationTransportReport, CrdtDataCodec, DataEnvelope,
    DeltaReplicatedData, GetResponse, ReadAggregationActor, ReadAggregationActorEvent,
    ReadAggregationActorMsg, ReadAggregationOperationEvent, ReadAggregationOutcome,
    ReadAggregationPlan, ReplicaId, ReplicatedDelta, ReplicatorKey, UpdateOutcome, UpdateResponse,
    WriteAggregationActor, WriteAggregationActorEvent, WriteAggregationActorMsg,
    WriteAggregationOutcome, WriteAggregationPlan,
};

#[derive(Debug, Clone)]
/// Diagnostic event emitted by a write aggregation session.
pub enum WriteAggregationSessionEvent {
    /// The child aggregator was created and primary publication was attempted.
    Started {
        /// Child actor that accepts addressed remote replies.
        reply_to: ActorRef<WriteAggregationActorMsg>,
        /// Primary publication result.
        report: AggregationTransportReport,
    },
    /// One delta NACK triggered an immediate full-state retry.
    RetryFullState {
        /// Key whose full state was retried.
        key: ReplicatorKey,
        /// Replica that rejected the causal delta.
        replica: ReplicaId,
        /// Full-state retry publication result.
        report: AggregationTransportReport,
    },
    /// Delayed full-state publication to the selected secondary replicas ran.
    SecondaryPublished {
        /// Secondary publication result.
        report: AggregationTransportReport,
    },
    /// The child aggregator reached a terminal write outcome.
    Completed(WriteAggregationOutcome),
}

/// Internal and externally observable protocol of a write aggregation session.
pub enum WriteAggregationSessionMsg {
    /// Event adapted from the child aggregation actor.
    Aggregation(WriteAggregationActorEvent),
    /// Timer signal that publishes full state to delayed secondary replicas.
    SendToSecondary,
}

/// Coordinates one remote write quorum from publication through client reply.
///
/// Primary replicas receive full state during actor startup. If the operation
/// is still alive, selected secondary replicas receive the same full state at
/// one fifth of `timeout`. The session stops after mapping a terminal child
/// outcome into [`UpdateResponse`].
pub struct WriteAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    plan: WriteAggregationPlan,
    envelope: DataEnvelope<D>,
    outcome: Option<UpdateOutcome<D::Delta>>,
    transport: AggregationTransport<Codec>,
    timeout: Duration,
    reply_to: ActorRef<UpdateResponse<D::Delta>>,
    events: Option<ActorRef<WriteAggregationSessionEvent>>,
    sender: Option<ActorRefWireData>,
    sender_settings: Option<RemoteSettings>,
}

impl<D, Codec> WriteAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    /// Creates a write session without diagnostic event publication.
    pub fn new(
        plan: WriteAggregationPlan,
        envelope: DataEnvelope<D>,
        outcome: UpdateOutcome<D::Delta>,
        transport: AggregationTransport<Codec>,
        timeout: Duration,
        reply_to: ActorRef<UpdateResponse<D::Delta>>,
    ) -> Self {
        Self {
            plan,
            envelope,
            outcome: Some(outcome),
            transport,
            timeout,
            reply_to,
            events: None,
            sender: None,
            sender_settings: None,
        }
    }

    /// Creates a write session that publishes lifecycle diagnostics to `events`.
    pub fn with_events(
        plan: WriteAggregationPlan,
        envelope: DataEnvelope<D>,
        outcome: UpdateOutcome<D::Delta>,
        transport: AggregationTransport<Codec>,
        timeout: Duration,
        reply_to: ActorRef<UpdateResponse<D::Delta>>,
        events: ActorRef<WriteAggregationSessionEvent>,
    ) -> Self {
        Self {
            plan,
            envelope,
            outcome: Some(outcome),
            transport,
            timeout,
            reply_to,
            events: Some(events),
            sender: None,
            sender_settings: None,
        }
    }

    /// Uses canonical remote sender paths derived from `settings`.
    ///
    /// Without this setting, child aggregator refs use their local actor paths.
    pub fn with_sender_remote_settings(mut self, settings: RemoteSettings) -> Self {
        self.sender_settings = Some(settings);
        self
    }
}

impl<D, Codec> Actor for WriteAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: ReplicatedDelta + Send + 'static,
    Codec: CrdtDataCodec<D> + Clone + Send + 'static,
{
    type Msg = WriteAggregationSessionMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let events = ctx.message_adapter(WriteAggregationSessionMsg::Aggregation)?;
        let aggregator = ctx.spawn_anonymous(Props::new({
            let plan = self.plan.clone();
            let timeout = self.timeout;
            move || WriteAggregationActor::with_timeout(plan, timeout, events)
        }))?;
        let sender = actor_ref_wire_data(&aggregator, ctx.system(), self.sender_settings.as_ref())?;
        self.sender = Some(sender.clone());
        let report = self
            .transport
            .publish_write_with_sender(&self.plan, &self.envelope, &sender);
        if !self.plan.selection().secondary().is_empty() {
            ctx.schedule_once_self(
                self.timeout / 5,
                WriteAggregationSessionMsg::SendToSecondary,
            );
        }
        self.emit(WriteAggregationSessionEvent::Started {
            reply_to: aggregator,
            report,
        })
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            WriteAggregationSessionMsg::Aggregation(event) => self.receive_event(ctx, event),
            WriteAggregationSessionMsg::SendToSecondary => {
                let report = if let Some(sender) = &self.sender {
                    self.transport.publish_write_to_replicas_with_sender(
                        self.plan.selection().secondary(),
                        &self.plan,
                        &self.envelope,
                        sender,
                    )
                } else {
                    self.transport
                        .publish_write_to_secondary(&self.plan, &self.envelope)
                };
                self.emit(WriteAggregationSessionEvent::SecondaryPublished { report })
            }
        }
    }
}

impl<D, Codec> WriteAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: ReplicatedDelta + Send + 'static,
    Codec: CrdtDataCodec<D> + Clone + Send + 'static,
{
    fn receive_event(
        &mut self,
        ctx: &mut Context<WriteAggregationSessionMsg>,
        event: WriteAggregationActorEvent,
    ) -> ActorResult {
        match event {
            WriteAggregationActorEvent::RetryFullState { replica } => {
                let report = if let Some(sender) = &self.sender {
                    self.transport.publish_write_to_replicas_with_sender(
                        std::slice::from_ref(&replica),
                        &self.plan,
                        &self.envelope,
                        sender,
                    )
                } else {
                    self.transport.publish_write_to_replicas(
                        std::slice::from_ref(&replica),
                        &self.plan,
                        &self.envelope,
                    )
                };
                self.emit(WriteAggregationSessionEvent::RetryFullState {
                    key: self.plan.state().key().clone(),
                    replica,
                    report,
                })
            }
            WriteAggregationActorEvent::Completed(outcome) => {
                self.emit(WriteAggregationSessionEvent::Completed(outcome.clone()))?;
                let response = crate::aggregation_operation::write_aggregation_response(
                    self.plan.state().key(),
                    &mut self.outcome,
                    outcome,
                );
                tell_or_actor_error(&self.reply_to, response)?;
                ctx.stop(ctx.myself())
            }
        }
    }

    fn emit(&self, event: WriteAggregationSessionEvent) -> ActorResult {
        if let Some(events) = &self.events {
            tell_or_actor_error(events, event)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
/// Diagnostic event emitted by a read aggregation session.
pub enum ReadAggregationSessionEvent {
    /// The child aggregator was created and primary publication was attempted.
    Started {
        /// Child actor that accepts addressed remote replies.
        reply_to: ActorRef<ReadAggregationActorMsg>,
        /// Primary publication result.
        report: AggregationTransportReport,
    },
    /// One remote read result could not be decoded and was not counted.
    DecodeFailed(ReadAggregationOperationEvent),
    /// Delayed publication to the selected secondary replicas ran.
    SecondaryPublished {
        /// Secondary publication result.
        report: AggregationTransportReport,
    },
    /// The child aggregator and any configured read repair completed.
    Completed(ReadAggregationSessionOutcome),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Data-independent diagnostic form of a read aggregation outcome.
pub enum ReadAggregationSessionOutcome {
    /// More remote replies would be required.
    InProgress,
    /// A merged value reached quorum and any configured repair completed.
    Success,
    /// The quorum completed without finding the key.
    NotFound,
    /// The quorum could not complete before its deadline.
    Failure {
        /// Number of remote results required.
        required: usize,
        /// Number of distinct accepted results received.
        received: usize,
    },
}

impl<D> From<&ReadAggregationOutcome<D>> for ReadAggregationSessionOutcome {
    fn from(value: &ReadAggregationOutcome<D>) -> Self {
        match value {
            ReadAggregationOutcome::InProgress => Self::InProgress,
            ReadAggregationOutcome::Success { .. } => Self::Success,
            ReadAggregationOutcome::NotFound => Self::NotFound,
            ReadAggregationOutcome::Failure { required, received } => Self::Failure {
                required: *required,
                received: *received,
            },
        }
    }
}

/// Internal and externally observable protocol of a read aggregation session.
pub enum ReadAggregationSessionMsg<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    /// Event adapted from the child aggregation actor.
    Aggregation(ReadAggregationActorEvent<D>),
    /// Timer signal that publishes reads to delayed secondary replicas.
    SendToSecondary,
    /// Parent acknowledgement for a pending read repair.
    ///
    /// The signal is ignored unless the session is awaiting a repair.
    ReadRepairApplied,
}

pub(crate) struct ReadRepairRequest<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    pub(crate) key: ReplicatorKey,
    pub(crate) envelope: DataEnvelope<D>,
    pub(crate) reply_to: ActorRef<()>,
}

/// Coordinates one remote read quorum from publication through client reply.
///
/// Primary replicas are queried during startup and selected secondary replicas
/// after one fifth of `timeout`. Sessions composed by [`crate::ReplicatorAggregation`]
/// repair their parent replicator before returning a successful merged value;
/// directly constructed sessions have no parent repair target and return the
/// merged result immediately.
pub struct ReadAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
{
    key: ReplicatorKey,
    plan: ReadAggregationPlan<D>,
    data_codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
    transport: AggregationTransport<Codec>,
    timeout: Duration,
    reply_to: ActorRef<GetResponse<D>>,
    events: Option<ActorRef<ReadAggregationSessionEvent>>,
    read_repair: Option<ActorRef<ReadRepairRequest<D>>>,
    pending_read_repair: Option<DataEnvelope<D>>,
    sender: Option<ActorRefWireData>,
    sender_settings: Option<RemoteSettings>,
}

impl<D, Codec> ReadAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
{
    /// Creates a read session without diagnostic event publication.
    pub fn new(
        plan: ReadAggregationPlan<D>,
        data_codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
        transport: AggregationTransport<Codec>,
        timeout: Duration,
        reply_to: ActorRef<GetResponse<D>>,
    ) -> Self {
        Self {
            key: plan.state().key().clone(),
            plan,
            data_codec,
            transport,
            timeout,
            reply_to,
            events: None,
            read_repair: None,
            pending_read_repair: None,
            sender: None,
            sender_settings: None,
        }
    }

    /// Creates a read session that publishes lifecycle diagnostics to `events`.
    pub fn with_events(
        plan: ReadAggregationPlan<D>,
        data_codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
        transport: AggregationTransport<Codec>,
        timeout: Duration,
        reply_to: ActorRef<GetResponse<D>>,
        events: ActorRef<ReadAggregationSessionEvent>,
    ) -> Self {
        Self {
            key: plan.state().key().clone(),
            plan,
            data_codec,
            transport,
            timeout,
            reply_to,
            events: Some(events),
            read_repair: None,
            pending_read_repair: None,
            sender: None,
            sender_settings: None,
        }
    }

    /// Uses canonical remote sender paths derived from `settings`.
    ///
    /// Without this setting, child aggregator refs use their local actor paths.
    pub fn with_sender_remote_settings(mut self, settings: RemoteSettings) -> Self {
        self.sender_settings = Some(settings);
        self
    }

    pub(crate) fn with_read_repair(mut self, read_repair: ActorRef<ReadRepairRequest<D>>) -> Self {
        self.read_repair = Some(read_repair);
        self
    }
}

impl<D, Codec> Actor for ReadAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    Codec: Clone + Send + 'static,
{
    type Msg = ReadAggregationSessionMsg<D>;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let events = ctx.message_adapter(ReadAggregationSessionMsg::Aggregation)?;
        let aggregator = ctx.spawn_anonymous(Props::new({
            let plan = self.plan.clone();
            let codec = Arc::clone(&self.data_codec);
            let timeout = self.timeout;
            move || ReadAggregationActor::with_timeout(plan, codec, timeout, events)
        }))?;
        let sender = actor_ref_wire_data(&aggregator, ctx.system(), self.sender_settings.as_ref())?;
        self.sender = Some(sender.clone());
        let report = self.transport.publish_read_with_sender(&self.plan, &sender);
        if !self.plan.selection().secondary().is_empty() {
            ctx.schedule_once_self(self.timeout / 5, ReadAggregationSessionMsg::SendToSecondary);
        }
        self.emit(ReadAggregationSessionEvent::Started {
            reply_to: aggregator,
            report,
        })
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ReadAggregationSessionMsg::Aggregation(event) => self.receive_event(ctx, event),
            ReadAggregationSessionMsg::SendToSecondary => {
                let report = if let Some(sender) = &self.sender {
                    self.transport.publish_read_to_replicas_with_sender(
                        self.plan.selection().secondary(),
                        &self.plan,
                        sender,
                    )
                } else {
                    self.transport.publish_read_to_secondary(&self.plan)
                };
                self.emit(ReadAggregationSessionEvent::SecondaryPublished { report })
            }
            ReadAggregationSessionMsg::ReadRepairApplied => {
                let Some(envelope) = self.pending_read_repair.take() else {
                    return Ok(());
                };
                self.complete(ctx, ReadAggregationOutcome::Success { envelope })
            }
        }
    }
}

impl<D, Codec> ReadAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    Codec: Clone + Send + 'static,
{
    fn receive_event(
        &mut self,
        ctx: &mut Context<ReadAggregationSessionMsg<D>>,
        event: ReadAggregationActorEvent<D>,
    ) -> ActorResult {
        match event {
            ReadAggregationActorEvent::DecodeFailed { replica, reason } => {
                self.emit(ReadAggregationSessionEvent::DecodeFailed(
                    ReadAggregationOperationEvent::DecodeFailed {
                        key: self.key.clone(),
                        replica,
                        reason,
                    },
                ))
            }
            ReadAggregationActorEvent::Completed(ReadAggregationOutcome::Success { envelope }) => {
                match &self.read_repair {
                    Some(read_repair) => {
                        self.pending_read_repair = Some(envelope.clone());
                        let reply_to = ctx.message_adapter(move |()| {
                            ReadAggregationSessionMsg::ReadRepairApplied
                        })?;
                        tell_or_actor_error(
                            read_repair,
                            ReadRepairRequest {
                                key: self.key.clone(),
                                envelope,
                                reply_to,
                            },
                        )
                    }
                    None => self.complete(ctx, ReadAggregationOutcome::Success { envelope }),
                }
            }
            ReadAggregationActorEvent::Completed(outcome) => self.complete(ctx, outcome),
        }
    }

    fn complete(
        &self,
        ctx: &mut Context<ReadAggregationSessionMsg<D>>,
        outcome: ReadAggregationOutcome<D>,
    ) -> ActorResult {
        self.emit(ReadAggregationSessionEvent::Completed(
            ReadAggregationSessionOutcome::from(&outcome),
        ))?;
        let response = crate::aggregation_operation::read_aggregation_response(&self.key, outcome);
        tell_or_actor_error(&self.reply_to, response)?;
        ctx.stop(ctx.myself())
    }

    fn emit(&self, event: ReadAggregationSessionEvent) -> ActorResult {
        if let Some(events) = &self.events {
            tell_or_actor_error(events, event)?;
        }
        Ok(())
    }
}

fn tell_or_actor_error<M>(target: &ActorRef<M>, message: M) -> ActorResult
where
    M: Send + 'static,
{
    target
        .tell(message)
        .map_err(|error| ActorError::Message(error.reason().to_string()))
}

fn actor_ref_wire_data<M>(
    actor: &ActorRef<M>,
    system: &ActorSystem,
    settings: Option<&RemoteSettings>,
) -> Result<ActorRefWireData, ActorError>
where
    M: Send + 'static,
{
    let path = match settings {
        Some(settings) => {
            let canonical_address =
                CanonicalLocalAddress::from_system_settings(system, settings.clone());
            canonical_address
                .canonical_recipient_path(actor.path().as_str())
                .ok_or_else(|| {
                    ActorError::Message(format!(
                        "failed to encode aggregation reply actor ref {}: actor ref is not owned by this actor system",
                        actor.path()
                    ))
                })?
        }
        None => actor.path().to_string(),
    };
    ActorRefWireData::new(path).map_err(|error| {
        ActorError::Message(format!(
            "failed to encode aggregation reply actor ref {}: {error}",
            actor.path()
        ))
    })
}

#[cfg(test)]
mod tests;
