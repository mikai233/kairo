use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, Context, Props};
use kairo_remote::RemoteSettings;

use crate::aggregation_session::ReadRepairRequest;
use crate::{
    AggregationTransport, CrdtDataCodec, DataEnvelope, DeltaReplicatedData, GetResponse,
    ReadAggregationPlan, ReadAggregationSession, ReadAggregationSessionMsg, ReplicatedDelta,
    ReplicatorActorMsg, UpdateOutcome, UpdateResponse, WriteAggregationPlan,
    WriteAggregationSession, WriteAggregationSessionMsg,
};

pub struct ReplicatorAggregation<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: ReplicatedDelta + Send + 'static,
{
    spawner: Arc<dyn ReplicatorAggregationSpawner<D>>,
}

impl<D> Clone for ReplicatorAggregation<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: ReplicatedDelta + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            spawner: Arc::clone(&self.spawner),
        }
    }
}

impl<D> ReplicatorAggregation<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: ReplicatedDelta + Send + 'static,
{
    pub fn new<Codec>(
        transport: AggregationTransport<Codec>,
        data_codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
    ) -> Self
    where
        Codec: CrdtDataCodec<D> + Clone + Send + Sync + 'static,
    {
        Self {
            spawner: Arc::new(SessionSpawner {
                transport,
                data_codec,
                sender_settings: None,
                _data: PhantomData,
            }),
        }
    }

    pub fn with_sender_remote_settings<Codec>(
        transport: AggregationTransport<Codec>,
        data_codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
        sender_settings: RemoteSettings,
    ) -> Self
    where
        Codec: CrdtDataCodec<D> + Clone + Send + Sync + 'static,
    {
        Self {
            spawner: Arc::new(SessionSpawner {
                transport,
                data_codec,
                sender_settings: Some(sender_settings),
                _data: PhantomData,
            }),
        }
    }

    pub(crate) fn spawn_write(
        &self,
        ctx: &Context<ReplicatorActorMsg<D>>,
        plan: WriteAggregationPlan,
        envelope: DataEnvelope<D>,
        outcome: UpdateOutcome<D::Delta>,
        timeout: Duration,
        reply_to: ActorRef<UpdateResponse<D::Delta>>,
    ) -> Result<ActorRef<WriteAggregationSessionMsg>, ActorError> {
        self.spawner
            .spawn_write(ctx, plan, envelope, outcome, timeout, reply_to)
    }

    pub(crate) fn spawn_read(
        &self,
        ctx: &Context<ReplicatorActorMsg<D>>,
        plan: ReadAggregationPlan<D>,
        timeout: Duration,
        reply_to: ActorRef<GetResponse<D>>,
    ) -> Result<ActorRef<ReadAggregationSessionMsg<D>>, ActorError> {
        self.spawner.spawn_read(ctx, plan, timeout, reply_to)
    }
}

pub(crate) trait ReplicatorAggregationSpawner<D>: Send + Sync
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: ReplicatedDelta + Send + 'static,
{
    fn spawn_write(
        &self,
        ctx: &Context<ReplicatorActorMsg<D>>,
        plan: WriteAggregationPlan,
        envelope: DataEnvelope<D>,
        outcome: UpdateOutcome<D::Delta>,
        timeout: Duration,
        reply_to: ActorRef<UpdateResponse<D::Delta>>,
    ) -> Result<ActorRef<WriteAggregationSessionMsg>, ActorError>;

    fn spawn_read(
        &self,
        ctx: &Context<ReplicatorActorMsg<D>>,
        plan: ReadAggregationPlan<D>,
        timeout: Duration,
        reply_to: ActorRef<GetResponse<D>>,
    ) -> Result<ActorRef<ReadAggregationSessionMsg<D>>, ActorError>;
}

struct SessionSpawner<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
{
    transport: AggregationTransport<Codec>,
    data_codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
    sender_settings: Option<RemoteSettings>,
    _data: PhantomData<fn(D)>,
}

impl<D, Codec> ReplicatorAggregationSpawner<D> for SessionSpawner<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: ReplicatedDelta + Send + 'static,
    Codec: CrdtDataCodec<D> + Clone + Send + Sync + 'static,
{
    fn spawn_write(
        &self,
        ctx: &Context<ReplicatorActorMsg<D>>,
        plan: WriteAggregationPlan,
        envelope: DataEnvelope<D>,
        outcome: UpdateOutcome<D::Delta>,
        timeout: Duration,
        reply_to: ActorRef<UpdateResponse<D::Delta>>,
    ) -> Result<ActorRef<WriteAggregationSessionMsg>, ActorError> {
        ctx.spawn_anonymous(Props::new({
            let transport = self.transport.clone();
            let sender_settings = self.sender_settings.clone();
            move || {
                let session = WriteAggregationSession::new(
                    plan, envelope, outcome, transport, timeout, reply_to,
                );
                match sender_settings {
                    Some(settings) => session.with_sender_remote_settings(settings),
                    None => session,
                }
            }
        }))
    }

    fn spawn_read(
        &self,
        ctx: &Context<ReplicatorActorMsg<D>>,
        plan: ReadAggregationPlan<D>,
        timeout: Duration,
        reply_to: ActorRef<GetResponse<D>>,
    ) -> Result<ActorRef<ReadAggregationSessionMsg<D>>, ActorError> {
        let read_repair = ctx.message_adapter(|request: ReadRepairRequest<D>| {
            ReplicatorActorMsg::ApplyReadRepair {
                key: request.key,
                envelope: request.envelope,
                reply_to: request.reply_to,
            }
        })?;
        ctx.spawn_anonymous(Props::new({
            let transport = self.transport.clone();
            let data_codec = Arc::clone(&self.data_codec);
            let sender_settings = self.sender_settings.clone();
            move || {
                let session =
                    ReadAggregationSession::new(plan, data_codec, transport, timeout, reply_to)
                        .with_read_repair(read_repair);
                match sender_settings {
                    Some(settings) => session.with_sender_remote_settings(settings),
                    None => session,
                }
            }
        }))
    }
}
