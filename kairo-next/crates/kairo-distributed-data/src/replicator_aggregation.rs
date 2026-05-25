use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, Context, Props};

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
            move || {
                WriteAggregationSession::new(plan, envelope, outcome, transport, timeout, reply_to)
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
        ctx.spawn_anonymous(Props::new({
            let transport = self.transport.clone();
            let data_codec = Arc::clone(&self.data_codec);
            move || ReadAggregationSession::new(plan, data_codec, transport, timeout, reply_to)
        }))
    }
}
