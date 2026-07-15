use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use kairo_actor::{Actor, ActorResult, ActorSystem, Address, Context, ManualScheduler, Props};
use kairo_cluster::UniqueAddress;

use crate::{
    AggregationError, AggregationTarget, AggregationTransport, AggregationTransportFailure,
    AggregationTransportOperation, ConsistencyError, CrdtDataCodec, CrdtError, DataEnvelope,
    DeltaPropagationLog, DeltaPropagationLoop, DeltaPropagationSink, DeltaPropagationTarget,
    DeltaPropagationTickReport, DeltaPropagationTransport, DeltaReceiveFailure, DeltaReceiveReply,
    DeltaReceiveStatus, DeltaReceiveTracker, DeltaReplicatedData, DeltaTransportFailure,
    DeltaTransportReport, DirectReadResult, DirectWriteResult, GCounter, GCounterCodec, GSet,
    GSetStringCodec, GSetStringDeltaCodec, GetResponse, LWWRegister, LWWRegisterStringCodec, ORMap,
    ORMapStringGSetCodec, ORMapStringGSetDeltaCodec, ORSet, ORSetStringCodec,
    ORSetStringDeltaCodec, PNCounter, PNCounterCodec, PruningPerformed, PruningState, PruningTable,
    ReadAggregationOutcome, ReadAggregationPlan, ReadAggregatorState, ReadConsistency,
    RemovedNodePruning, RemovedNodePruningTick, RemovedNodePruningTickReport, ReplicaId,
    ReplicatedData, ReplicatedDelta, ReplicatorActor, ReplicatorActorMsg, ReplicatorAggregation,
    ReplicatorClusterRouteReport, ReplicatorClusterRouteUpdate, ReplicatorDeltaPropagation,
    ReplicatorGossip, ReplicatorGossipReceiveReport, ReplicatorGossipStatus,
    ReplicatorGossipStatusReceiveReport, ReplicatorGossipTarget, ReplicatorGossipTickReport,
    ReplicatorGossipTransport, ReplicatorKey, ReplicatorState, UpdateResponse,
    WriteAggregationOutcome, WriteAggregationPlan, WriteAggregatorState, WriteConsistency,
    calculate_majority, decode_data_envelope, decode_delta_propagation, decode_read_result,
    encode_data_envelope, encode_delta_propagation, encode_read, encode_read_result, encode_write,
};

mod aggregation_core;
mod aggregation_transport;
mod aggregation_wire;
mod crdt_codecs;
mod crdt_foundation;
mod delta_log;
mod delta_receive_tracker;
mod delta_transport;
mod delta_wire;
mod direct_receive;
mod replicator_actor_client;
mod replicator_actor_delta_loop;
mod replicator_actor_delta_receive;
mod replicator_actor_gossip;
mod replicator_actor_planning;
mod replicator_actor_pruning;
mod replicator_actor_remote_receive;
mod replicator_state;

fn replica(id: &str) -> ReplicaId {
    ReplicaId::new(id)
}

fn delta_counter(id: &str, amount: u128) -> GCounter {
    GCounter::new()
        .increment(replica(id), amount)
        .unwrap()
        .delta()
        .unwrap()
}

fn full_counter(id: &str, amount: u128) -> GCounter {
    delta_counter(id, amount).reset_delta()
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

fn forward_ref<M>(system: &ActorSystem, name: &str) -> (kairo_actor::ActorRef<M>, mpsc::Receiver<M>)
where
    M: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let actor = system
        .spawn(name, Props::new(move || Forward { tx }))
        .expect("forward actor should spawn");
    (actor, rx)
}
