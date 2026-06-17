use std::collections::BTreeSet;

use kairo_actor::ActorRef;
use kairo_cluster::{ClusterEvent, UniqueAddress};

use crate::{
    CurrentTopics, LocalPubSubMsg, PubSubDeliveryPlan, PubSubDeliveryReport,
    PubSubPathDeliveryMode, PubSubPathDeliveryPlan, PubSubPathDeliveryReport,
    PubSubPathRegistration, PubSubRegistryDelta, PubSubRegistryState, PubSubRemoteTarget,
    PubSubSubscribeAck, TopicName, TopicPublishMode,
};

pub enum DistributedPubSubMediatorMsg<M>
where
    M: Send + 'static,
{
    AddRemoteMediator {
        node: UniqueAddress,
        mediator: ActorRef<DistributedPubSubMediatorMsg<M>>,
    },
    AddRemoteTarget {
        target: PubSubRemoteTarget<M>,
    },
    RemoveRemoteMediator {
        node: UniqueAddress,
    },
    ApplyClusterEvent {
        event: ClusterEvent,
    },
    Subscribe {
        topic: TopicName,
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    SubscribeGroup {
        topic: TopicName,
        group: String,
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    Unsubscribe {
        topic: TopicName,
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    UnsubscribeGroup {
        topic: TopicName,
        group: String,
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    Put {
        actor: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubPathRegistration>>,
    },
    RemovePath {
        path: String,
        reply_to: Option<ActorRef<PubSubPathRegistration>>,
    },
    Publish {
        topic: TopicName,
        message: M,
        mode: TopicPublishMode,
        reply_to: Option<ActorRef<DistributedPubSubPublishReport>>,
    },
    Send {
        path: String,
        message: M,
        local_affinity: bool,
        reply_to: Option<ActorRef<DistributedPubSubSendReport>>,
    },
    SendToAll {
        path: String,
        message: M,
        all_but_self: bool,
        reply_to: Option<ActorRef<DistributedPubSubSendReport>>,
    },
    LocalDelivery(LocalPubSubMsg<M>),
    MergeDelta {
        delta: PubSubRegistryDelta,
    },
    PruneTombstones {
        retained_version_gap: u64,
    },
    GetRegistry {
        reply_to: ActorRef<PubSubRegistryState>,
    },
    GetTopics {
        reply_to: ActorRef<CurrentTopics>,
    },
    GetState {
        reply_to: ActorRef<DistributedPubSubSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedPubSubPublishReport {
    pub topic: TopicName,
    pub mode: TopicPublishMode,
    pub plan: PubSubDeliveryPlan,
    pub delivery: PubSubDeliveryReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedPubSubSendReport {
    pub path: String,
    pub mode: PubSubPathDeliveryMode,
    pub plan: PubSubPathDeliveryPlan,
    pub delivery: PubSubPathDeliveryReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedPubSubSnapshot {
    pub registry: PubSubRegistryState,
    pub current_topics: BTreeSet<TopicName>,
    pub remote_target_count: usize,
}
