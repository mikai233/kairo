use std::collections::BTreeSet;

use kairo_actor::ActorRef;
use kairo_cluster::{ClusterEvent, UniqueAddress};

use crate::{
    CurrentTopics, LocalPubSubMsg, PubSubDeliveryPlan, PubSubDeliveryReport,
    PubSubPathDeliveryMode, PubSubPathDeliveryPlan, PubSubPathDeliveryReport,
    PubSubPathRegistration, PubSubRegistryDelta, PubSubRegistryState, PubSubRemoteTarget,
    PubSubSubscribeAck, TopicName, TopicPublishMode,
};

/// Typed command protocol for [`crate::DistributedPubSubMediatorActor`].
///
/// User-facing subscription, publication, path-delivery, and query commands
/// coexist with explicit adapter commands for membership, gossip, and remote
/// ingress. Business messages remain typed as `M`; this enum is never a wire
/// contract.
pub enum DistributedPubSubMediatorMsg<M>
where
    M: Send + 'static,
{
    /// Installs an in-process typed mediator route for an exact member.
    ///
    /// This is useful for deterministic tests. Transport-backed runtimes use
    /// [`Self::AddRemoteTarget`] instead.
    AddRemoteMediator {
        /// Exact member incarnation associated with the route.
        node: UniqueAddress,
        /// Typed mediator actor that accepts forwarded local-delivery commands.
        mediator: ActorRef<DistributedPubSubMediatorMsg<M>>,
    },
    /// Installs a typed transport-backed route for one exact member.
    AddRemoteTarget {
        /// Exact-incarnation recipient installed in the delivery table.
        target: PubSubRemoteTarget<M>,
    },
    /// Removes one exact member's route and registry bucket.
    RemoveRemoteMediator {
        /// Exact member incarnation to remove.
        node: UniqueAddress,
    },
    /// Applies an authoritative cluster-domain event.
    ///
    /// Left, downed, and removed peers are pruned. Removal of this mediator's
    /// own address stops the actor. Reachability alone does not rewrite pubsub
    /// membership or registry truth.
    ApplyClusterEvent {
        /// Cluster event emitted by the composed cluster extension.
        event: ClusterEvent,
    },
    /// Adds a local subscriber to a broadcast topic.
    Subscribe {
        /// Topic to subscribe to.
        topic: TopicName,
        /// Local typed subscriber.
        subscriber: ActorRef<M>,
        /// Optional acknowledgement recipient.
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    /// Adds a local subscriber to a named topic group.
    SubscribeGroup {
        /// Topic to subscribe to.
        topic: TopicName,
        /// Group in which one subscriber receives each grouped publication.
        group: String,
        /// Local typed subscriber.
        subscriber: ActorRef<M>,
        /// Optional acknowledgement recipient.
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    /// Removes a local subscriber from a broadcast topic.
    Unsubscribe {
        /// Topic to unsubscribe from.
        topic: TopicName,
        /// Local typed subscriber to remove.
        subscriber: ActorRef<M>,
        /// Optional acknowledgement recipient.
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    /// Removes a local subscriber from one named topic group.
    UnsubscribeGroup {
        /// Topic to unsubscribe from.
        topic: TopicName,
        /// Group to unsubscribe from.
        group: String,
        /// Local typed subscriber to remove.
        subscriber: ActorRef<M>,
        /// Optional acknowledgement recipient.
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    /// Registers a local actor under its logical actor path.
    Put {
        /// Local typed actor to register and watch.
        actor: ActorRef<M>,
        /// Optional registration result recipient.
        reply_to: Option<ActorRef<PubSubPathRegistration>>,
    },
    /// Removes the local actor registered under a logical path.
    RemovePath {
        /// Logical actor path to remove.
        path: String,
        /// Optional registration result recipient.
        reply_to: Option<ActorRef<PubSubPathRegistration>>,
    },
    /// Publishes a typed business message using the converged registry view.
    Publish {
        /// Topic to publish to.
        topic: TopicName,
        /// Typed business message.
        message: M,
        /// Broadcast or one-per-group delivery mode.
        mode: TopicPublishMode,
        /// Optional plan and delivery report recipient.
        reply_to: Option<ActorRef<DistributedPubSubPublishReport>>,
    },
    /// Sends a typed business message to one logical-path registration.
    Send {
        /// Logical actor path to resolve.
        path: String,
        /// Typed business message.
        message: M,
        /// Prefer a local registration when one exists.
        local_affinity: bool,
        /// Optional plan and delivery report recipient.
        reply_to: Option<ActorRef<DistributedPubSubSendReport>>,
    },
    /// Sends a typed business message to every logical-path registration.
    SendToAll {
        /// Logical actor path to resolve.
        path: String,
        /// Typed business message cloned for each selected mediator.
        message: M,
        /// Exclude this node's local registration from the plan.
        all_but_self: bool,
        /// Optional plan and delivery report recipient.
        reply_to: Option<ActorRef<DistributedPubSubSendReport>>,
    },
    /// Executes one command against local state without distributed planning.
    ///
    /// This adapter command is used by validated remote ingress and mediator
    /// route bridges so a remote hop cannot recursively fan out across peers.
    LocalDelivery(LocalPubSubMsg<M>),
    /// Merges a registry delta already accepted by the gossip peer filter.
    ///
    /// Direct callers must not use this to bypass known-member validation.
    MergeDelta {
        /// Versioned registry delta to merge.
        delta: PubSubRegistryDelta,
    },
    /// Drops sufficiently old tombstones from retained registry buckets.
    PruneTombstones {
        /// Number of owner-version increments a tombstone remains protected.
        retained_version_gap: u64,
    },
    /// Replies with a clone of the current distributed registry view.
    GetRegistry {
        /// Recipient for the registry snapshot.
        reply_to: ActorRef<PubSubRegistryState>,
    },
    /// Replies with the names of topics that currently have local subscribers.
    GetTopics {
        /// Recipient for the local topic-name set.
        reply_to: ActorRef<CurrentTopics>,
    },
    /// Replies with an operator-facing mediator snapshot.
    GetState {
        /// Recipient for the mediator snapshot.
        reply_to: ActorRef<DistributedPubSubSnapshot>,
    },
}

/// Plan and per-target outcome of one distributed topic publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedPubSubPublishReport {
    /// Topic supplied by the publisher.
    pub topic: TopicName,
    /// Requested broadcast or one-per-group mode.
    pub mode: TopicPublishMode,
    /// Immutable target plan derived from the registry snapshot.
    pub plan: PubSubDeliveryPlan,
    /// Per-target result of executing the plan.
    pub delivery: PubSubDeliveryReport,
}

/// Plan and per-target outcome of one distributed logical-path send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedPubSubSendReport {
    /// Logical actor path supplied by the sender.
    pub path: String,
    /// Requested one-target or all-target mode.
    pub mode: PubSubPathDeliveryMode,
    /// Immutable target plan derived from the registry snapshot.
    pub plan: PubSubPathDeliveryPlan,
    /// Per-target result of executing the plan.
    pub delivery: PubSubPathDeliveryReport,
}

/// Operator-facing snapshot of mediator registry, local topics, and routes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedPubSubSnapshot {
    /// Current merged registry, including remote buckets and tombstones.
    pub registry: PubSubRegistryState,
    /// Topics with at least one current local subscriber.
    pub current_topics: BTreeSet<TopicName>,
    /// Number of exact-incarnation remote delivery routes currently installed.
    pub remote_target_count: usize,
}
