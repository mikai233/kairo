#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::sync::Arc;

use kairo_actor::Recipient;
use kairo_cluster::UniqueAddress;

use crate::{LocalPubSubMsg, PubSubRegistryState, TopicName, TopicPublishMode};

type PubSubRecipient<M> = Arc<dyn Recipient<LocalPubSubMsg<M>> + Send + Sync>;

/// One mediator target selected for a topic publication.
///
/// A target names the mediator that must perform the final local fan-out. It
/// does not identify an individual subscriber: broadcast and group routing
/// remain local-mediator responsibilities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubDeliveryTarget {
    /// Deliver a broadcast publication through this node's local mediator.
    LocalTopic,
    /// Deliver a broadcast publication through another node's mediator.
    RemoteTopic {
        /// Exact cluster-member incarnation hosting the mediator.
        node: UniqueAddress,
    },
    /// Deliver one publication to a local subscriber in the named group.
    LocalGroup {
        /// Subscription group selected for delivery.
        group: String,
    },
    /// Deliver one publication to a subscriber group on another node.
    RemoteGroup {
        /// Subscription group selected for delivery.
        group: String,
        /// Exact cluster-member incarnation selected for the group.
        node: UniqueAddress,
    },
}

/// One mediator target selected for logical actor-path delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubPathDeliveryTarget {
    /// Deliver through this node's local mediator.
    LocalPath,
    /// Deliver through another node's mediator.
    RemotePath {
        /// Exact cluster-member incarnation hosting the mediator.
        node: UniqueAddress,
    },
}

/// Immutable topic-publication plan derived from a registry snapshot.
///
/// The plan separates registry selection from transport delivery so callers
/// can inspect, test, or defer a deterministic routing decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubDeliveryPlan {
    /// Topic to publish to.
    pub topic: TopicName,
    /// Subscriber fan-out mode requested by the publisher.
    pub mode: TopicPublishMode,
    /// Ordered local and remote mediator targets selected from the registry.
    pub targets: Vec<PubSubDeliveryTarget>,
}

/// Routing mode for a logical actor-path delivery plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubSubPathDeliveryMode {
    /// Select exactly one registered mediator when any target exists.
    One {
        /// Prefer the local mediator whenever the path is registered locally.
        local_affinity: bool,
    },
    /// Select every registered mediator, optionally excluding this node.
    All {
        /// Exclude the local mediator from the selected targets.
        all_but_self: bool,
    },
}

/// Immutable logical actor-path delivery plan derived from a registry snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubPathDeliveryPlan {
    /// Logical actor path registered with the mediators.
    pub path: String,
    /// One-target or all-target routing mode.
    pub mode: PubSubPathDeliveryMode,
    /// Ordered local and remote mediator targets selected from the registry.
    pub targets: Vec<PubSubPathDeliveryTarget>,
}

impl PubSubDeliveryPlan {
    /// Builds a topic plan from the currently known registry state.
    ///
    /// Broadcast selects every node with the topic. One-per-group selects the
    /// registry's deterministic lowest node for each distinct group. The plan
    /// is empty when the registry contains no matching subscriptions.
    pub fn for_registry(
        registry: &PubSubRegistryState,
        topic: TopicName,
        mode: TopicPublishMode,
    ) -> Self {
        let targets = match mode {
            TopicPublishMode::Broadcast => registry
                .broadcast_targets(&topic, true)
                .into_iter()
                .map(|node| {
                    if &node == registry.self_node() {
                        PubSubDeliveryTarget::LocalTopic
                    } else {
                        PubSubDeliveryTarget::RemoteTopic { node }
                    }
                })
                .collect(),
            TopicPublishMode::OnePerGroup => registry
                .one_per_group_targets(&topic)
                .into_iter()
                .map(|(group, node)| {
                    if &node == registry.self_node() {
                        PubSubDeliveryTarget::LocalGroup { group }
                    } else {
                        PubSubDeliveryTarget::RemoteGroup { group, node }
                    }
                })
                .collect(),
        };

        Self {
            topic,
            mode,
            targets,
        }
    }

    /// Returns the distinct remote member incarnations in target order.
    pub fn remote_nodes(&self) -> Vec<UniqueAddress> {
        let mut nodes = Vec::new();
        for target in &self.targets {
            let node = match target {
                PubSubDeliveryTarget::LocalTopic | PubSubDeliveryTarget::LocalGroup { .. } => {
                    continue;
                }
                PubSubDeliveryTarget::RemoteTopic { node }
                | PubSubDeliveryTarget::RemoteGroup { node, .. } => node,
            };
            if !nodes.contains(node) {
                nodes.push(node.clone());
            }
        }
        nodes
    }

    /// Returns whether this plan includes any local mediator delivery.
    pub fn has_local_target(&self) -> bool {
        self.targets.iter().any(|target| {
            matches!(
                target,
                PubSubDeliveryTarget::LocalTopic | PubSubDeliveryTarget::LocalGroup { .. }
            )
        })
    }

    /// Returns whether this plan has no delivery targets.
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

impl PubSubPathDeliveryPlan {
    /// Builds a plan that sends to one mediator registered for `path`.
    ///
    /// With local affinity, a local registration wins. Otherwise the first
    /// target in the registry's stable member order is selected, which may be
    /// local or remote.
    pub fn send(
        registry: &PubSubRegistryState,
        path: impl Into<String>,
        local_affinity: bool,
    ) -> Self {
        let path = path.into();
        let all_targets = registry.path_targets(&path, true);
        let targets =
            if local_affinity && all_targets.iter().any(|node| node == registry.self_node()) {
                vec![PubSubPathDeliveryTarget::LocalPath]
            } else {
                all_targets
                    .into_iter()
                    .next()
                    .map(|node| {
                        if &node == registry.self_node() {
                            PubSubPathDeliveryTarget::LocalPath
                        } else {
                            PubSubPathDeliveryTarget::RemotePath { node }
                        }
                    })
                    .into_iter()
                    .collect()
            };
        Self {
            path,
            mode: PubSubPathDeliveryMode::One { local_affinity },
            targets,
        }
    }

    /// Builds a plan that sends to every mediator registered for `path`.
    ///
    /// `all_but_self` removes this node from the plan while retaining all
    /// matching remote member incarnations.
    pub fn send_to_all(
        registry: &PubSubRegistryState,
        path: impl Into<String>,
        all_but_self: bool,
    ) -> Self {
        let path = path.into();
        let targets = registry
            .path_targets(&path, !all_but_self)
            .into_iter()
            .map(|node| {
                if &node == registry.self_node() {
                    PubSubPathDeliveryTarget::LocalPath
                } else {
                    PubSubPathDeliveryTarget::RemotePath { node }
                }
            })
            .collect();
        Self {
            path,
            mode: PubSubPathDeliveryMode::All { all_but_self },
            targets,
        }
    }

    /// Returns whether this plan has no delivery targets.
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

/// Typed recipient for a remote member's local pubsub mediator.
///
/// The member's full [`UniqueAddress`] is retained so a route for an old node
/// incarnation cannot satisfy delivery to a replacement incarnation.
#[derive(Clone)]
pub struct PubSubRemoteTarget<M>
where
    M: Send + 'static,
{
    node: UniqueAddress,
    recipient: PubSubRecipient<M>,
}

impl<M> PubSubRemoteTarget<M>
where
    M: Send + 'static,
{
    /// Creates a remote target from a concrete typed recipient.
    pub fn new(
        node: UniqueAddress,
        recipient: impl Recipient<LocalPubSubMsg<M>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            node,
            recipient: Arc::new(recipient),
        }
    }

    /// Creates a remote target from a shared typed recipient.
    pub fn from_arc(node: UniqueAddress, recipient: PubSubRecipient<M>) -> Self {
        Self { node, recipient }
    }

    /// Returns the exact member incarnation associated with this target.
    pub fn node(&self) -> &UniqueAddress {
        &self.node
    }
}

/// Result of attempting every target in a topic-publication plan.
///
/// Delivery is best effort per target: one missing or failed recipient does
/// not prevent attempts to the plan's remaining targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubDeliveryReport {
    sent_to: Vec<PubSubDeliveryTarget>,
    failures: Vec<PubSubDeliveryFailure>,
}

impl PubSubDeliveryReport {
    /// Returns targets whose mediator recipient accepted the message.
    pub fn sent_to(&self) -> &[PubSubDeliveryTarget] {
        &self.sent_to
    }

    /// Returns missing-recipient and send failures in plan order.
    pub fn failures(&self) -> &[PubSubDeliveryFailure] {
        &self.failures
    }

    /// Returns whether every target accepted the message.
    ///
    /// An empty plan is successful because no attempted delivery failed.
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Failure to submit a topic publication to one selected mediator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubDeliveryFailure {
    /// No local or remote recipient was registered for the selected target.
    MissingTarget {
        /// Target that could not be resolved.
        target: PubSubDeliveryTarget,
    },
    /// The selected recipient rejected the message.
    SendFailed {
        /// Target whose recipient rejected the message.
        target: PubSubDeliveryTarget,
        /// Recipient-provided rejection reason.
        reason: String,
    },
}

/// Result of attempting every target in a logical actor-path plan.
///
/// Delivery is best effort per target: one missing or failed recipient does
/// not prevent attempts to the plan's remaining targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubPathDeliveryReport {
    sent_to: Vec<PubSubPathDeliveryTarget>,
    failures: Vec<PubSubPathDeliveryFailure>,
}

impl PubSubPathDeliveryReport {
    /// Returns targets whose mediator recipient accepted the message.
    pub fn sent_to(&self) -> &[PubSubPathDeliveryTarget] {
        &self.sent_to
    }

    /// Returns missing-recipient and send failures in plan order.
    pub fn failures(&self) -> &[PubSubPathDeliveryFailure] {
        &self.failures
    }

    /// Returns whether every target accepted the message.
    ///
    /// An empty plan is successful because no attempted delivery failed.
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Failure to submit a logical actor-path message to one selected mediator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubPathDeliveryFailure {
    /// No local or remote recipient was registered for the selected target.
    MissingTarget {
        /// Target that could not be resolved.
        target: PubSubPathDeliveryTarget,
    },
    /// The selected recipient rejected the message.
    SendFailed {
        /// Target whose recipient rejected the message.
        target: PubSubPathDeliveryTarget,
        /// Recipient-provided rejection reason.
        reason: String,
    },
}

/// In-process dispatch table used to execute pubsub delivery plans.
///
/// The table maps exact member incarnations to typed mediator recipients. A
/// remote transport adapter can implement those recipients without making the
/// business message or local mediator dynamically typed.
#[derive(Clone)]
pub struct PubSubDeliveryTransport<M>
where
    M: Send + 'static,
{
    local: Option<PubSubRecipient<M>>,
    remotes: BTreeMap<String, PubSubRecipient<M>>,
}

impl<M> Default for PubSubDeliveryTransport<M>
where
    M: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<M> PubSubDeliveryTransport<M>
where
    M: Send + 'static,
{
    /// Creates an empty transport with no local or remote recipients.
    pub fn new() -> Self {
        Self {
            local: None,
            remotes: BTreeMap::new(),
        }
    }

    /// Installs the local mediator recipient and returns this transport.
    pub fn with_local(
        mut self,
        recipient: impl Recipient<LocalPubSubMsg<M>> + Send + Sync + 'static,
    ) -> Self {
        self.set_local(recipient);
        self
    }

    /// Replaces the local mediator recipient.
    pub fn set_local(
        &mut self,
        recipient: impl Recipient<LocalPubSubMsg<M>> + Send + Sync + 'static,
    ) {
        self.local = Some(Arc::new(recipient));
    }

    /// Replaces the local mediator recipient with a shared recipient.
    pub fn set_local_arc(&mut self, recipient: PubSubRecipient<M>) {
        self.local = Some(recipient);
    }

    /// Removes the local mediator recipient.
    pub fn clear_local(&mut self) {
        self.local = None;
    }

    /// Replaces every remote route with the supplied exact-incarnation targets.
    pub fn set_remote_targets(&mut self, targets: impl IntoIterator<Item = PubSubRemoteTarget<M>>) {
        self.remotes = targets
            .into_iter()
            .map(|target| (node_key(&target.node), target.recipient))
            .collect();
    }

    /// Inserts or replaces the route for one exact member incarnation.
    pub fn insert_remote_target(&mut self, target: PubSubRemoteTarget<M>) {
        self.remotes
            .insert(node_key(&target.node), target.recipient);
    }

    /// Removes the route for one exact member incarnation.
    pub fn remove_remote_target(&mut self, node: &UniqueAddress) {
        self.remotes.remove(&node_key(node));
    }

    /// Returns the number of installed remote member-incarnation routes.
    pub fn remote_target_count(&self) -> usize {
        self.remotes.len()
    }
}

impl<M> PubSubDeliveryTransport<M>
where
    M: Clone + Send + 'static,
{
    /// Attempts a cloned publication through every target in `plan`.
    ///
    /// Broadcast targets receive a local broadcast command. Group targets
    /// receive a command scoped to the selected group. All failures are
    /// collected without short-circuiting later targets.
    pub fn publish(&self, plan: &PubSubDeliveryPlan, message: M) -> PubSubDeliveryReport {
        let mut sent_to = Vec::new();
        let mut failures = Vec::new();

        for target in &plan.targets {
            let Some(recipient) = self.recipient_for(target) else {
                failures.push(PubSubDeliveryFailure::MissingTarget {
                    target: target.clone(),
                });
                continue;
            };
            let delivery = delivery_message(plan, target, message.clone());
            if let Err(error) = recipient.tell(delivery) {
                failures.push(PubSubDeliveryFailure::SendFailed {
                    target: target.clone(),
                    reason: error.reason().to_string(),
                });
            } else {
                sent_to.push(target.clone());
            }
        }

        PubSubDeliveryReport { sent_to, failures }
    }

    /// Attempts a cloned logical-path message through every target in `plan`.
    ///
    /// One-target plans re-enter each mediator as `Send`; all-target plans
    /// re-enter as `SendToAll`. All failures are collected without
    /// short-circuiting later targets.
    pub fn send_path(&self, plan: &PubSubPathDeliveryPlan, message: M) -> PubSubPathDeliveryReport {
        let mut sent_to = Vec::new();
        let mut failures = Vec::new();

        for target in &plan.targets {
            let Some(recipient) = self.path_recipient_for(target) else {
                failures.push(PubSubPathDeliveryFailure::MissingTarget {
                    target: target.clone(),
                });
                continue;
            };
            let delivery = path_delivery_message(plan, target, message.clone());
            if let Err(error) = recipient.tell(delivery) {
                failures.push(PubSubPathDeliveryFailure::SendFailed {
                    target: target.clone(),
                    reason: error.reason().to_string(),
                });
            } else {
                sent_to.push(target.clone());
            }
        }

        PubSubPathDeliveryReport { sent_to, failures }
    }

    fn recipient_for(&self, target: &PubSubDeliveryTarget) -> Option<&PubSubRecipient<M>> {
        match target {
            PubSubDeliveryTarget::LocalTopic | PubSubDeliveryTarget::LocalGroup { .. } => {
                self.local.as_ref()
            }
            PubSubDeliveryTarget::RemoteTopic { node }
            | PubSubDeliveryTarget::RemoteGroup { node, .. } => self.remotes.get(&node_key(node)),
        }
    }

    fn path_recipient_for(&self, target: &PubSubPathDeliveryTarget) -> Option<&PubSubRecipient<M>> {
        match target {
            PubSubPathDeliveryTarget::LocalPath => self.local.as_ref(),
            PubSubPathDeliveryTarget::RemotePath { node } => self.remotes.get(&node_key(node)),
        }
    }
}

fn delivery_message<M: Clone + Send + 'static>(
    plan: &PubSubDeliveryPlan,
    target: &PubSubDeliveryTarget,
    message: M,
) -> LocalPubSubMsg<M> {
    match target {
        PubSubDeliveryTarget::LocalTopic | PubSubDeliveryTarget::RemoteTopic { .. } => {
            LocalPubSubMsg::Publish {
                topic: plan.topic.clone(),
                message,
                mode: TopicPublishMode::Broadcast,
                reply_to: None,
            }
        }
        PubSubDeliveryTarget::LocalGroup { group }
        | PubSubDeliveryTarget::RemoteGroup { group, .. } => LocalPubSubMsg::PublishGroup {
            topic: plan.topic.clone(),
            group: group.clone(),
            message,
            reply_to: None,
        },
    }
}

fn path_delivery_message<M: Clone + Send + 'static>(
    plan: &PubSubPathDeliveryPlan,
    target: &PubSubPathDeliveryTarget,
    message: M,
) -> LocalPubSubMsg<M> {
    match target {
        PubSubPathDeliveryTarget::LocalPath | PubSubPathDeliveryTarget::RemotePath { .. } => {
            match plan.mode {
                PubSubPathDeliveryMode::One { .. } => LocalPubSubMsg::Send {
                    path: plan.path.clone(),
                    message,
                    reply_to: None,
                },
                PubSubPathDeliveryMode::All { .. } => LocalPubSubMsg::SendToAll {
                    path: plan.path.clone(),
                    message,
                    reply_to: None,
                },
            }
        }
    }
}

fn node_key(node: &UniqueAddress) -> String {
    node.ordering_key()
}
