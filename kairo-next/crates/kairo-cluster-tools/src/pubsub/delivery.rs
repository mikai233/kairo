use std::collections::BTreeMap;
use std::sync::Arc;

use kairo_actor::Recipient;
use kairo_cluster::UniqueAddress;

use crate::{LocalPubSubMsg, PubSubRegistryState, TopicName, TopicPublishMode};

type PubSubRecipient<M> = Arc<dyn Recipient<LocalPubSubMsg<M>> + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubDeliveryTarget {
    LocalTopic,
    RemoteTopic { node: UniqueAddress },
    LocalGroup { group: String },
    RemoteGroup { group: String, node: UniqueAddress },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubPathDeliveryTarget {
    LocalPath,
    RemotePath { node: UniqueAddress },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubDeliveryPlan {
    pub topic: TopicName,
    pub mode: TopicPublishMode,
    pub targets: Vec<PubSubDeliveryTarget>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubSubPathDeliveryMode {
    One { local_affinity: bool },
    All { all_but_self: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubPathDeliveryPlan {
    pub path: String,
    pub mode: PubSubPathDeliveryMode,
    pub targets: Vec<PubSubPathDeliveryTarget>,
}

impl PubSubDeliveryPlan {
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

    pub fn has_local_target(&self) -> bool {
        self.targets.iter().any(|target| {
            matches!(
                target,
                PubSubDeliveryTarget::LocalTopic | PubSubDeliveryTarget::LocalGroup { .. }
            )
        })
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

impl PubSubPathDeliveryPlan {
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

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

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
    pub fn new(
        node: UniqueAddress,
        recipient: impl Recipient<LocalPubSubMsg<M>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            node,
            recipient: Arc::new(recipient),
        }
    }

    pub fn from_arc(node: UniqueAddress, recipient: PubSubRecipient<M>) -> Self {
        Self { node, recipient }
    }

    pub fn node(&self) -> &UniqueAddress {
        &self.node
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubDeliveryReport {
    sent_to: Vec<PubSubDeliveryTarget>,
    failures: Vec<PubSubDeliveryFailure>,
}

impl PubSubDeliveryReport {
    pub fn sent_to(&self) -> &[PubSubDeliveryTarget] {
        &self.sent_to
    }

    pub fn failures(&self) -> &[PubSubDeliveryFailure] {
        &self.failures
    }

    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubDeliveryFailure {
    MissingTarget {
        target: PubSubDeliveryTarget,
    },
    SendFailed {
        target: PubSubDeliveryTarget,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubPathDeliveryReport {
    sent_to: Vec<PubSubPathDeliveryTarget>,
    failures: Vec<PubSubPathDeliveryFailure>,
}

impl PubSubPathDeliveryReport {
    pub fn sent_to(&self) -> &[PubSubPathDeliveryTarget] {
        &self.sent_to
    }

    pub fn failures(&self) -> &[PubSubPathDeliveryFailure] {
        &self.failures
    }

    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubPathDeliveryFailure {
    MissingTarget {
        target: PubSubPathDeliveryTarget,
    },
    SendFailed {
        target: PubSubPathDeliveryTarget,
        reason: String,
    },
}

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
    pub fn new() -> Self {
        Self {
            local: None,
            remotes: BTreeMap::new(),
        }
    }

    pub fn with_local(
        mut self,
        recipient: impl Recipient<LocalPubSubMsg<M>> + Send + Sync + 'static,
    ) -> Self {
        self.set_local(recipient);
        self
    }

    pub fn set_local(
        &mut self,
        recipient: impl Recipient<LocalPubSubMsg<M>> + Send + Sync + 'static,
    ) {
        self.local = Some(Arc::new(recipient));
    }

    pub fn set_local_arc(&mut self, recipient: PubSubRecipient<M>) {
        self.local = Some(recipient);
    }

    pub fn clear_local(&mut self) {
        self.local = None;
    }

    pub fn set_remote_targets(&mut self, targets: impl IntoIterator<Item = PubSubRemoteTarget<M>>) {
        self.remotes = targets
            .into_iter()
            .map(|target| (node_key(&target.node), target.recipient))
            .collect();
    }

    pub fn insert_remote_target(&mut self, target: PubSubRemoteTarget<M>) {
        self.remotes
            .insert(node_key(&target.node), target.recipient);
    }

    pub fn remove_remote_target(&mut self, node: &UniqueAddress) {
        self.remotes.remove(&node_key(node));
    }

    pub fn remote_target_count(&self) -> usize {
        self.remotes.len()
    }
}

impl<M> PubSubDeliveryTransport<M>
where
    M: Clone + Send + 'static,
{
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
