use kairo_cluster::UniqueAddress;

use crate::{PubSubRegistryState, TopicName, TopicPublishMode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubDeliveryTarget {
    LocalTopic,
    RemoteTopic { node: UniqueAddress },
    LocalGroup { group: String },
    RemoteGroup { group: String, node: UniqueAddress },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubDeliveryPlan {
    pub topic: TopicName,
    pub mode: TopicPublishMode,
    pub targets: Vec<PubSubDeliveryTarget>,
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
