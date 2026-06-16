use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::time::Duration;

use kairo::actor::{ActorSystem, Address, Props};
use kairo::cluster::UniqueAddress;
use kairo::cluster_tools::{
    DistributedPubSubMediatorActor, DistributedPubSubMediatorMsg, DistributedPubSubPublishReport,
    PubSubDeliveryTarget, TopicName, TopicPublishMode,
};

use crate::reply::spawn_one_shot_reply;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterToolsDistributedObservation {
    pub broadcast_topic: String,
    pub remote_broadcast_message: String,
    pub broadcast_targets: Vec<String>,
    pub group_topic: String,
    pub local_group_message: String,
    pub remote_group_message: String,
    pub group_targets: Vec<String>,
    pub current_topics: Vec<String>,
    pub remote_target_count: usize,
}

pub fn run_cluster_tools_distributed(
    system_name: &str,
) -> Result<ClusterToolsDistributedObservation, Box<dyn Error>> {
    let example = ClusterToolsDistributedExample::start(system_name)?;
    let observation = example.run(Duration::from_secs(1))?;
    example.shutdown(Duration::from_secs(1))?;
    Ok(observation)
}

pub struct ClusterToolsDistributedExample {
    system: ActorSystem,
    node_a: UniqueAddress,
    node_b: UniqueAddress,
}

impl ClusterToolsDistributedExample {
    pub fn start(system_name: &str) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            system: ActorSystem::builder(system_name).build()?,
            node_a: node(system_name, 25520, 1),
            node_b: node(system_name, 25521, 2),
        })
    }

    pub fn run(
        &self,
        timeout: Duration,
    ) -> Result<ClusterToolsDistributedObservation, Box<dyn Error>> {
        let mediator_a = self.system.spawn(
            "mediator-a",
            Props::new({
                let node_a = self.node_a.clone();
                move || DistributedPubSubMediatorActor::<String>::new(node_a)
            }),
        )?;
        let mediator_b = self.system.spawn(
            "mediator-b",
            Props::new({
                let node_b = self.node_b.clone();
                move || DistributedPubSubMediatorActor::<String>::new(node_b)
            }),
        )?;

        mediator_a.tell(DistributedPubSubMediatorMsg::AddRemoteMediator {
            node: self.node_b.clone(),
            mediator: mediator_b.clone(),
        })?;

        let broadcast_topic = TopicName::new("orders");
        let (remote_orders, remote_orders_rx) =
            spawn_one_shot_reply::<String>(&self.system, "remote-orders")?;
        mediator_b.tell(DistributedPubSubMediatorMsg::Subscribe {
            topic: broadcast_topic.clone(),
            subscriber: remote_orders,
            reply_to: None,
        })?;
        self.merge_registry_b_into_a(
            "mediator-b-orders-registry",
            &mediator_a,
            &mediator_b,
            timeout,
        )?;

        let (broadcast_report_ref, broadcast_report_rx) = spawn_one_shot_reply::<
            DistributedPubSubPublishReport,
        >(
            &self.system, "broadcast-report"
        )?;
        mediator_a.tell(DistributedPubSubMediatorMsg::Publish {
            topic: broadcast_topic.clone(),
            message: "created".to_string(),
            mode: TopicPublishMode::Broadcast,
            reply_to: Some(broadcast_report_ref),
        })?;
        let broadcast_report = broadcast_report_rx.recv_timeout(timeout)?;
        let remote_broadcast_message = remote_orders_rx.recv_timeout(timeout)?;

        let group_topic = TopicName::new("jobs");
        let (local_jobs, local_jobs_rx) =
            spawn_one_shot_reply::<String>(&self.system, "local-blue-jobs")?;
        let (remote_jobs, remote_jobs_rx) =
            spawn_one_shot_reply::<String>(&self.system, "remote-red-jobs")?;
        mediator_a.tell(DistributedPubSubMediatorMsg::SubscribeGroup {
            topic: group_topic.clone(),
            group: "blue".to_string(),
            subscriber: local_jobs,
            reply_to: None,
        })?;
        mediator_b.tell(DistributedPubSubMediatorMsg::SubscribeGroup {
            topic: group_topic.clone(),
            group: "red".to_string(),
            subscriber: remote_jobs,
            reply_to: None,
        })?;
        self.merge_registry_b_into_a(
            "mediator-b-jobs-registry",
            &mediator_a,
            &mediator_b,
            timeout,
        )?;

        let (group_report_ref, group_report_rx) =
            spawn_one_shot_reply::<DistributedPubSubPublishReport>(&self.system, "group-report")?;
        mediator_a.tell(DistributedPubSubMediatorMsg::Publish {
            topic: group_topic.clone(),
            message: "run".to_string(),
            mode: TopicPublishMode::OnePerGroup,
            reply_to: Some(group_report_ref),
        })?;
        let group_report = group_report_rx.recv_timeout(timeout)?;
        let local_group_message = local_jobs_rx.recv_timeout(timeout)?;
        let remote_group_message = remote_jobs_rx.recv_timeout(timeout)?;

        let (state_ref, state_rx) = spawn_one_shot_reply(&self.system, "mediator-a-state")?;
        mediator_a.tell(DistributedPubSubMediatorMsg::GetState {
            reply_to: state_ref,
        })?;
        let state = state_rx.recv_timeout(timeout)?;

        Ok(ClusterToolsDistributedObservation {
            broadcast_topic: broadcast_topic.as_str().to_string(),
            remote_broadcast_message,
            broadcast_targets: target_names(&broadcast_report.plan.targets),
            group_topic: group_topic.as_str().to_string(),
            local_group_message,
            remote_group_message,
            group_targets: target_names(&group_report.plan.targets),
            current_topics: topic_names(state.current_topics),
            remote_target_count: state.remote_target_count,
        })
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), Box<dyn Error>> {
        self.system.terminate(timeout)?;
        Ok(())
    }

    fn merge_registry_b_into_a(
        &self,
        reply_actor_name: &str,
        mediator_a: &kairo::actor::ActorRef<DistributedPubSubMediatorMsg<String>>,
        mediator_b: &kairo::actor::ActorRef<DistributedPubSubMediatorMsg<String>>,
        timeout: Duration,
    ) -> Result<(), Box<dyn Error>> {
        let (registry_ref, registry_rx) = spawn_one_shot_reply(&self.system, reply_actor_name)?;
        mediator_b.tell(DistributedPubSubMediatorMsg::GetRegistry {
            reply_to: registry_ref,
        })?;
        let registry_b = registry_rx.recv_timeout(timeout)?;
        mediator_a.tell(DistributedPubSubMediatorMsg::MergeDelta {
            delta: registry_b.collect_delta(&BTreeMap::new(), 10),
        })?;
        Ok(())
    }
}

fn node(system: &str, port: u16, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
        uid,
    )
}

fn topic_names(topics: BTreeSet<TopicName>) -> Vec<String> {
    topics
        .into_iter()
        .map(|topic| topic.as_str().to_string())
        .collect()
}

fn target_names(targets: &[PubSubDeliveryTarget]) -> Vec<String> {
    targets
        .iter()
        .map(|target| match target {
            PubSubDeliveryTarget::LocalTopic => "local-topic".to_string(),
            PubSubDeliveryTarget::RemoteTopic { node } => {
                format!("remote-topic:{}", node.ordering_key())
            }
            PubSubDeliveryTarget::LocalGroup { group } => format!("local-group:{group}"),
            PubSubDeliveryTarget::RemoteGroup { group, node } => {
                format!("remote-group:{group}:{}", node.ordering_key())
            }
        })
        .collect()
}
