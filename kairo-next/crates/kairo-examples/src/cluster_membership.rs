use std::error::Error;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use kairo::actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, Address, Context, Props,
};
use kairo::cluster::{
    Cluster, ClusterEvent, ClusterEventPublisher, ClusterEventPublisherMsg,
    ClusterSubscriptionEvent, ClusterSubscriptionInitialState, CurrentClusterState, Gossip, Member,
    MemberEvent, MemberStatus, UniqueAddress,
};

use crate::reply::spawn_one_shot_reply;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMembershipObservation {
    pub initial_member_count: usize,
    pub up_member: String,
    pub removed_member: String,
    pub previous_status: MemberStatus,
    pub final_member_count: usize,
    pub final_members: Vec<String>,
    pub final_leader: Option<String>,
}

struct ClusterEventCollector {
    events: mpsc::Sender<ClusterSubscriptionEvent>,
}

impl ClusterEventCollector {
    fn new(events: mpsc::Sender<ClusterSubscriptionEvent>) -> Self {
        Self { events }
    }
}

impl Actor for ClusterEventCollector {
    type Msg = ClusterSubscriptionEvent;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.events
            .send(msg)
            .map_err(|error| ActorError::Message(error.to_string()))?;
        Ok(())
    }
}

pub struct ClusterMembershipExample {
    system: ActorSystem,
    publisher: ActorRef<ClusterEventPublisherMsg>,
    cluster: Cluster,
    self_node: UniqueAddress,
    peer_node: UniqueAddress,
    events: mpsc::Receiver<ClusterSubscriptionEvent>,
}

impl ClusterMembershipExample {
    pub fn start(system_name: &str) -> Result<Self, Box<dyn Error>> {
        let system = ActorSystem::builder(system_name).build()?;
        let self_node = UniqueAddress::new(Address::local(system_name), 1);
        let peer_node = UniqueAddress::new(
            Address::new(
                "kairo",
                format!("{system_name}-peer"),
                Some("127.0.0.1".to_string()),
                Some(25521),
            ),
            2,
        );
        let publisher_node = self_node.clone();
        let publisher = system.spawn(
            "cluster-events",
            Props::new(move || ClusterEventPublisher::new(publisher_node.clone())),
        )?;
        let cluster = Cluster::new(publisher.clone());
        let (events_tx, events) = mpsc::channel();
        let subscriber = system.spawn(
            "cluster-event-collector",
            Props::new(move || ClusterEventCollector::new(events_tx.clone())),
        )?;

        cluster
            .subscribe_with_initial_state(subscriber, ClusterSubscriptionInitialState::Snapshot)?;

        Ok(Self {
            system,
            publisher,
            cluster,
            self_node,
            peer_node,
            events,
        })
    }

    pub fn publish_membership_flow(
        &self,
        timeout: Duration,
    ) -> Result<ClusterMembershipObservation, Box<dyn Error>> {
        let initial_member_count = match self.events.recv_timeout(timeout)? {
            ClusterSubscriptionEvent::CurrentState(state) => state.members.len(),
            other => return Err(format!("expected initial cluster state, got {other:?}").into()),
        };

        let up_gossip = Gossip::from_members([
            up_member(self.self_node.clone(), 1),
            up_member(self.peer_node.clone(), 2),
        ])
        .seen(self.self_node.clone())
        .seen(self.peer_node.clone());
        self.publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(up_gossip.clone()))?;

        let up_member = self.recv_until(timeout, |event| match event {
            ClusterSubscriptionEvent::Event(ClusterEvent::Member(MemberEvent::Up(member)))
                if member.unique_address == self.peer_node =>
            {
                Some(member.unique_address.ordering_key())
            }
            _ => None,
        })?;

        let removed_gossip = up_gossip
            .remove(&self.peer_node, 1)
            .seen(self.self_node.clone());
        self.publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(removed_gossip))?;

        let (removed_member, previous_status) = self.recv_until(timeout, |event| match event {
            ClusterSubscriptionEvent::Event(ClusterEvent::Member(MemberEvent::Removed {
                member,
                previous_status,
            })) if member.unique_address == self.peer_node => {
                Some((member.unique_address.ordering_key(), previous_status))
            }
            _ => None,
        })?;

        let (reply_to, replies) =
            spawn_one_shot_reply::<CurrentClusterState>(&self.system, "cluster-current-state")?;
        self.cluster.send_current_state(reply_to)?;
        let final_state = replies.recv_timeout(timeout)?;
        let final_member_count = final_state.members.len();
        let final_members = final_state
            .members
            .iter()
            .map(|member| member.unique_address.ordering_key())
            .collect();
        let final_leader = final_state.leader.as_ref().map(UniqueAddress::ordering_key);

        Ok(ClusterMembershipObservation {
            initial_member_count,
            up_member,
            removed_member,
            previous_status,
            final_member_count,
            final_members,
            final_leader,
        })
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), Box<dyn Error>> {
        self.system.terminate(timeout)?;
        Ok(())
    }

    fn recv_until<T>(
        &self,
        timeout: Duration,
        mut match_event: impl FnMut(ClusterSubscriptionEvent) -> Option<T>,
    ) -> Result<T, Box<dyn Error>> {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or("timed out waiting for cluster event")?;
            let event = self.events.recv_timeout(remaining)?;
            if let Some(matched) = match_event(event) {
                return Ok(matched);
            }
        }
    }
}

pub fn run_cluster_membership(
    system_name: &str,
) -> Result<ClusterMembershipObservation, Box<dyn Error>> {
    let example = ClusterMembershipExample::start(system_name)?;
    let observation = example.publish_membership_flow(Duration::from_secs(1))?;
    example.shutdown(Duration::from_secs(1))?;
    Ok(observation)
}

fn up_member(node: UniqueAddress, up_number: u64) -> Member {
    Member::new(node, vec![])
        .with_status(MemberStatus::Up)
        .with_up_number(up_number)
}
