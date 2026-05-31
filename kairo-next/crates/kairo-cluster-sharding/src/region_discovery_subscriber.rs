use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};
use kairo_cluster::{Cluster, ClusterSubscriptionEvent, ClusterSubscriptionInitialState};

use crate::ShardRegionMsg;

pub struct ShardRegionDiscoverySubscriber<M>
where
    M: Send + 'static,
{
    cluster: Cluster,
    region: ActorRef<ShardRegionMsg<M>>,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    forwarded_snapshots: usize,
    forwarded_events: usize,
    last_error: Option<String>,
}

impl<M> ShardRegionDiscoverySubscriber<M>
where
    M: Send + 'static,
{
    pub fn new(cluster: Cluster, region: ActorRef<ShardRegionMsg<M>>) -> Self {
        Self {
            cluster,
            region,
            subscription: None,
            forwarded_snapshots: 0,
            forwarded_events: 0,
            last_error: None,
        }
    }

    pub fn props(cluster: Cluster, region: ActorRef<ShardRegionMsg<M>>) -> Props<Self> {
        Props::new(move || Self::new(cluster.clone(), region.clone()))
    }

    fn forward_cluster_event(&mut self, event: ClusterSubscriptionEvent) {
        let send_result = match event {
            ClusterSubscriptionEvent::CurrentState(state) => {
                self.forwarded_snapshots += 1;
                self.region
                    .tell(ShardRegionMsg::CoordinatorDiscoverySnapshot { state })
            }
            ClusterSubscriptionEvent::Event(event) => {
                self.forwarded_events += 1;
                self.region
                    .tell(ShardRegionMsg::CoordinatorDiscoveryEvent { event })
            }
        };
        match send_result {
            Ok(()) => self.last_error = None,
            Err(error) => self.last_error = Some(error.reason().to_string()),
        }
    }

    fn snapshot(&self) -> ShardRegionDiscoverySubscriberSnapshot {
        ShardRegionDiscoverySubscriberSnapshot {
            region: self.region.path().to_string(),
            subscribed: self.subscription.is_some(),
            forwarded_snapshots: self.forwarded_snapshots,
            forwarded_events: self.forwarded_events,
            last_error: self.last_error.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ShardRegionDiscoverySubscriberMsg {
    Cluster(ClusterSubscriptionEvent),
    Snapshot {
        reply_to: ActorRef<ShardRegionDiscoverySubscriberSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRegionDiscoverySubscriberSnapshot {
    pub region: String,
    pub subscribed: bool,
    pub forwarded_snapshots: usize,
    pub forwarded_events: usize,
    pub last_error: Option<String>,
}

impl<M> Actor for ShardRegionDiscoverySubscriber<M>
where
    M: Send + 'static,
{
    type Msg = ShardRegionDiscoverySubscriberMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription = ctx.message_adapter(ShardRegionDiscoverySubscriberMsg::Cluster)?;
        self.cluster
            .subscribe_with_initial_state(
                subscription.clone(),
                ClusterSubscriptionInitialState::Snapshot,
            )
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.subscription = Some(subscription);
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        if let Some(subscription) = self.subscription.take() {
            let _ = self.cluster.unsubscribe(subscription);
        }
        Ok(())
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ShardRegionDiscoverySubscriberMsg::Cluster(event) => {
                self.forward_cluster_event(event);
                Ok(())
            }
            ShardRegionDiscoverySubscriberMsg::Snapshot { reply_to } => reply_to
                .tell(self.snapshot())
                .map_err(|error| ActorError::Message(error.reason().to_string())),
        }
    }
}
