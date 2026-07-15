#![deny(missing_docs)]

//! Cluster-membership subscription adapter for coordinator discovery.
//!
//! Pekko's shard region subscribes and unsubscribes directly in its actor
//! lifecycle. Kairo keeps that ownership in a focused actor that translates
//! the initial membership snapshot and later events into typed
//! [`ShardRegionMsg`] values, leaving the region runtime independent of the
//! cluster extension API.

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};
use kairo_cluster::{Cluster, ClusterSubscriptionEvent, ClusterSubscriptionInitialState};

use crate::ShardRegionMsg;

/// Actor that owns one cluster subscription and forwards it to a shard region.
///
/// The actor subscribes with an initial snapshot when it starts and performs a
/// best-effort unsubscribe when it stops. A failed forward is recorded for
/// diagnostics rather than stopping the subscriber.
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
    /// Creates an unstarted subscriber for `region`.
    ///
    /// The cluster subscription is installed by the actor's `started`
    /// lifecycle callback after this value is spawned.
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

    /// Creates repeatable actor properties for a region discovery subscriber.
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
/// Local protocol accepted by [`ShardRegionDiscoverySubscriber`].
pub enum ShardRegionDiscoverySubscriberMsg {
    /// Initial state or a later membership event from the cluster subscription.
    Cluster(ClusterSubscriptionEvent),
    /// Requests a diagnostic snapshot of subscription and forwarding state.
    Snapshot {
        /// Actor that receives the current diagnostic snapshot.
        reply_to: ActorRef<ShardRegionDiscoverySubscriberSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Diagnostic state for a region discovery subscriber.
pub struct ShardRegionDiscoverySubscriberSnapshot {
    /// Local actor path of the target shard region.
    pub region: String,
    /// Whether startup successfully installed the cluster subscription.
    pub subscribed: bool,
    /// Number of initial membership snapshots the actor attempted to forward.
    pub forwarded_snapshots: usize,
    /// Number of incremental membership events the actor attempted to forward.
    pub forwarded_events: usize,
    /// Most recent region-send failure, cleared by the next successful forward.
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
