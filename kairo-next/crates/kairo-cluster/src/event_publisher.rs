#![deny(missing_docs)]

use std::sync::Arc;

use kairo_actor::{Actor, ActorRef, ActorResult, Context};

mod subscription;

pub use subscription::{
    ClusterSubscriptionEvent, ClusterSubscriptionInitialState, CurrentClusterState,
    SubscriptionInitialState,
};

use crate::{ClusterEvent, ClusterEvents, Gossip, UniqueAddress};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Optional backend-neutral diagnostic emitted by the cluster event publisher.
pub enum ClusterDiagnostic {
    /// Gossip changed and produced the listed ordered domain events.
    GossipStateChanged {
        /// Membership state before the change.
        previous: Gossip,
        /// Membership state after the change.
        current: Gossip,
        /// Deterministic domain-event diff published for the change.
        events: Vec<ClusterEvent>,
    },
}

/// Sink for optional cluster runtime diagnostics.
pub trait ClusterDiagnostics: Send + Sync + 'static {
    /// Records one diagnostic observation.
    fn record(&self, diagnostic: ClusterDiagnostic);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Selects which optional cluster diagnostics reach a sink.
pub struct ClusterDiagnosticFilter {
    gossip_state_changes: bool,
}

impl ClusterDiagnosticFilter {
    /// Creates a filter that optionally observes gossip state changes.
    pub fn new(gossip_state_changes: bool) -> Self {
        Self {
            gossip_state_changes,
        }
    }

    /// Creates a filter that observes every diagnostic category.
    pub fn all() -> Self {
        Self::new(true)
    }

    /// Creates a filter that suppresses every diagnostic category.
    pub fn disabled() -> Self {
        Self::new(false)
    }

    /// Returns whether gossip state-change diagnostics are enabled.
    pub fn gossip_state_changes(&self) -> bool {
        self.gossip_state_changes
    }

    /// Returns whether `diagnostic` passes this filter.
    pub fn observes(&self, diagnostic: &ClusterDiagnostic) -> bool {
        match diagnostic {
            ClusterDiagnostic::GossipStateChanged { .. } => self.gossip_state_changes,
        }
    }

    /// Wraps `diagnostics` with this filter, or returns `None` when disabled.
    pub fn wrap(
        self,
        diagnostics: Arc<dyn ClusterDiagnostics>,
    ) -> Option<Arc<dyn ClusterDiagnostics>> {
        if self == Self::disabled() {
            None
        } else {
            Some(Arc::new(FilteredClusterDiagnostics {
                filter: self,
                diagnostics,
            }))
        }
    }
}

impl Default for ClusterDiagnosticFilter {
    fn default() -> Self {
        Self::all()
    }
}

struct FilteredClusterDiagnostics {
    filter: ClusterDiagnosticFilter,
    diagnostics: Arc<dyn ClusterDiagnostics>,
}

impl ClusterDiagnostics for FilteredClusterDiagnostics {
    fn record(&self, diagnostic: ClusterDiagnostic) {
        if self.filter.observes(&diagnostic) {
            self.diagnostics.record(diagnostic);
        }
    }
}

impl<F> ClusterDiagnostics for F
where
    F: Fn(ClusterDiagnostic) + Send + Sync + 'static,
{
    fn record(&self, diagnostic: ClusterDiagnostic) {
        self(diagnostic);
    }
}

#[derive(Debug, Clone)]
/// Commands accepted by [`ClusterEventPublisher`].
pub enum ClusterEventPublisherMsg {
    /// Replaces the current gossip view and publishes its deterministic diff.
    PublishChanges(Gossip),
    /// Publishes one explicit domain event without changing stored gossip.
    PublishEvent(ClusterEvent),
    /// Adds a raw domain-event subscriber.
    Subscribe {
        /// Actor that receives raw domain events.
        subscriber: ActorRef<ClusterEvent>,
        /// Initial-state replay mode.
        initial_state: SubscriptionInitialState,
    },
    /// Removes a raw domain-event subscriber.
    Unsubscribe {
        /// Actor path whose subscription should be removed.
        subscriber: ActorRef<ClusterEvent>,
    },
    /// Adds a snapshot-or-event subscriber.
    SubscribeCluster {
        /// Actor that receives cluster subscription values.
        subscriber: ActorRef<ClusterSubscriptionEvent>,
        /// Snapshot, event replay, or live-only initial mode.
        initial_state: ClusterSubscriptionInitialState,
    },
    /// Removes a snapshot-or-event subscriber.
    UnsubscribeCluster {
        /// Actor path whose subscription should be removed.
        subscriber: ActorRef<ClusterSubscriptionEvent>,
    },
    /// Sends the latest full state to one recipient without subscribing it.
    SendCurrentState {
        /// Recipient of the current cluster snapshot.
        reply_to: ActorRef<CurrentClusterState>,
    },
}

/// Actor that owns the latest gossip view and publishes cluster-domain changes.
///
/// Subscription registration is idempotent by actor path. Stopped subscribers
/// are removed when a subsequent event delivery fails.
pub struct ClusterEventPublisher {
    self_node: UniqueAddress,
    gossip: Gossip,
    subscribers: Vec<ActorRef<ClusterEvent>>,
    cluster_subscribers: Vec<ActorRef<ClusterSubscriptionEvent>>,
    diagnostics: Option<Arc<dyn ClusterDiagnostics>>,
}

impl ClusterEventPublisher {
    /// Creates an empty publisher whose leader calculations use `self_node`.
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            self_node,
            gossip: Gossip::new(),
            subscribers: Vec::new(),
            cluster_subscribers: Vec::new(),
            diagnostics: None,
        }
    }

    /// Attaches a backend-neutral diagnostics sink.
    pub fn with_diagnostics(mut self, diagnostics: Arc<dyn ClusterDiagnostics>) -> Self {
        self.diagnostics = Some(diagnostics);
        self
    }

    fn subscribe(
        &mut self,
        subscriber: ActorRef<ClusterEvent>,
        initial_state: SubscriptionInitialState,
    ) {
        if !self
            .subscribers
            .iter()
            .any(|existing| existing.path() == subscriber.path())
        {
            self.subscribers.push(subscriber.clone());
        }

        if initial_state == SubscriptionInitialState::Events {
            let empty = Gossip::new();
            for event in ClusterEvents::diff(&empty, &self.gossip, &self.self_node) {
                let _ = subscriber.tell(event);
            }
        }
    }

    fn unsubscribe(&mut self, subscriber: &ActorRef<ClusterEvent>) {
        self.subscribers
            .retain(|existing| existing.path() != subscriber.path());
    }

    fn subscribe_cluster(
        &mut self,
        subscriber: ActorRef<ClusterSubscriptionEvent>,
        initial_state: ClusterSubscriptionInitialState,
    ) {
        if !self
            .cluster_subscribers
            .iter()
            .any(|existing| existing.path() == subscriber.path())
        {
            self.cluster_subscribers.push(subscriber.clone());
        }

        match initial_state {
            ClusterSubscriptionInitialState::None => {}
            ClusterSubscriptionInitialState::Snapshot => {
                let _ = subscriber.tell(ClusterSubscriptionEvent::CurrentState(
                    CurrentClusterState::from_gossip(&self.gossip, &self.self_node),
                ));
            }
            ClusterSubscriptionInitialState::Events => {
                let empty = Gossip::new();
                for event in ClusterEvents::diff(&empty, &self.gossip, &self.self_node) {
                    let _ = subscriber.tell(ClusterSubscriptionEvent::Event(event));
                }
            }
        }
    }

    fn unsubscribe_cluster(&mut self, subscriber: &ActorRef<ClusterSubscriptionEvent>) {
        self.cluster_subscribers
            .retain(|existing| existing.path() != subscriber.path());
    }

    fn publish_changes(&mut self, new_gossip: Gossip) {
        let events = ClusterEvents::diff(&self.gossip, &new_gossip, &self.self_node);
        if self.gossip != new_gossip {
            self.record_diagnostic(ClusterDiagnostic::GossipStateChanged {
                previous: self.gossip.clone(),
                current: new_gossip.clone(),
                events: events.clone(),
            });
        }
        self.gossip = new_gossip;
        for event in events {
            self.publish(event);
        }
    }

    fn publish(&mut self, event: ClusterEvent) {
        self.subscribers
            .retain(|subscriber| subscriber.tell(event.clone()).is_ok());
        self.cluster_subscribers.retain(|subscriber| {
            subscriber
                .tell(ClusterSubscriptionEvent::Event(event.clone()))
                .is_ok()
        });
    }

    fn record_diagnostic(&self, diagnostic: ClusterDiagnostic) {
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.record(diagnostic);
        }
    }
}

impl Actor for ClusterEventPublisher {
    type Msg = ClusterEventPublisherMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterEventPublisherMsg::PublishChanges(gossip) => self.publish_changes(gossip),
            ClusterEventPublisherMsg::PublishEvent(event) => self.publish(event),
            ClusterEventPublisherMsg::Subscribe {
                subscriber,
                initial_state,
            } => self.subscribe(subscriber, initial_state),
            ClusterEventPublisherMsg::Unsubscribe { subscriber } => self.unsubscribe(&subscriber),
            ClusterEventPublisherMsg::SubscribeCluster {
                subscriber,
                initial_state,
            } => self.subscribe_cluster(subscriber, initial_state),
            ClusterEventPublisherMsg::UnsubscribeCluster { subscriber } => {
                self.unsubscribe_cluster(&subscriber);
            }
            ClusterEventPublisherMsg::SendCurrentState { reply_to } => {
                let _ = reply_to.tell(CurrentClusterState::from_gossip(
                    &self.gossip,
                    &self.self_node,
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
