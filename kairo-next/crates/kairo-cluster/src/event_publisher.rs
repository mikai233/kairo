use kairo_actor::{Actor, ActorRef, ActorResult, Context};

mod subscription;

pub use subscription::{
    ClusterSubscriptionEvent, ClusterSubscriptionInitialState, CurrentClusterState,
    SubscriptionInitialState,
};

use crate::{ClusterEvent, ClusterEvents, Gossip, UniqueAddress};

#[derive(Debug, Clone)]
pub enum ClusterEventPublisherMsg {
    PublishChanges(Gossip),
    PublishEvent(ClusterEvent),
    Subscribe {
        subscriber: ActorRef<ClusterEvent>,
        initial_state: SubscriptionInitialState,
    },
    Unsubscribe {
        subscriber: ActorRef<ClusterEvent>,
    },
    SubscribeCluster {
        subscriber: ActorRef<ClusterSubscriptionEvent>,
        initial_state: ClusterSubscriptionInitialState,
    },
    UnsubscribeCluster {
        subscriber: ActorRef<ClusterSubscriptionEvent>,
    },
    SendCurrentState {
        reply_to: ActorRef<CurrentClusterState>,
    },
}

pub struct ClusterEventPublisher {
    self_node: UniqueAddress,
    gossip: Gossip,
    subscribers: Vec<ActorRef<ClusterEvent>>,
    cluster_subscribers: Vec<ActorRef<ClusterSubscriptionEvent>>,
}

impl ClusterEventPublisher {
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            self_node,
            gossip: Gossip::new(),
            subscribers: Vec::new(),
            cluster_subscribers: Vec::new(),
        }
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
