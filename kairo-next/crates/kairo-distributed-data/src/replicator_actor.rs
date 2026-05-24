use std::collections::BTreeMap;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};

use crate::{
    DataEnvelope, DeltaReplicatedData, GetResponse, ReplicatorChange, ReplicatorKey,
    ReplicatorState, UpdateResponse, WriteConsistency,
};

pub struct ReplicatorActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    state: ReplicatorState<D>,
    subscribers: BTreeMap<ReplicatorKey, Vec<ActorRef<ReplicatorChange<D>>>>,
    remote_replica_count: usize,
}

impl<D> ReplicatorActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    pub fn new() -> Self {
        Self::with_remote_replica_count(0)
    }

    pub fn with_remote_replica_count(remote_replica_count: usize) -> Self {
        Self {
            state: ReplicatorState::new(),
            subscribers: BTreeMap::new(),
            remote_replica_count,
        }
    }

    pub fn state(&self) -> &ReplicatorState<D> {
        &self.state
    }
}

impl<D> Default for ReplicatorActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

pub enum ReplicatorActorMsg<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    Get {
        key: ReplicatorKey,
        consistency: crate::ReadConsistency,
        reply_to: ActorRef<GetResponse<D>>,
    },
    Update {
        key: ReplicatorKey,
        initial: D,
        consistency: WriteConsistency,
        modify: Box<dyn FnOnce(D) -> Result<D, String> + Send>,
        reply_to: ActorRef<UpdateResponse<D::Delta>>,
    },
    WriteFull {
        key: ReplicatorKey,
        envelope: DataEnvelope<D>,
    },
    WriteDelta {
        key: ReplicatorKey,
        delta: D::Delta,
    },
    Subscribe {
        key: ReplicatorKey,
        subscriber: ActorRef<ReplicatorChange<D>>,
    },
    Unsubscribe {
        key: ReplicatorKey,
        subscriber: ActorRef<ReplicatorChange<D>>,
    },
    FlushChanges,
}

impl<D> Actor for ReplicatorActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    type Msg = ReplicatorActorMsg<D>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ReplicatorActorMsg::Get {
                key,
                consistency,
                reply_to,
            } => {
                let response = if consistency.is_local(self.remote_replica_count) {
                    self.state.get_local(&key)
                } else {
                    GetResponse::Failure {
                        key,
                        reason: "non-local read aggregation is not wired yet".to_string(),
                    }
                };
                tell_or_actor_error(&reply_to, response)?;
            }
            ReplicatorActorMsg::Update {
                key,
                initial,
                consistency,
                modify,
                reply_to,
            } => {
                let response = match self.state.update_local(key.clone(), initial, modify) {
                    Ok(outcome) if consistency.is_local(self.remote_replica_count) => {
                        UpdateResponse::Success(outcome)
                    }
                    Ok(_) => UpdateResponse::Timeout { key },
                    Err(reason) => UpdateResponse::ModifyFailure { key, reason },
                };
                tell_or_actor_error(&reply_to, response)?;
            }
            ReplicatorActorMsg::WriteFull { key, envelope } => {
                self.state.write_full(key, envelope);
            }
            ReplicatorActorMsg::WriteDelta { key, delta } => {
                self.state.write_delta(key, delta);
            }
            ReplicatorActorMsg::Subscribe { key, subscriber } => {
                self.subscribe(key, subscriber);
            }
            ReplicatorActorMsg::Unsubscribe { key, subscriber } => {
                self.unsubscribe(&key, &subscriber);
            }
            ReplicatorActorMsg::FlushChanges => {
                self.flush_changes();
            }
        }
        Ok(())
    }
}

impl<D> ReplicatorActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    fn subscribe(&mut self, key: ReplicatorKey, subscriber: ActorRef<ReplicatorChange<D>>) {
        let current = self.state.get_local(&key).data().cloned();
        if let Some(data) = current {
            let change = ReplicatorChange::new(key.clone(), data);
            if subscriber.tell(change).is_err() {
                return;
            }
        }

        let subscribers = self.subscribers.entry(key).or_default();
        if subscribers
            .iter()
            .all(|existing| existing.path() != subscriber.path())
        {
            subscribers.push(subscriber);
        }
    }

    fn unsubscribe(&mut self, key: &ReplicatorKey, subscriber: &ActorRef<ReplicatorChange<D>>) {
        if let Some(subscribers) = self.subscribers.get_mut(key) {
            subscribers.retain(|existing| existing.path() != subscriber.path());
            if subscribers.is_empty() {
                self.subscribers.remove(key);
            }
        }
    }

    fn flush_changes(&mut self) {
        for change in self.state.flush_changes() {
            if let Some(subscribers) = self.subscribers.get_mut(change.key()) {
                subscribers.retain(|subscriber| subscriber.tell(change.clone()).is_ok());
            }
        }
        self.subscribers
            .retain(|_, subscribers| !subscribers.is_empty());
    }
}

fn tell_or_actor_error<M>(target: &ActorRef<M>, message: M) -> ActorResult
where
    M: Send + 'static,
{
    target
        .tell(message)
        .map_err(|error| ActorError::Message(error.reason().to_string()))
}
