use std::error::Error;
use std::time::Duration;

use kairo::actor::{ActorRef, ActorSystem, Props};
use kairo::distributed_data::{
    GCounter, GetResponse, ReadConsistency, ReplicaId, ReplicatorActor, ReplicatorActorMsg,
    ReplicatorChange, ReplicatorKey, UpdateResponse, WriteConsistency,
};

use crate::reply::spawn_one_shot_reply;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DDataCounterObservation {
    pub key: String,
    pub replica: String,
    pub initial_not_found: bool,
    pub update_changed: bool,
    pub change_value: u128,
    pub read_value: u128,
}

pub struct DDataCounterExample {
    system: ActorSystem,
    replicator: ActorRef<ReplicatorActorMsg<GCounter>>,
}

impl DDataCounterExample {
    pub fn start(system_name: &str) -> Result<Self, Box<dyn Error>> {
        let system = ActorSystem::builder(system_name).build()?;
        let replicator =
            system.spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))?;
        Ok(Self { system, replicator })
    }

    pub fn increment_and_read(
        &self,
        key: &str,
        replica: &str,
        amount: u128,
        timeout: Duration,
    ) -> Result<DDataCounterObservation, Box<dyn Error>> {
        let key = ReplicatorKey::new(key);
        let replica = ReplicaId::new(replica);
        let (initial_ref, initial_rx) =
            spawn_one_shot_reply::<GetResponse<GCounter>>(&self.system, "ddata-initial-get")?;
        let (update_ref, update_rx) =
            spawn_one_shot_reply::<UpdateResponse<GCounter>>(&self.system, "ddata-update")?;
        let (change_ref, change_rx) =
            spawn_one_shot_reply::<ReplicatorChange<GCounter>>(&self.system, "ddata-change")?;
        let (read_ref, read_rx) =
            spawn_one_shot_reply::<GetResponse<GCounter>>(&self.system, "ddata-read")?;

        self.replicator.tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::local(),
            reply_to: initial_ref,
        })?;
        let initial_not_found = matches!(
            initial_rx.recv_timeout(timeout)?,
            GetResponse::NotFound { key: initial_key } if initial_key == key
        );

        self.replicator.tell(ReplicatorActorMsg::Subscribe {
            key: key.clone(),
            subscriber: change_ref,
        })?;
        self.replicator.tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GCounter::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new({
                let replica = replica.clone();
                move |counter| {
                    counter
                        .increment(replica, amount)
                        .map_err(|error| error.to_string())
                }
            }),
            reply_to: update_ref,
        })?;

        let update_changed = match update_rx.recv_timeout(timeout)? {
            UpdateResponse::Success(outcome) if outcome.key() == &key => outcome.changed(),
            other => return Err(format!("unexpected update response: {other:?}").into()),
        };
        self.replicator.tell(ReplicatorActorMsg::FlushChanges)?;
        let change = change_rx.recv_timeout(timeout)?;
        let change_value = change.data().value()?;

        self.replicator.tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::local(),
            reply_to: read_ref,
        })?;
        let read_value = match read_rx.recv_timeout(timeout)? {
            GetResponse::Success {
                key: read_key,
                data,
            } if read_key == key => data.value()?,
            other => return Err(format!("unexpected read response: {other:?}").into()),
        };

        Ok(DDataCounterObservation {
            key: key.as_str().to_string(),
            replica: replica.as_str().to_string(),
            initial_not_found,
            update_changed,
            change_value,
            read_value,
        })
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), Box<dyn Error>> {
        self.system.terminate(timeout)?;
        Ok(())
    }
}

pub fn run_ddata_counter(
    system_name: &str,
    amount: u128,
) -> Result<DDataCounterObservation, Box<dyn Error>> {
    let example = DDataCounterExample::start(system_name)?;
    let observation = example.increment_and_read(
        "counters.requests",
        "node-a",
        amount,
        Duration::from_secs(1),
    )?;
    example.shutdown(Duration::from_secs(1))?;
    Ok(observation)
}
