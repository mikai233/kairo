use super::*;
use std::marker::PhantomData;

use kairo_remote::RemoteSettings;
use kairo_serialization::ActorRefWireData;

use crate::{ReplicatorRead, ReplicatorWrite, SenderAwareRecipient};

struct CaptureSender<M> {
    tx: mpsc::Sender<ActorRefWireData>,
    _message: PhantomData<fn(M)>,
}

impl<M> CaptureSender<M> {
    fn new(tx: mpsc::Sender<ActorRefWireData>) -> Self {
        Self {
            tx,
            _message: PhantomData,
        }
    }
}

impl<M> SenderAwareRecipient<M> for CaptureSender<M>
where
    M: Send + 'static,
{
    fn tell_with_sender(
        &self,
        message: M,
        sender: &ActorRefWireData,
    ) -> Result<(), kairo_actor::SendError<M>> {
        self.tx
            .send(sender.clone())
            .map_err(|error| kairo_actor::SendError::new(message, error.to_string()))
    }
}

#[test]
fn replicator_actor_handles_local_get_and_update() {
    let system = ActorSystem::builder("ddata-replicator-get-update")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (get_ref, get_rx) = forward_ref(&system, "get-replies");
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let key = ReplicatorKey::new("counter");
    let node = replica("a");

    replicator
        .tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::local(),
            reply_to: get_ref.clone(),
        })
        .unwrap();
    assert_eq!(
        get_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        GetResponse::NotFound { key: key.clone() }
    );

    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GCounter::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(move |counter| {
                counter
                    .increment(node.clone(), 4)
                    .map_err(|e| e.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();
    let update = update_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(matches!(update, UpdateResponse::Success(_)));

    replicator
        .tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::local(),
            reply_to: get_ref,
        })
        .unwrap();
    let GetResponse::Success { data, .. } = get_rx.recv_timeout(Duration::from_secs(1)).unwrap()
    else {
        panic!("counter should be available");
    };
    assert_eq!(data.value().unwrap(), 4);

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_aggregation_uses_canonical_sender_ref() {
    let system = ActorSystem::builder("ddata-replicator-canonical-aggregate")
        .build()
        .unwrap();
    let (write_ref, _write_rx) = forward_ref::<ReplicatorWrite>(&system, "remote-writes");
    let (read_ref, _read_rx) = forward_ref::<ReplicatorRead>(&system, "remote-reads");
    let (update_ref, _update_rx) = forward_ref(&system, "update-replies");
    let (write_sender_tx, write_sender_rx) = mpsc::channel();
    let (read_sender_tx, _read_sender_rx) = mpsc::channel();
    let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(AggregationTarget::new_sender_aware(
        replica("remote"),
        write_ref,
        read_ref,
        CaptureSender::<ReplicatorWrite>::new(write_sender_tx),
        CaptureSender::<ReplicatorRead>::new(read_sender_tx),
    ));
    let aggregation = ReplicatorAggregation::with_sender_remote_settings(
        transport,
        Arc::new(GCounterCodec),
        RemoteSettings::new("127.0.0.1", 25520),
    );
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || ReplicatorActor::<GCounter>::with_aggregation(aggregation)),
        )
        .unwrap();
    let key = ReplicatorKey::new("counter");

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![replica("remote")],
            unreachable: BTreeSet::new(),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key,
            initial: GCounter::new(),
            consistency: WriteConsistency::to(2, Duration::from_millis(20)).unwrap(),
            modify: Box::new(|counter| {
                counter
                    .increment(replica("local"), 5)
                    .map_err(|error| error.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();

    let sender = write_sender_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(sender.protocol(), "kairo");
    assert_eq!(sender.system(), "ddata-replicator-canonical-aggregate");
    assert_eq!(sender.host(), Some("127.0.0.1"));
    assert_eq!(sender.port(), Some(25520));
    assert!(sender.path().starts_with(
        "kairo://ddata-replicator-canonical-aggregate@127.0.0.1:25520/user/replicator#"
    ));
    assert!(sender.path().contains("/$anon-"));
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_spawns_write_session_for_non_local_update() {
    let system = ActorSystem::builder("ddata-replicator-aggregate-update")
        .build()
        .unwrap();
    let (write_ref, write_rx) = forward_ref::<crate::ReplicatorWrite>(&system, "remote-writes");
    let (read_ref, _read_rx) = forward_ref::<crate::ReplicatorRead>(&system, "remote-reads");
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(AggregationTarget::new(
        replica("remote"),
        write_ref,
        read_ref,
    ));
    let aggregation = ReplicatorAggregation::new(transport, Arc::new(GCounterCodec));
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || ReplicatorActor::<GCounter>::with_aggregation(aggregation)),
        )
        .unwrap();
    let key = ReplicatorKey::new("counter");

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![replica("remote")],
            unreachable: BTreeSet::new(),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GCounter::new(),
            consistency: WriteConsistency::to(2, Duration::from_millis(20)).unwrap(),
            modify: Box::new(|counter| {
                counter
                    .increment(replica("local"), 5)
                    .map_err(|error| error.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();

    let write = write_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(write.key, key.as_str());
    assert_eq!(write.from, Some(replica("local")));
    assert_eq!(
        update_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        UpdateResponse::Timeout { key }
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_spawns_read_session_for_non_local_get() {
    let system = ActorSystem::builder("ddata-replicator-aggregate-get")
        .build()
        .unwrap();
    let (write_ref, _write_rx) = forward_ref::<crate::ReplicatorWrite>(&system, "remote-writes");
    let (read_ref, read_rx) = forward_ref::<crate::ReplicatorRead>(&system, "remote-reads");
    let (get_ref, get_rx) = forward_ref(&system, "get-replies");
    let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(AggregationTarget::new(
        replica("remote"),
        write_ref,
        read_ref,
    ));
    let aggregation = ReplicatorAggregation::new(transport, Arc::new(GCounterCodec));
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || ReplicatorActor::<GCounter>::with_aggregation(aggregation)),
        )
        .unwrap();
    let key = ReplicatorKey::new("counter");

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![replica("remote")],
            unreachable: BTreeSet::new(),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::from(2, Duration::from_millis(20)).unwrap(),
            reply_to: get_ref,
        })
        .unwrap();

    let read = read_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(read.key, key.as_str());
    assert_eq!(read.from, Some(replica("local")));
    assert!(matches!(
        get_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        GetResponse::Failure { key: failed_key, reason }
            if failed_key == key && reason.contains("required 1")
    ));
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_sends_current_value_on_subscribe_and_flushes_later_changes() {
    let system = ActorSystem::builder("ddata-replicator-subscribe")
        .build()
        .unwrap();
    let replicator = system
        .spawn(
            "replicator",
            Props::new(ReplicatorActor::<GSet<&'static str>>::new),
        )
        .unwrap();
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let (change_ref, change_rx) = forward_ref(&system, "changes");
    let key = ReplicatorKey::new("set");

    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GSet::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(|set| Ok(set.add("a"))),
            reply_to: update_ref.clone(),
        })
        .unwrap();
    update_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    replicator
        .tell(ReplicatorActorMsg::Subscribe {
            key: key.clone(),
            subscriber: change_ref.clone(),
        })
        .unwrap();
    let current = change_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(current.key(), &key);
    assert!(current.data().contains(&"a"));

    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GSet::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(|set| Ok(set.add("b"))),
            reply_to: update_ref,
        })
        .unwrap();
    replicator.tell(ReplicatorActorMsg::FlushChanges).unwrap();

    let changed = change_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(changed.key(), &key);
    assert!(changed.data().contains(&"a"));
    assert!(changed.data().contains(&"b"));

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_can_unsubscribe_from_later_flushes() {
    let system = ActorSystem::builder("ddata-replicator-unsubscribe")
        .build()
        .unwrap();
    let replicator = system
        .spawn(
            "replicator",
            Props::new(ReplicatorActor::<GSet<&'static str>>::new),
        )
        .unwrap();
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let (change_ref, change_rx) = forward_ref(&system, "changes");
    let key = ReplicatorKey::new("set");

    replicator
        .tell(ReplicatorActorMsg::Subscribe {
            key: key.clone(),
            subscriber: change_ref.clone(),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Unsubscribe {
            key: key.clone(),
            subscriber: change_ref,
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key,
            initial: GSet::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(|set| Ok(set.add("a"))),
            reply_to: update_ref,
        })
        .unwrap();
    update_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    replicator.tell(ReplicatorActorMsg::FlushChanges).unwrap();

    assert!(change_rx.recv_timeout(Duration::from_millis(100)).is_err());

    system.terminate(Duration::from_secs(1)).unwrap();
}
