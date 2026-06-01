#[cfg(feature = "remote")]
#[derive(Debug)]
struct PreludeRemoteMsg;

#[cfg(feature = "remote")]
impl crate::prelude::RemoteMessage for PreludeRemoteMsg {
    const MANIFEST: &'static str = "kairo.facade.test.PreludeRemoteMsg";
    const VERSION: u16 = 1;
}

#[cfg(feature = "remote")]
#[test]
fn prelude_exposes_remote_entry_points() {
    use crate::prelude::*;

    fn assert_remote_outbound<T: RemoteOutbound + ?Sized>() {}

    let settings = RemoteSettings::new("127.0.0.1", 25520);
    assert_eq!(settings.canonical_hostname, "127.0.0.1");
    assert_eq!(settings.canonical_port, 25520);
    assert_remote_outbound::<dyn RemoteOutbound>();
    let _ = std::mem::size_of::<Option<RemoteActorRef<PreludeRemoteMsg>>>();
    let _ = std::mem::size_of::<Option<RemoteActorRefProvider>>();
    let _ = std::mem::size_of::<Option<TcpRemoteActorSystem<PreludeRemoteMsg>>>();
    let error = RemoteError::Outbound("send failed".to_string());
    assert!(error.to_string().contains("send failed"));
}

#[cfg(feature = "distributed-data")]
#[test]
fn prelude_exposes_distributed_data_entry_points() {
    use crate::prelude::*;

    let replica = ReplicaId::new("node-a");
    let key = ReplicatorKey::new("counters.requests");
    let mut state = ReplicatorState::<GCounter>::new();
    let outcome = state
        .update_local(key.clone(), GCounter::new(), |counter| {
            counter.increment(replica, 2)
        })
        .expect("counter update should succeed");

    assert!(outcome.changed());
    assert!(matches!(state.get_local(&key), GetResponse::Success { .. }));
    let _ = std::mem::size_of::<Option<ReplicatorActor<GCounter>>>();
    let _ = std::mem::size_of::<Option<ReplicatorActorMsg<GCounter>>>();
    let _ = std::mem::size_of::<Option<UpdateResponse<<GCounter as DeltaReplicatedData>::Delta>>>();
    let _ = std::mem::size_of::<Option<GSet<String>>>();
    let _ = std::mem::size_of::<Option<ORSet<String>>>();
    let _ = std::mem::size_of::<Option<PNCounter>>();
    let _ = ReadConsistency::Local;
    let _ = WriteConsistency::Local;
}

#[cfg(feature = "cluster-sharding")]
#[test]
fn prelude_exposes_sharding_entry_points() {
    use crate::prelude::*;

    let envelope = ShardingEnvelope::new("entity-1", "credit".to_string());
    let (entity_id, message) = envelope.into_parts();
    assert_eq!(entity_id, "entity-1");
    assert_eq!(message, "credit");
    assert_eq!(
        shard_id_for("entity-1", DEFAULT_SHARD_COUNT).expect("valid shard count"),
        default_shard_id_for("entity-1")
    );
    assert_ne!(stable_hash_entity_id("entity-1"), 0);
    let _ = EntityTypeKey::<String>::new("Account");
    let _ = std::mem::size_of::<Option<EntityRef<String>>>();
    let _ = std::mem::size_of::<Option<ShardingEnvelopeRouter<String>>>();
    let _ = std::mem::size_of::<Option<ShardRegionActor<String>>>();
    let _ = std::mem::size_of::<Option<ShardRegionMsg<String>>>();
    let _ = ShardingError::InvalidShardCount;
}

#[cfg(feature = "cluster-tools")]
#[test]
fn prelude_exposes_cluster_tools_entry_points() {
    use crate::prelude::*;

    struct NoopSingleton;

    impl Actor for NoopSingleton {
        type Msg = String;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
            Ok(())
        }
    }

    let topic = TopicName::new("events");
    assert_eq!(topic.as_str(), "events");
    assert_eq!(SingletonScope::for_role("backend").role(), Some("backend"));
    let _ = TopicPublishMode::Broadcast;
    let _ = std::mem::size_of::<Option<LocalPubSub<String>>>();
    let _ = std::mem::size_of::<Option<DistributedPubSubMediatorActor<String>>>();
    let _ = std::mem::size_of::<Option<DistributedPubSubMediatorMsg<String>>>();
    let _ = std::mem::size_of::<Option<LocalSingletonManagerActor<NoopSingleton>>>();
    let _ = std::mem::size_of::<Option<LocalSingletonManagerMsg<String>>>();
    let _ = std::mem::size_of::<Option<SingletonProxyActor<String>>>();
    let _ = std::mem::size_of::<Option<SingletonProxyMsg<String>>>();
}

#[cfg(feature = "testkit")]
#[test]
fn prelude_exposes_testkit_entry_points() -> Result<(), Box<dyn std::error::Error>> {
    use crate::prelude::*;

    let (kit, manual_time) = ActorSystemTestKit::with_manual_time("facade-testkit-prelude")?;
    let probe = kit.create_probe::<&'static str>("probe")?;
    let handle = manual_time.schedule_once(
        std::time::Duration::from_millis(1),
        probe.actor_ref(),
        "tick",
    );
    assert!(handle.cancel());
    let _ = std::mem::size_of::<Option<ManualTimeHandle>>();
    let _ = std::mem::size_of::<Option<ActorSystemTestKit>>();
    let _ = std::mem::size_of::<Option<MultiNode>>();
    let _ = std::mem::size_of::<Option<MultiNodeError>>();
    let _ = std::mem::size_of::<Option<MultiNodeResult<()>>>();
    let multi_node = MultiNodeTestKit::new(["facade-node-a", "facade-node-b"])?;
    assert_eq!(
        multi_node.node_names().collect::<Vec<_>>(),
        vec!["facade-node-a", "facade-node-b"]
    );
    let _ = std::mem::size_of::<Option<TestProbe<String>>>();
    let _ = std::mem::size_of::<Option<FishingOutcome>>();
    let _ = await_assert(
        std::time::Duration::from_millis(1),
        std::time::Duration::from_millis(1),
        || Ok::<_, &'static str>(()),
    );
    multi_node.shutdown(std::time::Duration::from_secs(1))?;
    kit.shutdown(std::time::Duration::from_secs(1))?;
    Ok(())
}
