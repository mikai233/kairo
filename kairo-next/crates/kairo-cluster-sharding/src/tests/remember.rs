use super::*;

struct PartiallyFailingRememberReplicator;

impl Actor for PartiallyFailingRememberReplicator {
    type Msg = kairo_distributed_data::ReplicatorActorMsg<ORSet<String>>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        if let kairo_distributed_data::ReplicatorActorMsg::Get { key, reply_to, .. } = msg {
            let response = if key.as_str().ends_with("-2") {
                kairo_distributed_data::GetResponse::Failure {
                    key,
                    reason: "injected read failure".to_string(),
                }
            } else {
                kairo_distributed_data::GetResponse::NotFound { key }
            };
            reply_to
                .tell(response)
                .map_err(|error| ActorError::Message(error.reason().to_string()))?;
        }
        Ok(())
    }
}

#[test]
fn shard_ids_use_documented_stable_hash() {
    assert_eq!(stable_hash_entity_id("counter-1"), 0x31c4c004cce265c1);
    assert_eq!(shard_id_for("counter-1", 100).unwrap(), "65");
    assert_eq!(default_shard_id_for("counter-1"), "65");
}

#[test]
fn shard_id_rejects_zero_shards() {
    assert_eq!(
        shard_id_for("counter-1", 0),
        Err(ShardingError::InvalidShardCount)
    );
}

#[test]
fn remember_entity_keys_use_pekkos_stable_partitioning() {
    assert_eq!(remember_entity_key_index("entity-1"), 3);
    assert_eq!(remember_entity_key_index("entity-2"), 2);
    assert_eq!(remember_entity_key_index("counter-1"), 2);
    assert_eq!(
        remember_entity_shard_key("orders", "shard-1", 3).unwrap(),
        "shard-orders-shard-1-3"
    );
}

#[test]
fn remember_entity_key_helpers_reject_invalid_counts_and_indexes() {
    assert_eq!(
        remember_entity_key_index_for("entity-1", 0),
        Err(ShardingError::InvalidRememberEntityKeyCount)
    );
    assert_eq!(
        remember_entity_shard_key("orders", "shard-1", 5),
        Err(ShardingError::InvalidRememberEntityKeyIndex {
            index: 5,
            key_count: 5,
        })
    );
}

#[cfg(target_pointer_width = "64")]
#[test]
fn remember_entity_key_index_supports_counts_wider_than_u32() {
    let key_count = 1_usize << 32;
    let index = remember_entity_key_index_for("entity-1", key_count).unwrap();

    assert!(index < key_count);
}

#[test]
fn remember_shard_store_loads_and_updates_partitioned_entities() {
    let mut state = RememberShardStoreState::with_entities(
        "orders",
        "shard-1",
        ["entity-1".to_string(), "entity-2".to_string()],
    );

    assert_eq!(
        state.remembered_entities(),
        BTreeSet::from(["entity-1".to_string(), "entity-2".to_string()])
    );
    assert_eq!(
        state.entities_for_key(3),
        Some(&BTreeSet::from(["entity-1".to_string()]))
    );
    assert_eq!(
        state.entities_for_key(2),
        Some(&BTreeSet::from(["entity-2".to_string()]))
    );

    let done = state
        .apply_update(RememberShardUpdate::new(
            ["entity-3".to_string()],
            ["entity-1".to_string()],
        ))
        .unwrap();

    assert_eq!(done.started, BTreeSet::from(["entity-3".to_string()]));
    assert_eq!(done.stopped, BTreeSet::from(["entity-1".to_string()]));
    assert_eq!(
        state.remembered_entities(),
        BTreeSet::from(["entity-2".to_string(), "entity-3".to_string()])
    );

    state
        .apply_update(RememberShardUpdate::new(
            ["entity-2".to_string()],
            ["entity-2".to_string()],
        ))
        .unwrap();
    assert!(state.remembered_entities().contains("entity-2"));
}

#[test]
fn remember_shard_store_treats_stopping_unknown_entity_as_idempotent() {
    let mut state = RememberShardStoreState::new("orders", "shard-1");

    state
        .apply_update(RememberShardUpdate::new(
            std::iter::empty::<String>(),
            ["missing".to_string()],
        ))
        .unwrap();

    assert!(state.remembered_entities().is_empty());
}

#[test]
fn remember_coordinator_store_remembers_shards_additively() {
    let mut state = RememberCoordinatorStoreState::with_shards(["1".to_string()]);

    assert_eq!(state.get_shards().shards, BTreeSet::from(["1".to_string()]));
    assert_eq!(state.add_shard("2").shard, "2");
    assert_eq!(state.add_shard("2").shard, "2");
    assert_eq!(
        state.remembered_shards(),
        &BTreeSet::from(["1".to_string(), "2".to_string()])
    );
}

#[test]
fn remember_coordinator_store_actor_adds_and_lists_shards() {
    let kit = kairo_testkit::ActorSystemTestKit::new("remember-coordinator-store").unwrap();
    let store = kit
        .system()
        .spawn(
            "store",
            RememberCoordinatorStoreActor::props(RememberCoordinatorStoreState::with_shards([
                "1".to_string()
            ])),
        )
        .unwrap();
    let updates = kit
        .create_probe::<crate::RememberCoordinatorUpdateDone>("updates")
        .unwrap();
    let shards = kit
        .create_probe::<crate::RememberedShards>("shards")
        .unwrap();
    let state = kit
        .create_probe::<RememberCoordinatorStoreSnapshot>("state")
        .unwrap();

    store
        .tell(RememberCoordinatorStoreMsg::AddShard {
            shard: "2".to_string(),
            reply_to: updates.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        updates
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .shard,
        "2"
    );

    store
        .tell(RememberCoordinatorStoreMsg::GetShards {
            reply_to: shards.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        shards
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .shards,
        BTreeSet::from(["1".to_string(), "2".to_string()])
    );

    store
        .tell(RememberCoordinatorStoreMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap(),
        RememberCoordinatorStoreSnapshot {
            shards: BTreeSet::from(["1".to_string(), "2".to_string()]),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remember_coordinator_ddata_store_adds_and_loads_shards() {
    let kit = kairo_testkit::ActorSystemTestKit::new("remember-coordinator-ddata-store").unwrap();
    let replicator = kit
        .system()
        .spawn(
            "replicator",
            Props::new(ReplicatorActor::<GSet<String>>::new),
        )
        .unwrap();
    let store = kit
        .system()
        .spawn(
            "store",
            RememberCoordinatorDDataStoreActor::props("orders", replicator),
        )
        .unwrap();
    let updates = kit
        .create_probe::<Result<crate::RememberCoordinatorUpdateDone, ShardingError>>("updates")
        .unwrap();
    let shards = kit
        .create_probe::<Result<crate::RememberedShards, ShardingError>>("shards")
        .unwrap();
    let state = kit
        .create_probe::<RememberCoordinatorDDataStoreSnapshot>("state")
        .unwrap();

    store
        .tell(RememberCoordinatorDDataStoreMsg::GetShards {
            reply_to: shards.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        shards.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(crate::RememberedShards {
            shards: BTreeSet::new(),
        })
    );

    store
        .tell(RememberCoordinatorDDataStoreMsg::AddShard {
            shard: "1".to_string(),
            reply_to: updates.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        updates.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(crate::RememberCoordinatorUpdateDone {
            shard: "1".to_string(),
        })
    );

    store
        .tell(RememberCoordinatorDDataStoreMsg::AddShard {
            shard: "1".to_string(),
            reply_to: updates.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        updates.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(crate::RememberCoordinatorUpdateDone {
            shard: "1".to_string(),
        })
    );

    store
        .tell(RememberCoordinatorDDataStoreMsg::AddShard {
            shard: "2".to_string(),
            reply_to: updates.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        updates.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(crate::RememberCoordinatorUpdateDone {
            shard: "2".to_string(),
        })
    );

    store
        .tell(RememberCoordinatorDDataStoreMsg::GetShards {
            reply_to: shards.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        shards.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(crate::RememberedShards {
            shards: BTreeSet::from(["1".to_string(), "2".to_string()]),
        })
    );

    store
        .tell(RememberCoordinatorDDataStoreMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap(),
        RememberCoordinatorDDataStoreSnapshot {
            type_name: "orders".to_string(),
            key: remember_coordinator_shards_key("orders")
                .as_str()
                .to_string(),
            read_consistency: kairo_distributed_data::ReadConsistency::local(),
            write_consistency: kairo_distributed_data::WriteConsistency::local(),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remember_coordinator_orset_ddata_store_adds_and_loads_shards() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("remember-coordinator-orset-ddata-store").unwrap();
    let replicator = kit
        .system()
        .spawn(
            "replicator",
            Props::new(ReplicatorActor::<ORSet<String>>::new),
        )
        .unwrap();
    let store = kit
        .system()
        .spawn(
            "store",
            RememberCoordinatorORSetDDataStoreActor::props(
                "orders",
                ReplicaId::new("node-a"),
                replicator,
            ),
        )
        .unwrap();
    let updates = kit
        .create_probe::<Result<crate::RememberCoordinatorUpdateDone, ShardingError>>("updates")
        .unwrap();
    let shards = kit
        .create_probe::<Result<crate::RememberedShards, ShardingError>>("shards")
        .unwrap();

    for shard in ["1", "1", "2"] {
        store
            .tell(RememberCoordinatorDDataStoreMsg::AddShard {
                shard: shard.to_string(),
                reply_to: updates.actor_ref(),
            })
            .unwrap();
        assert_eq!(
            updates.expect_msg(Duration::from_millis(500)).unwrap(),
            Ok(crate::RememberCoordinatorUpdateDone {
                shard: shard.to_string(),
            })
        );
    }
    store
        .tell(RememberCoordinatorDDataStoreMsg::GetShards {
            reply_to: shards.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        shards.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(crate::RememberedShards {
            shards: BTreeSet::from(["1".to_string(), "2".to_string()]),
        })
    );

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remember_coordinator_ddata_store_updates_emit_gset_delta_wire() {
    let kit = kairo_testkit::ActorSystemTestKit::new("remember-coordinator-ddata-store-delta-wire")
        .unwrap();
    let delta_target = kit
        .create_probe::<ReplicatorDeltaPropagation>("delta-target")
        .unwrap();
    let mut transport =
        DeltaPropagationTransport::new(ReplicaId::new("node-a"), GSetStringDeltaCodec);
    transport.insert_target(DeltaPropagationTarget::new(
        ReplicaId::new("node-b"),
        delta_target.actor_ref(),
    ));
    let delta_loop = DeltaPropagationLoop::new(transport).with_cleanup_every_ticks(1);
    let replicator = kit
        .system()
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GSet<String>>::with_delta_propagation_loop(delta_loop)
            }),
        )
        .unwrap();
    replicator
        .tell(kairo_distributed_data::ReplicatorActorMsg::SetDeltaNodes {
            nodes: vec![ReplicaId::new("node-b")],
        })
        .unwrap();
    let store = kit
        .system()
        .spawn(
            "store",
            RememberCoordinatorDDataStoreActor::props("orders", replicator.clone()),
        )
        .unwrap();
    let shards = kit
        .create_probe::<Result<crate::RememberedShards, ShardingError>>("shards")
        .unwrap();
    let updates = kit
        .create_probe::<Result<crate::RememberCoordinatorUpdateDone, ShardingError>>("updates")
        .unwrap();
    let ticks = kit
        .create_probe::<DeltaPropagationTickReport>("ticks")
        .unwrap();

    store
        .tell(RememberCoordinatorDDataStoreMsg::GetShards {
            reply_to: shards.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        shards.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(crate::RememberedShards {
            shards: BTreeSet::new(),
        })
    );

    store
        .tell(RememberCoordinatorDDataStoreMsg::AddShard {
            shard: "1".to_string(),
            reply_to: updates.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        updates.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(crate::RememberCoordinatorUpdateDone {
            shard: "1".to_string(),
        })
    );

    replicator
        .tell(
            kairo_distributed_data::ReplicatorActorMsg::RunDeltaPropagation {
                reply_to: ticks.actor_ref(),
            },
        )
        .unwrap();
    let tick = ticks.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(tick.propagation_count(), 1);
    assert_eq!(tick.transport().sent_to(), &[ReplicaId::new("node-b")]);

    let outbound = delta_target.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(outbound.from, ReplicaId::new("node-a"));
    assert_eq!(outbound.deltas.len(), 1);
    assert_eq!(
        outbound.deltas[0].key,
        remember_coordinator_shards_key("orders").as_str()
    );
    assert_eq!(
        outbound.deltas[0].crdt_manifest,
        kairo_distributed_data::GSET_STRING_DELTA_MANIFEST
    );
    let decoded =
        kairo_distributed_data::decode_delta_propagation(&outbound, &GSetStringDeltaCodec).unwrap();
    assert_eq!(decoded.len(), 1);
    assert!(
        decoded[0]
            .delta()
            .zero()
            .merge_delta(decoded[0].delta())
            .contains(&"1".to_string())
    );

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remember_shard_ddata_store_updates_and_reloads_entities() {
    let kit = kairo_testkit::ActorSystemTestKit::new("remember-shard-ddata-store").unwrap();
    let replicator = kit
        .system()
        .spawn(
            "replicator",
            Props::new(ReplicatorActor::<ORSet<String>>::new),
        )
        .unwrap();
    let store = kit
        .system()
        .spawn(
            "store",
            RememberShardDDataStoreActor::props(
                "orders",
                "shard-1",
                ReplicaId::new("node-a"),
                replicator.clone(),
            ),
        )
        .unwrap();
    let updates = kit
        .create_probe::<Result<crate::RememberShardUpdateDone, ShardingError>>("updates")
        .unwrap();
    let entities = kit
        .create_probe::<Result<RememberedEntities, ShardingError>>("entities")
        .unwrap();
    let state = kit
        .create_probe::<RememberShardDDataStoreSnapshot>("state")
        .unwrap();

    store
        .tell(RememberShardDDataStoreMsg::GetEntities {
            reply_to: entities.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        entities.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(RememberedEntities {
            entities: BTreeSet::new(),
        })
    );

    store
        .tell(RememberShardDDataStoreMsg::Update {
            update: RememberShardUpdate::new(
                ["entity-1".to_string(), "entity-2".to_string()],
                std::iter::empty::<String>(),
            ),
            reply_to: updates.actor_ref(),
        })
        .unwrap();
    let started = updates
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();
    assert_eq!(
        started.started,
        BTreeSet::from(["entity-1".to_string(), "entity-2".to_string()])
    );
    assert!(started.stopped.is_empty());

    store
        .tell(RememberShardDDataStoreMsg::Update {
            update: RememberShardUpdate::new(
                ["entity-3".to_string()],
                ["entity-1".to_string(), "missing".to_string()],
            ),
            reply_to: updates.actor_ref(),
        })
        .unwrap();
    let changed = updates
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();
    assert_eq!(changed.started, BTreeSet::from(["entity-3".to_string()]));
    assert_eq!(
        changed.stopped,
        BTreeSet::from(["entity-1".to_string(), "missing".to_string()])
    );

    store
        .tell(RememberShardDDataStoreMsg::GetEntities {
            reply_to: entities.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        entities.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(RememberedEntities {
            entities: BTreeSet::from(["entity-2".to_string(), "entity-3".to_string()]),
        })
    );

    store
        .tell(RememberShardDDataStoreMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(snapshot.type_name, "orders");
    assert_eq!(snapshot.shard_id, "shard-1");
    assert!(snapshot.loaded);
    assert!(snapshot.pending_load_keys.is_empty());
    assert_eq!(snapshot.pending_updates, 0);
    assert_eq!(
        snapshot
            .entities_by_key
            .values()
            .flat_map(|ids| ids.iter().cloned())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["entity-2".to_string(), "entity-3".to_string()])
    );

    let reloaded = kit
        .system()
        .spawn(
            "store-reloaded",
            RememberShardDDataStoreActor::props(
                "orders",
                "shard-1",
                ReplicaId::new("node-a"),
                replicator,
            ),
        )
        .unwrap();
    reloaded
        .tell(RememberShardDDataStoreMsg::GetEntities {
            reply_to: entities.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        entities.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(RememberedEntities {
            entities: BTreeSet::from(["entity-2".to_string(), "entity-3".to_string()]),
        })
    );

    assert_eq!(
        remember_entity_shard_replicator_key("orders", "shard-1", 2)
            .unwrap()
            .as_str(),
        "shard-orders-shard-1-2"
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remember_shard_ddata_store_never_exposes_partial_state_after_load_failure() {
    let kit = kairo_testkit::ActorSystemTestKit::new("remember-shard-ddata-load-failure").unwrap();
    let replicator = kit
        .system()
        .spawn(
            "replicator",
            Props::new(|| PartiallyFailingRememberReplicator),
        )
        .unwrap();
    let store = kit
        .system()
        .spawn(
            "store",
            RememberShardDDataStoreActor::props(
                "orders",
                "shard-1",
                ReplicaId::new("node-a"),
                replicator,
            ),
        )
        .unwrap();
    let entities = kit
        .create_probe::<Result<RememberedEntities, ShardingError>>("entities")
        .unwrap();
    let updates = kit
        .create_probe::<Result<RememberShardUpdateDone, ShardingError>>("updates")
        .unwrap();
    let state = kit
        .create_probe::<RememberShardDDataStoreSnapshot>("state")
        .unwrap();
    let expected = ShardingError::RememberStoreReadFailed {
        key: "shard-orders-shard-1-2".to_string(),
        reason: "injected read failure".to_string(),
    };

    for _ in 0..2 {
        store
            .tell(RememberShardDDataStoreMsg::GetEntities {
                reply_to: entities.actor_ref(),
            })
            .unwrap();
        assert_eq!(
            entities.expect_msg(Duration::from_millis(500)).unwrap(),
            Err(expected.clone())
        );
    }
    store
        .tell(RememberShardDDataStoreMsg::Update {
            update: RememberShardUpdate::new(
                ["entity-1".to_string()],
                std::iter::empty::<String>(),
            ),
            reply_to: updates.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        updates.expect_msg(Duration::from_millis(500)).unwrap(),
        Err(expected.clone())
    );
    store
        .tell(RememberShardDDataStoreMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert!(!snapshot.loaded);
    assert!(snapshot.pending_load_keys.is_empty());
    assert_eq!(snapshot.load_error, Some(expected));
    assert!(snapshot.entities_by_key.values().all(BTreeSet::is_empty));

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remember_shard_ddata_store_updates_emit_orset_delta_wire() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("remember-shard-ddata-store-delta-wire").unwrap();
    let delta_target = kit
        .create_probe::<ReplicatorDeltaPropagation>("delta-target")
        .unwrap();
    let mut transport =
        DeltaPropagationTransport::new(ReplicaId::new("node-a"), ORSetStringDeltaCodec);
    transport.insert_target(DeltaPropagationTarget::new(
        ReplicaId::new("node-b"),
        delta_target.actor_ref(),
    ));
    let delta_loop = DeltaPropagationLoop::new(transport).with_cleanup_every_ticks(1);
    let replicator = kit
        .system()
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<ORSet<String>>::with_delta_propagation_loop(delta_loop)
            }),
        )
        .unwrap();
    replicator
        .tell(kairo_distributed_data::ReplicatorActorMsg::SetDeltaNodes {
            nodes: vec![ReplicaId::new("node-b")],
        })
        .unwrap();
    let store = kit
        .system()
        .spawn(
            "store",
            RememberShardDDataStoreActor::props(
                "orders",
                "shard-1",
                ReplicaId::new("node-a"),
                replicator.clone(),
            ),
        )
        .unwrap();
    let entities = kit
        .create_probe::<Result<RememberedEntities, ShardingError>>("entities")
        .unwrap();
    let updates = kit
        .create_probe::<Result<crate::RememberShardUpdateDone, ShardingError>>("updates")
        .unwrap();
    let ticks = kit
        .create_probe::<DeltaPropagationTickReport>("ticks")
        .unwrap();
    let entity = "entity-1".to_string();

    store
        .tell(RememberShardDDataStoreMsg::GetEntities {
            reply_to: entities.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        entities.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(RememberedEntities {
            entities: BTreeSet::new(),
        })
    );

    store
        .tell(RememberShardDDataStoreMsg::Update {
            update: RememberShardUpdate::new([entity.clone()], std::iter::empty::<String>()),
            reply_to: updates.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        updates.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(crate::RememberShardUpdateDone {
            started: BTreeSet::from([entity.clone()]),
            stopped: BTreeSet::new(),
        })
    );

    replicator
        .tell(
            kairo_distributed_data::ReplicatorActorMsg::RunDeltaPropagation {
                reply_to: ticks.actor_ref(),
            },
        )
        .unwrap();
    let tick = ticks.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(tick.propagation_count(), 1);
    assert_eq!(tick.transport().sent_to(), &[ReplicaId::new("node-b")]);

    let outbound = delta_target.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(outbound.from, ReplicaId::new("node-a"));
    assert_eq!(outbound.deltas.len(), 1);
    assert_eq!(
        outbound.deltas[0].key,
        remember_entity_shard_replicator_key(
            "orders",
            "shard-1",
            remember_entity_key_index(&entity)
        )
        .unwrap()
        .as_str()
    );
    assert_eq!(
        outbound.deltas[0].crdt_manifest,
        kairo_distributed_data::ORSET_STRING_DELTA_MANIFEST
    );
    let decoded =
        kairo_distributed_data::decode_delta_propagation(&outbound, &ORSetStringDeltaCodec)
            .unwrap();
    assert_eq!(decoded.len(), 1);
    assert!(
        decoded[0]
            .delta()
            .zero()
            .merge_delta(decoded[0].delta())
            .contains(&entity)
    );

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remember_shard_store_actor_updates_and_lists_entities() {
    let kit = kairo_testkit::ActorSystemTestKit::new("remember-shard-store").unwrap();
    let store = kit
        .system()
        .spawn(
            "store",
            RememberShardStoreActor::props(RememberShardStoreState::with_entities(
                "orders",
                "shard-1",
                ["entity-1".to_string(), "entity-2".to_string()],
            )),
        )
        .unwrap();
    let updates = kit
        .create_probe::<Result<crate::RememberShardUpdateDone, ShardingError>>("updates")
        .unwrap();
    let entities = kit.create_probe::<RememberedEntities>("entities").unwrap();
    let state = kit
        .create_probe::<RememberShardStoreSnapshot>("state")
        .unwrap();

    store
        .tell(RememberShardStoreMsg::Update {
            update: RememberShardUpdate::new(["entity-3".to_string()], ["entity-1".to_string()]),
            reply_to: updates.actor_ref(),
        })
        .unwrap();
    let done = updates
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();
    assert_eq!(done.started, BTreeSet::from(["entity-3".to_string()]));
    assert_eq!(done.stopped, BTreeSet::from(["entity-1".to_string()]));

    store
        .tell(RememberShardStoreMsg::GetEntities {
            reply_to: entities.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        entities.expect_msg(Duration::from_millis(500)).unwrap(),
        RememberedEntities {
            entities: BTreeSet::from(["entity-2".to_string(), "entity-3".to_string()]),
        }
    );

    store
        .tell(RememberShardStoreMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(snapshot.type_name, "orders");
    assert_eq!(snapshot.shard_id, "shard-1");
    let remembered: BTreeSet<_> = snapshot
        .entities_by_key
        .values()
        .flat_map(|entities| entities.iter().cloned())
        .collect();
    assert_eq!(
        remembered,
        BTreeSet::from(["entity-2".to_string(), "entity-3".to_string()])
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
