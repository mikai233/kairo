use super::*;
use bytes::Bytes;

fn with_trailing_byte(serialized: crate::SerializedCrdt) -> crate::SerializedCrdt {
    let mut payload = serialized.payload().to_vec();
    payload.push(0xff);
    crate::SerializedCrdt::new(
        serialized.manifest(),
        serialized.version(),
        Bytes::from(payload),
    )
}

#[test]
fn crdt_codecs_round_trip_gset_strings_in_stable_order() {
    let data = GSet::new()
        .add("b".to_string())
        .add("a".to_string())
        .reset_delta();

    let serialized = GSetStringCodec.serialize(&data).unwrap();
    let serialized_again = GSetStringCodec.serialize(&data).unwrap();

    assert_eq!(serialized.manifest(), crate::GSET_STRING_MANIFEST);
    assert_eq!(serialized.payload(), serialized_again.payload());
    assert_eq!(GSetStringCodec.deserialize(serialized).unwrap(), data);
}

#[test]
fn crdt_codecs_round_trip_gset_string_delta() {
    let data = GSet::new()
        .add("b".to_string())
        .add("a".to_string())
        .delta()
        .unwrap();

    let serialized = GSetStringDeltaCodec.serialize(&data).unwrap();
    let serialized_again = GSetStringDeltaCodec.serialize(&data).unwrap();

    assert_eq!(serialized.manifest(), crate::GSET_STRING_DELTA_MANIFEST);
    assert_eq!(serialized.payload(), serialized_again.payload());

    let decoded = GSetStringDeltaCodec.deserialize(serialized).unwrap();
    assert_eq!(decoded, data);
    assert_eq!(
        decoded.zero().merge_delta(&decoded).elements(),
        &BTreeSet::from(["a".to_string(), "b".to_string()])
    );
}

#[test]
fn crdt_codecs_round_trip_gcounter_by_sorted_replica_ids() {
    let data = GCounter::new()
        .increment(replica("b"), 2)
        .unwrap()
        .increment(replica("a"), 5)
        .unwrap()
        .reset_delta();

    let serialized = GCounterCodec.serialize(&data).unwrap();
    let serialized_again = GCounterCodec.serialize(&data).unwrap();

    assert_eq!(serialized.manifest(), crate::GCOUNTER_MANIFEST);
    assert_eq!(serialized.payload(), serialized_again.payload());
    assert_eq!(GCounterCodec.deserialize(serialized).unwrap(), data);
}

#[test]
fn crdt_codecs_round_trip_pncounter() {
    let data = PNCounter::new()
        .increment(replica("a"), 7)
        .unwrap()
        .decrement(replica("b"), 4)
        .unwrap()
        .reset_delta();

    let serialized = PNCounterCodec.serialize(&data).unwrap();

    assert_eq!(serialized.manifest(), crate::PNCOUNTER_MANIFEST);
    assert_eq!(PNCounterCodec.deserialize(serialized).unwrap(), data);
}

#[test]
fn crdt_codecs_round_trip_lww_register_string() {
    let data = LWWRegister::new(replica("b"), "value".to_string(), -42);

    let serialized = LWWRegisterStringCodec.serialize(&data).unwrap();
    let serialized_again = LWWRegisterStringCodec.serialize(&data).unwrap();

    assert_eq!(serialized.manifest(), crate::LWW_REGISTER_STRING_MANIFEST);
    assert_eq!(serialized.payload(), serialized_again.payload());
    assert_eq!(
        LWWRegisterStringCodec.deserialize(serialized).unwrap(),
        data
    );

    let positive = LWWRegister::new(replica("a"), "later".to_string(), 42);
    assert_eq!(
        LWWRegisterStringCodec
            .deserialize(LWWRegisterStringCodec.serialize(&positive).unwrap())
            .unwrap(),
        positive
    );
}

#[test]
fn crdt_codecs_round_trip_orset_strings_with_dots() {
    let entity = "entity".to_string();
    let other = "other".to_string();
    let left = ORSet::new().add(replica("b"), entity.clone()).reset_delta();
    let right = ORSet::new()
        .add(replica("a"), entity.clone())
        .add(replica("a"), other)
        .reset_delta();
    let data = left.merge(&right).reset_delta();

    let serialized = ORSetStringCodec.serialize(&data).unwrap();
    let serialized_again = ORSetStringCodec.serialize(&data).unwrap();

    assert_eq!(serialized.manifest(), crate::ORSET_STRING_MANIFEST);
    assert_eq!(serialized.payload(), serialized_again.payload());

    let decoded = ORSetStringCodec.deserialize(serialized).unwrap();
    assert_eq!(decoded, data);
    assert_eq!(decoded.dots_for(&entity).unwrap().len(), 2);
}

#[test]
fn crdt_codecs_orset_strings_preserve_removal_context() {
    let entity = "entity".to_string();
    let added = ORSet::new().add(replica("a"), entity.clone()).reset_delta();
    let removed = added.remove(replica("b"), &entity).reset_delta();

    let decoded_removed = ORSetStringCodec
        .deserialize(ORSetStringCodec.serialize(&removed).unwrap())
        .unwrap();

    assert_eq!(decoded_removed, removed);
    assert!(!decoded_removed.merge(&added).contains(&entity));
}

#[test]
fn crdt_codecs_round_trip_orset_string_add_delta() {
    let entity = "entity".to_string();
    let data = ORSet::new()
        .add(replica("a"), entity.clone())
        .delta()
        .unwrap();

    let serialized = ORSetStringDeltaCodec.serialize(&data).unwrap();
    let serialized_again = ORSetStringDeltaCodec.serialize(&data).unwrap();

    assert_eq!(serialized.manifest(), crate::ORSET_STRING_DELTA_MANIFEST);
    assert_eq!(serialized.payload(), serialized_again.payload());

    let decoded = ORSetStringDeltaCodec.deserialize(serialized).unwrap();
    assert_eq!(decoded, data);
    assert!(decoded.zero().merge_delta(&decoded).contains(&entity));
}

#[test]
fn crdt_codecs_orset_string_remove_delta_preserves_seen_context() {
    let entity = "entity".to_string();
    let added = ORSet::new().add(replica("a"), entity.clone()).reset_delta();
    let data = added.remove(replica("b"), &entity).delta().unwrap();

    let decoded = ORSetStringDeltaCodec
        .deserialize(ORSetStringDeltaCodec.serialize(&data).unwrap())
        .unwrap();

    assert_eq!(decoded, data);
    assert!(!added.merge_delta(&decoded).contains(&entity));
}

#[test]
fn crdt_codecs_round_trip_orset_string_delta_group() {
    let entity = "entity".to_string();
    let added = ORSet::new().add(replica("a"), entity.clone()).reset_delta();
    let add_delta = ORSet::new()
        .add(replica("a"), entity.clone())
        .delta()
        .unwrap();
    let remove_delta = added.remove(replica("b"), &entity).delta().unwrap();
    let data = add_delta.merge(&remove_delta);

    let decoded = ORSetStringDeltaCodec
        .deserialize(ORSetStringDeltaCodec.serialize(&data).unwrap())
        .unwrap();

    assert_eq!(decoded, data);
    assert!(!decoded.zero().merge_delta(&decoded).contains(&entity));
}

#[test]
fn crdt_codecs_round_trip_ormap_string_gset_full_state() {
    let data = ORMap::new()
        .updated(replica("b"), "users".to_string(), GSet::new(), |set| {
            set.add("bob".to_string())
        })
        .updated(replica("a"), "users".to_string(), GSet::new(), |set| {
            set.add("alice".to_string())
        })
        .updated(replica("a"), "rooms".to_string(), GSet::new(), |set| {
            set.add("green".to_string())
        })
        .reset_delta();

    let serialized = ORMapStringGSetCodec.serialize(&data).unwrap();
    let serialized_again = ORMapStringGSetCodec.serialize(&data).unwrap();

    assert_eq!(serialized.manifest(), crate::ORMAP_STRING_GSET_MANIFEST);
    assert_eq!(serialized.payload(), serialized_again.payload());

    let decoded = ORMapStringGSetCodec.deserialize(serialized).unwrap();
    assert_eq!(decoded.keys(), data.keys());
    assert_eq!(
        *decoded.get(&"users".to_string()).unwrap().elements(),
        BTreeSet::from(["alice".to_string(), "bob".to_string()])
    );
    assert_eq!(
        *decoded.get(&"rooms".to_string()).unwrap().elements(),
        BTreeSet::from(["green".to_string()])
    );
}

#[test]
fn crdt_codecs_round_trip_ormap_string_gset_put_update_remove_and_group_deltas() {
    let users = "users".to_string();
    let rooms = "rooms".to_string();
    let base = ORMap::new()
        .updated(replica("a"), users.clone(), GSet::new(), |set| {
            set.add("alice".to_string())
        })
        .reset_delta();
    let put_delta = ORMap::new()
        .put(
            replica("a"),
            rooms.clone(),
            GSet::new().add("green".to_string()),
        )
        .delta()
        .unwrap();
    let update_delta = base
        .updated(replica("b"), users.clone(), GSet::new(), |set| {
            set.add("bob".to_string())
        })
        .delta()
        .unwrap();
    let remove_delta = base.remove(replica("c"), &users).delta().unwrap();
    let group_delta = put_delta.clone().merge(&update_delta).merge(&remove_delta);

    for delta in [&put_delta, &update_delta, &remove_delta, &group_delta] {
        let serialized = ORMapStringGSetDeltaCodec.serialize(delta).unwrap();
        let serialized_again = ORMapStringGSetDeltaCodec.serialize(delta).unwrap();

        assert_eq!(
            serialized.manifest(),
            crate::ORMAP_STRING_GSET_DELTA_MANIFEST
        );
        assert_eq!(serialized.payload(), serialized_again.payload());
    }

    let decoded_put = ORMapStringGSetDeltaCodec
        .deserialize(ORMapStringGSetDeltaCodec.serialize(&put_delta).unwrap())
        .unwrap();
    assert_eq!(
        *decoded_put
            .zero()
            .merge_delta(&decoded_put)
            .get(&rooms)
            .unwrap()
            .elements(),
        BTreeSet::from(["green".to_string()])
    );

    let decoded_update = ORMapStringGSetDeltaCodec
        .deserialize(ORMapStringGSetDeltaCodec.serialize(&update_delta).unwrap())
        .unwrap();
    assert_eq!(
        *base
            .merge_delta(&decoded_update)
            .get(&users)
            .unwrap()
            .elements(),
        BTreeSet::from(["alice".to_string(), "bob".to_string()])
    );

    let decoded_remove = ORMapStringGSetDeltaCodec
        .deserialize(ORMapStringGSetDeltaCodec.serialize(&remove_delta).unwrap())
        .unwrap();
    assert!(!base.merge_delta(&decoded_remove).contains_key(&users));

    let decoded_group = ORMapStringGSetDeltaCodec
        .deserialize(ORMapStringGSetDeltaCodec.serialize(&group_delta).unwrap())
        .unwrap();
    let expected_merged = base.merge_delta(&group_delta);
    let merged = base.merge_delta(&decoded_group);
    assert_eq!(merged.keys(), expected_merged.keys());
    for key in expected_merged.keys() {
        assert_eq!(
            merged.get(&key).unwrap().elements(),
            expected_merged.get(&key).unwrap().elements()
        );
    }
}

#[test]
fn crdt_codecs_reject_wrong_manifest_and_unknown_version() {
    let data = GCounter::new().increment(replica("a"), 1).unwrap();
    let serialized = GCounterCodec.serialize(&data).unwrap();
    let wrong_manifest = crate::SerializedCrdt::new(
        crate::GSET_STRING_MANIFEST,
        serialized.version(),
        serialized.payload().clone(),
    );
    let wrong_version = crate::SerializedCrdt::new(
        crate::GCOUNTER_MANIFEST,
        crate::CRDT_CODEC_VERSION + 1,
        serialized.payload().clone(),
    );

    assert!(
        GCounterCodec
            .deserialize(wrong_manifest)
            .unwrap_err()
            .to_string()
            .contains("expected CRDT manifest")
    );
    assert!(
        GCounterCodec
            .deserialize(wrong_version)
            .unwrap_err()
            .to_string()
            .contains("unsupported")
    );

    let invalid_delta_tag = crate::SerializedCrdt::new(
        crate::ORSET_STRING_DELTA_MANIFEST,
        crate::CRDT_CODEC_VERSION,
        Bytes::from_static(&[0xff]),
    );
    assert!(
        ORSetStringDeltaCodec
            .deserialize(invalid_delta_tag)
            .unwrap_err()
            .to_string()
            .contains("unknown ORSet delta operation tag")
    );

    let gset_delta_wrong_version = crate::SerializedCrdt::new(
        crate::GSET_STRING_DELTA_MANIFEST,
        crate::CRDT_CODEC_VERSION + 1,
        GSetStringDeltaCodec
            .serialize(&GSet::new().add("a".to_string()).delta().unwrap())
            .unwrap()
            .payload()
            .clone(),
    );
    assert!(
        GSetStringDeltaCodec
            .deserialize(gset_delta_wrong_version)
            .unwrap_err()
            .to_string()
            .contains("unsupported")
    );

    let ormap_delta_wrong_version = crate::SerializedCrdt::new(
        crate::ORMAP_STRING_GSET_DELTA_MANIFEST,
        crate::CRDT_CODEC_VERSION + 1,
        ORMapStringGSetDeltaCodec
            .serialize(
                &ORMap::new()
                    .put(
                        replica("a"),
                        "rooms".to_string(),
                        GSet::new().add("green".to_string()),
                    )
                    .delta()
                    .unwrap(),
            )
            .unwrap()
            .payload()
            .clone(),
    );
    assert!(
        ORMapStringGSetDeltaCodec
            .deserialize(ormap_delta_wrong_version)
            .unwrap_err()
            .to_string()
            .contains("unsupported")
    );

    let invalid_ormap_delta_tag = crate::SerializedCrdt::new(
        crate::ORMAP_STRING_GSET_DELTA_MANIFEST,
        crate::CRDT_CODEC_VERSION,
        Bytes::from_static(&[0xff]),
    );
    assert!(
        ORMapStringGSetDeltaCodec
            .deserialize(invalid_ormap_delta_tag)
            .unwrap_err()
            .to_string()
            .contains("unknown ORMap delta operation tag")
    );
}

#[test]
fn crdt_codecs_reject_trailing_payload_bytes() {
    let set = GSet::new()
        .add("b".to_string())
        .add("a".to_string())
        .reset_delta();
    let set_delta = GSet::new()
        .add("delta".to_string())
        .delta()
        .expect("GSet add should record a delta");
    let counter = GCounter::new()
        .increment(replica("a"), 5)
        .unwrap()
        .reset_delta();
    let pn_counter = PNCounter::new()
        .increment(replica("a"), 7)
        .unwrap()
        .decrement(replica("b"), 4)
        .unwrap()
        .reset_delta();
    let register = LWWRegister::new(replica("a"), "value".to_string(), -7);
    let or_set = ORSet::new()
        .add(replica("a"), "value".to_string())
        .reset_delta();
    let or_set_delta = ORSet::new()
        .add(replica("a"), "delta".to_string())
        .delta()
        .unwrap();
    let or_map = ORMap::new()
        .updated(replica("a"), "key".to_string(), GSet::new(), |set| {
            set.add("value".to_string())
        })
        .reset_delta();
    let or_map_delta = ORMap::new()
        .put(
            replica("a"),
            "key".to_string(),
            GSet::new().add("value".to_string()),
        )
        .delta()
        .unwrap();

    let error = GSetStringCodec
        .deserialize(with_trailing_byte(GSetStringCodec.serialize(&set).unwrap()))
        .expect_err("trailing GSet payload byte should fail");
    assert!(error.to_string().contains("trailing byte"));

    let error = GSetStringDeltaCodec
        .deserialize(with_trailing_byte(
            GSetStringDeltaCodec.serialize(&set_delta).unwrap(),
        ))
        .expect_err("trailing GSet delta payload byte should fail");
    assert!(error.to_string().contains("trailing byte"));

    let error = GCounterCodec
        .deserialize(with_trailing_byte(
            GCounterCodec.serialize(&counter).unwrap(),
        ))
        .expect_err("trailing GCounter payload byte should fail");
    assert!(error.to_string().contains("trailing byte"));

    let error = PNCounterCodec
        .deserialize(with_trailing_byte(
            PNCounterCodec.serialize(&pn_counter).unwrap(),
        ))
        .expect_err("trailing PNCounter payload byte should fail");
    assert!(error.to_string().contains("trailing byte"));

    let error = LWWRegisterStringCodec
        .deserialize(with_trailing_byte(
            LWWRegisterStringCodec.serialize(&register).unwrap(),
        ))
        .expect_err("trailing LWWRegister payload byte should fail");
    assert!(error.to_string().contains("trailing byte"));

    let error = ORSetStringCodec
        .deserialize(with_trailing_byte(
            ORSetStringCodec.serialize(&or_set).unwrap(),
        ))
        .expect_err("trailing ORSet payload byte should fail");
    assert!(error.to_string().contains("trailing byte"));

    let error = ORSetStringDeltaCodec
        .deserialize(with_trailing_byte(
            ORSetStringDeltaCodec.serialize(&or_set_delta).unwrap(),
        ))
        .expect_err("trailing ORSet delta payload byte should fail");
    assert!(error.to_string().contains("trailing byte"));

    let error = ORMapStringGSetCodec
        .deserialize(with_trailing_byte(
            ORMapStringGSetCodec.serialize(&or_map).unwrap(),
        ))
        .err()
        .expect("trailing ORMap payload byte should fail");
    assert!(error.to_string().contains("trailing byte"));

    let error = ORMapStringGSetDeltaCodec
        .deserialize(with_trailing_byte(
            ORMapStringGSetDeltaCodec.serialize(&or_map_delta).unwrap(),
        ))
        .expect_err("trailing ORMap delta payload byte should fail");
    assert!(error.to_string().contains("trailing byte"));
}
