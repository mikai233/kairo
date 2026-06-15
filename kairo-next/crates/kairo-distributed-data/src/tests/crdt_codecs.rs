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
}

#[test]
fn crdt_codecs_reject_trailing_payload_bytes() {
    let set = GSet::new()
        .add("b".to_string())
        .add("a".to_string())
        .reset_delta();
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

    let error = GSetStringCodec
        .deserialize(with_trailing_byte(GSetStringCodec.serialize(&set).unwrap()))
        .expect_err("trailing GSet payload byte should fail");
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
}
