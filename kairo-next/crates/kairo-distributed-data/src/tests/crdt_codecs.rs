use super::*;

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
}
