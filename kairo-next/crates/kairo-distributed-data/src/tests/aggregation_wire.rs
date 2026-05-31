use super::*;

#[test]
fn aggregation_wire_round_trips_manifest_tagged_data_envelopes() {
    let removed = replica("removed");
    let owner = replica("local");
    let seen = replica("peer");
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(owner.clone(), 7)
            .unwrap()
            .increment(removed.clone(), 3)
            .unwrap()
            .reset_delta(),
    )
    .init_removed_node_pruning(removed.clone(), owner.clone())
    .add_pruning_seen(seen.clone());
    let key = ReplicatorKey::new("counter");
    let from = owner.clone();

    let wire_envelope = encode_data_envelope(&envelope, &GCounterCodec).unwrap();
    assert_eq!(wire_envelope.crdt_manifest, crate::GCOUNTER_MANIFEST);
    assert_eq!(wire_envelope.crdt_version, crate::CRDT_CODEC_VERSION);
    assert_eq!(wire_envelope.pruning.len(), 1);
    let decoded = decode_data_envelope(&wire_envelope, &GCounterCodec).unwrap();
    assert_eq!(decoded.data().value().unwrap(), 10);
    let PruningState::Initialized(initialized) = decoded.pruning().get(&removed).unwrap() else {
        panic!("expected initialized pruning marker");
    };
    assert_eq!(initialized.owner(), &owner);
    assert!(initialized.seen().contains(&seen));

    let write = encode_write(&key, Some(from.clone()), &envelope, &GCounterCodec).unwrap();
    assert_eq!(write.key, key.as_str());
    assert_eq!(write.from, Some(from));
    assert_eq!(write.envelope.crdt_manifest, crate::GCOUNTER_MANIFEST);

    let read_result = encode_read_result(Some(&envelope), &GCounterCodec).unwrap();
    assert_eq!(
        decode_read_result(&read_result, &GCounterCodec)
            .unwrap()
            .unwrap()
            .data()
            .value()
            .unwrap(),
        10
    );
    assert_eq!(
        decode_read_result::<GCounter, _>(
            &encode_read_result::<GCounter, _>(None, &GCounterCodec).unwrap(),
            &GCounterCodec,
        )
        .unwrap(),
        None
    );

    let wrong_manifest = crate::ReplicatorDataEnvelope {
        crdt_manifest: crate::GSET_STRING_MANIFEST.to_string(),
        crdt_version: crate::CRDT_CODEC_VERSION,
        payload: wire_envelope.payload,
        pruning: Vec::new(),
    };
    assert!(
        decode_data_envelope::<GCounter, _>(&wrong_manifest, &GCounterCodec)
            .unwrap_err()
            .to_string()
            .contains("expected CRDT manifest")
    );
}

#[test]
fn aggregation_wire_round_trips_performed_pruning_markers() {
    let removed = replica("removed");
    let envelope = DataEnvelope::new(GCounter::new().reset_delta())
        .init_removed_node_pruning(removed.clone(), replica("owner"))
        .prune_removed_node(&removed, PruningPerformed::new(123))
        .unwrap();

    let decoded = decode_data_envelope(
        &encode_data_envelope(&envelope, &GCounterCodec).unwrap(),
        &GCounterCodec,
    )
    .unwrap();

    assert_eq!(
        decoded.pruning().get(&removed),
        Some(&PruningState::Performed(PruningPerformed::new(123)))
    );
}
