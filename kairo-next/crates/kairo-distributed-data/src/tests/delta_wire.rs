use super::*;

#[test]
fn delta_wire_encodes_manifest_tagged_propagation_entries() {
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let local = replica("local");
    let mut log = DeltaPropagationLog::new([remote]);
    log.record_delta(key.clone(), Some(delta_counter("a", 1)));
    log.record_delta(key.clone(), Some(delta_counter("b", 2)));
    let propagation = log
        .collect_propagations()
        .remove(&replica("remote"))
        .unwrap();

    let wire = encode_delta_propagation(local.clone(), true, &propagation, &GCounterCodec).unwrap();

    assert_eq!(wire.from, local);
    assert!(wire.reply);
    assert_eq!(wire.deltas.len(), 1);
    assert_eq!(wire.deltas[0].key, key.as_str());
    assert_eq!(wire.deltas[0].crdt_manifest, crate::GCOUNTER_MANIFEST);
    assert_eq!(wire.deltas[0].crdt_version, crate::CRDT_CODEC_VERSION);
    assert_eq!(wire.deltas[0].from_version, 1);
    assert_eq!(wire.deltas[0].to_version, 2);

    let decoded = decode_delta_propagation(&wire, &GCounterCodec).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].key(), &key);
    assert_eq!(decoded[0].from_version(), 1);
    assert_eq!(decoded[0].to_version(), 2);
    assert_eq!(decoded[0].delta().value().unwrap(), 3);
}

#[test]
fn delta_wire_rejects_unregistered_crdt_manifest_for_codec() {
    let wire_delta = crate::ReplicatorDelta {
        key: "counter".to_string(),
        crdt_manifest: "kairo.ddata.some-other-crdt".to_string(),
        crdt_version: crate::CRDT_CODEC_VERSION,
        payload: bytes::Bytes::new(),
        from_version: 1,
        to_version: 1,
    };
    let wire = crate::ReplicatorDeltaPropagation {
        from: replica("remote"),
        reply: false,
        deltas: vec![wire_delta],
    };

    let error = decode_delta_propagation::<GCounter, _>(&wire, &GCounterCodec)
        .expect_err("wrong CRDT manifest should fail");

    assert!(error.to_string().contains("expected CRDT manifest"));
}
