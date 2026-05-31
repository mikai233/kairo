use super::*;

#[test]
fn direct_read_write_receive_applies_writes_and_serves_reads() {
    let key = ReplicatorKey::new("counter");
    let from = replica("remote");
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(from.clone(), 6)
            .unwrap()
            .reset_delta(),
    );
    let write = encode_write(&key, Some(from.clone()), &envelope, &GCounterCodec).unwrap();
    let mut state = ReplicatorState::<GCounter>::new();

    let write_result = crate::apply_write(&mut state, &write, &GCounterCodec);

    assert!(write_result.is_ack());
    assert_eq!(write_result.key(), &key);
    assert_eq!(write_result.from(), Some(&from));
    assert!(matches!(
        write_result,
        DirectWriteResult::Ack { changed: true, .. }
    ));
    assert_eq!(state.envelope(&key).unwrap().data().value().unwrap(), 6);

    let read = encode_read(&key, Some(from.clone()));
    let read_result = crate::serve_read(&state, &read, &GCounterCodec).unwrap();

    assert_eq!(read_result.key(), &key);
    assert_eq!(read_result.from(), Some(&from));
    assert_eq!(
        decode_read_result(read_result.message(), &GCounterCodec)
            .unwrap()
            .unwrap()
            .data()
            .value()
            .unwrap(),
        6
    );

    let missing = crate::serve_read(
        &state,
        &encode_read(&ReplicatorKey::new("missing"), None),
        &GCounterCodec,
    )
    .unwrap();
    assert_eq!(missing.message().envelope, None);
}

#[test]
fn direct_write_receive_nacks_decode_failures_without_changing_state() {
    let key = ReplicatorKey::new("counter");
    let mut state = ReplicatorState::<GCounter>::new();
    let write = crate::ReplicatorWrite {
        key: key.as_str().to_string(),
        from: Some(replica("remote")),
        envelope: crate::ReplicatorDataEnvelope {
            crdt_manifest: crate::GSET_STRING_MANIFEST.to_string(),
            crdt_version: crate::CRDT_CODEC_VERSION,
            payload: bytes::Bytes::new(),
            pruning: Vec::new(),
        },
    };

    let result = crate::apply_write(&mut state, &write, &GCounterCodec);

    assert!(matches!(
        result,
        DirectWriteResult::Nack { reason, .. } if reason.contains("expected CRDT manifest")
    ));
    assert!(!state.contains_key(&key));
}
