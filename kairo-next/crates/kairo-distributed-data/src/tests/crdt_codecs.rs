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
}
