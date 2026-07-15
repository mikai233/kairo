use super::*;

#[test]
fn replicator_actor_applies_remote_write_and_serves_remote_read_messages() {
    let system = ActorSystem::builder("ddata-replicator-direct-read-write")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (write_result_ref, write_result_rx) = forward_ref(&system, "write-results");
    let (read_result_ref, read_result_rx) =
        forward_ref::<Result<DirectReadResult, String>>(&system, "read-results");
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(remote.clone(), 8)
            .unwrap()
            .reset_delta(),
    );
    let write = encode_write(&key, Some(remote.clone()), &envelope, &GCounterCodec).unwrap();
    let write_codec: Arc<dyn CrdtDataCodec<GCounter> + Send + Sync> = Arc::new(GCounterCodec);
    let read_codec: Arc<dyn CrdtDataCodec<GCounter> + Send + Sync> = Arc::new(GCounterCodec);

    replicator
        .tell(ReplicatorActorMsg::ApplyWrite {
            write,
            codec: write_codec,
            reply_to: write_result_ref,
        })
        .unwrap();
    assert!(matches!(
        write_result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        DirectWriteResult::Ack { changed: true, .. }
    ));

    replicator
        .tell(ReplicatorActorMsg::ServeRead {
            read: encode_read(&key, Some(remote.clone())),
            codec: read_codec,
            reply_to: read_result_ref,
        })
        .unwrap();

    let read_result = read_result_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(read_result.key(), &key);
    assert_eq!(read_result.from(), Some(&remote));
    assert_eq!(
        decode_read_result(read_result.message(), &GCounterCodec)
            .unwrap()
            .unwrap()
            .data()
            .value()
            .unwrap(),
        8
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_marks_direct_write_pruning_seen_before_ack() {
    let system = ActorSystem::builder("ddata-replicator-direct-write-pruning-seen")
        .build()
        .unwrap();
    let local = replica("local");
    let remote = replica("remote");
    let removed = replica("removed");
    let replicator = system
        .spawn(
            "replicator",
            Props::new({
                let local = local.clone();
                move || ReplicatorActor::<GCounter>::new().with_self_replica(local.clone())
            }),
        )
        .unwrap();
    let (write_result_ref, write_result_rx) = forward_ref(&system, "write-results");
    let (read_result_ref, read_result_rx) =
        forward_ref::<Result<DirectReadResult, String>>(&system, "read-results");
    let key = ReplicatorKey::new("counter");
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(removed.clone(), 8)
            .unwrap()
            .reset_delta(),
    )
    .init_removed_node_pruning(removed.clone(), remote.clone());
    let write = encode_write(&key, Some(remote.clone()), &envelope, &GCounterCodec).unwrap();
    let write_codec: Arc<dyn CrdtDataCodec<GCounter> + Send + Sync> = Arc::new(GCounterCodec);
    let read_codec: Arc<dyn CrdtDataCodec<GCounter> + Send + Sync> = Arc::new(GCounterCodec);

    replicator
        .tell(ReplicatorActorMsg::ApplyWrite {
            write,
            codec: write_codec,
            reply_to: write_result_ref,
        })
        .unwrap();
    assert!(matches!(
        write_result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        DirectWriteResult::Ack { changed: true, .. }
    ));

    replicator
        .tell(ReplicatorActorMsg::ServeRead {
            read: encode_read(&key, Some(remote)),
            codec: read_codec,
            reply_to: read_result_ref,
        })
        .unwrap();
    let read_result = read_result_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    let decoded = decode_read_result(read_result.message(), &GCounterCodec)
        .unwrap()
        .unwrap();
    let PruningState::Initialized(initialized) = decoded.pruning().get(&removed).unwrap() else {
        panic!("expected initialized pruning marker");
    };
    assert!(initialized.seen().contains(&local));

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_nacks_remote_write_decode_failures() {
    let system = ActorSystem::builder("ddata-replicator-direct-write-nack")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (write_result_ref, write_result_rx) = forward_ref(&system, "write-results");
    let (get_ref, get_rx) = forward_ref(&system, "get-replies");
    let key = ReplicatorKey::new("counter");
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
    let codec: Arc<dyn CrdtDataCodec<GCounter> + Send + Sync> = Arc::new(GCounterCodec);

    replicator
        .tell(ReplicatorActorMsg::ApplyWrite {
            write,
            codec,
            reply_to: write_result_ref,
        })
        .unwrap();
    assert!(matches!(
        write_result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        DirectWriteResult::Nack { reason, .. } if reason.contains("expected CRDT manifest")
    ));

    replicator
        .tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::local(),
            reply_to: get_ref,
        })
        .unwrap();
    assert_eq!(
        get_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        GetResponse::NotFound { key }
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}
