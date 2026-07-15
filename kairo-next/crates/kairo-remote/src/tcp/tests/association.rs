use std::io::Write;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use kairo_testkit::await_assert;

use super::*;
use crate::{
    AssociationState, RemoteAssociationCache, RemoteAssociationRegistry, RemoteOutbound,
    RemoteStreamId, decode_remote_envelope_frame,
};

struct RejectingFrameHandler;

impl RemoteFrameHandler for RejectingFrameHandler {
    fn handle_frame(&self, _stream_id: RemoteStreamId, _frame: Bytes) -> crate::Result<()> {
        Err(RemoteError::Inbound(
            "forced inbound frame rejection".to_string(),
        ))
    }
}

#[test]
fn tcp_association_listener_drains_dialed_lane_streams_to_frame_handler() {
    let handler = Arc::new(CollectingFrameHandler::default());
    let listener = TcpAssociationListener::bind(
        ("127.0.0.1", 0),
        handler.clone() as Arc<dyn RemoteFrameHandler>,
    )
    .unwrap();
    let port = listener.local_addr().unwrap().port();
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let accepted = listener.accept_association().unwrap();
        accepted_tx.send(accepted.stream_count()).unwrap();
        accepted.drain().unwrap()
    });

    let cache = RemoteAssociationCache::new();
    let installer = crate::RemoteAssociationRouteInstaller::new(cache.clone());
    let dialer = TcpAssociationDialer::new(installer).with_connect_timeout(Duration::from_secs(1));
    let registration = dialer.dial(address(port)).unwrap();
    assert_eq!(accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 3);

    cache.send(envelope(port, 13)).unwrap();
    drop(registration);
    drop(cache);
    drop(dialer);

    let report = handle.join().unwrap();
    assert_eq!(report.streams, 3);
    assert_eq!(report.frames, 1);

    let frames = handler.frames();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].0, RemoteStreamId::Ordinary);
    let decoded = decode_remote_envelope_frame(frames[0].1.clone()).unwrap();
    assert_eq!(decoded.message.payload, Bytes::from_static(&[13]));
}

#[test]
fn tcp_accepted_association_can_read_lanes_while_streams_remain_open() {
    let (frame_tx, frame_rx) = mpsc::channel();
    let handler = Arc::new(ChannelFrameHandler::new(frame_tx)) as Arc<dyn RemoteFrameHandler>;
    let listener = TcpAssociationListener::bind(("127.0.0.1", 0), handler).unwrap();
    let port = listener.local_addr().unwrap().port();
    let accept_handle = thread::spawn(move || {
        let accepted = listener.accept_association().unwrap();
        accepted.spawn_lane_readers()
    });

    let cache = RemoteAssociationCache::new();
    let installer = crate::RemoteAssociationRouteInstaller::new(cache.clone());
    let dialer = TcpAssociationDialer::new(installer).with_connect_timeout(Duration::from_secs(1));
    let registration = dialer.dial(address(port)).unwrap();
    let reader_handle = accept_handle.join().unwrap();

    cache.send(envelope(port, 21)).unwrap();
    let (stream_id, frame) = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(stream_id, RemoteStreamId::Ordinary);
    let decoded = decode_remote_envelope_frame(frame).unwrap();
    assert_eq!(decoded.message.payload, Bytes::from_static(&[21]));

    drop(registration);
    drop(cache);
    drop(dialer);
    let report = reader_handle.join().unwrap();
    assert_eq!(report.streams, 3);
    assert_eq!(report.frames, 1);
}

#[test]
fn tcp_listener_accept_loop_spawns_and_joins_lane_readers() {
    let (frame_tx, frame_rx) = mpsc::channel();
    let handler = Arc::new(ChannelFrameHandler::new(frame_tx)) as Arc<dyn RemoteFrameHandler>;
    let listener = TcpAssociationListener::bind(("127.0.0.1", 0), handler)
        .unwrap()
        .with_accept_poll_interval(Duration::from_millis(1));
    let port = listener.local_addr().unwrap().port();
    let listener_handle = listener.spawn_accept_loop().unwrap();

    let cache = RemoteAssociationCache::new();
    let installer = crate::RemoteAssociationRouteInstaller::new(cache.clone());
    let dialer = TcpAssociationDialer::new(installer).with_connect_timeout(Duration::from_secs(1));
    let registration = dialer.dial(address(port)).unwrap();

    cache.send(envelope(port, 34)).unwrap();
    let (stream_id, frame) = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(stream_id, RemoteStreamId::Ordinary);
    let decoded = decode_remote_envelope_frame(frame).unwrap();
    assert_eq!(decoded.message.payload, Bytes::from_static(&[34]));

    listener_handle.stop();
    drop(registration);
    drop(cache);
    drop(dialer);

    let report = listener_handle.join().unwrap();
    assert_eq!(report.accepted_associations, 1);
    assert!(report.remote_identities.is_empty());
    assert_eq!(report.read.streams, 3);
    assert_eq!(report.read.frames, 1);
    assert!(report.supervision.is_empty());
}

#[test]
fn tcp_listener_validates_handshaken_lanes_before_reading_frames() {
    let handler = Arc::new(CollectingFrameHandler::default());
    let remote_address = association_address("sender", 25521);
    let listener = TcpAssociationListener::bind(
        ("127.0.0.1", 0),
        handler.clone() as Arc<dyn RemoteFrameHandler>,
    )
    .unwrap();
    let port = listener.local_addr().unwrap().port();
    let listener = listener.with_local_identity(association_address("receiver", port), 11);
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let accepted = listener.accept_association().unwrap();
        accepted_tx
            .send((accepted.remote_address().cloned(), accepted.remote_uid()))
            .unwrap();
        accepted.drain().unwrap()
    });

    let cache = RemoteAssociationCache::new();
    let installer = crate::RemoteAssociationRouteInstaller::new(cache.clone());
    let dialer = TcpAssociationDialer::new(installer)
        .with_local_identity(remote_address.clone(), 22)
        .with_handshake_response_required()
        .with_connect_timeout(Duration::from_secs(1));
    let registration = dialer.dial(association_address("receiver", port)).unwrap();

    assert_eq!(
        accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        (Some(remote_address), Some(22))
    );

    cache.send(envelope_to("receiver", port, 55)).unwrap();
    drop(registration);
    drop(cache);
    drop(dialer);

    let report = handle.join().unwrap();
    assert_eq!(report.streams, 3);
    assert_eq!(report.frames, 1);
    let frames = handler.frames();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].0, RemoteStreamId::Ordinary);
}

#[test]
fn tcp_listener_accept_loop_reports_handshaken_remote_identity() {
    let (frame_tx, frame_rx) = mpsc::channel();
    let handler = Arc::new(ChannelFrameHandler::new(frame_tx)) as Arc<dyn RemoteFrameHandler>;
    let remote_address = association_address("sender", 25521);
    let listener = TcpAssociationListener::bind(("127.0.0.1", 0), handler)
        .unwrap()
        .with_accept_poll_interval(Duration::from_millis(1));
    let port = listener.local_addr().unwrap().port();
    let listener = listener.with_local_identity(association_address("receiver", port), 11);
    let listener_handle = listener.spawn_accept_loop().unwrap();

    let cache = RemoteAssociationCache::new();
    let installer = crate::RemoteAssociationRouteInstaller::new(cache.clone());
    let dialer = TcpAssociationDialer::new(installer)
        .with_local_identity(remote_address.clone(), 42)
        .with_handshake_response_required()
        .with_connect_timeout(Duration::from_secs(1));
    let registration = dialer.dial(association_address("receiver", port)).unwrap();

    cache.send(envelope_to("receiver", port, 56)).unwrap();
    let (stream_id, frame) = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(stream_id, RemoteStreamId::Ordinary);
    let decoded = decode_remote_envelope_frame(frame).unwrap();
    assert_eq!(decoded.message.payload, Bytes::from_static(&[56]));

    listener_handle.stop();
    drop(registration);
    drop(cache);
    drop(dialer);

    let report = listener_handle.join().unwrap();
    assert_eq!(report.accepted_associations, 1);
    assert_eq!(
        report.remote_identities,
        vec![TcpAssociationIdentity::new(remote_address, 42)]
    );
    assert_eq!(report.read.streams, 3);
    assert_eq!(report.read.frames, 1);
}

#[test]
fn tcp_listener_accept_loop_records_handshaken_identity_in_registry() {
    let (frame_tx, frame_rx) = mpsc::channel();
    let handler = Arc::new(ChannelFrameHandler::new(frame_tx)) as Arc<dyn RemoteFrameHandler>;
    let (sender_frame_tx, sender_frame_rx) = mpsc::channel();
    let sender_reader = TcpAssociationStreamReader::new(Arc::new(ChannelFrameHandler::new(
        sender_frame_tx,
    )) as Arc<dyn RemoteFrameHandler>);
    let registry = RemoteAssociationRegistry::new();
    let receiver_cache = RemoteAssociationCache::new();
    let receiver_installer = crate::RemoteAssociationRouteInstaller::new(receiver_cache.clone());
    let remote_address = association_address("sender", 25521);
    let listener = TcpAssociationListener::bind(("127.0.0.1", 0), handler)
        .unwrap()
        .with_accept_poll_interval(Duration::from_millis(1));
    let port = listener.local_addr().unwrap().port();
    let listener = listener
        .with_local_identity(association_address("receiver", port), 11)
        .with_association_registry(registry.clone())
        .with_route_installer(receiver_installer);
    let listener_handle = listener.spawn_accept_loop().unwrap();

    let cache = RemoteAssociationCache::new();
    let sender_registry = RemoteAssociationRegistry::new();
    let installer = crate::RemoteAssociationRouteInstaller::new(cache.clone())
        .with_association_registry(sender_registry.clone());
    let dialer = TcpAssociationDialer::new(installer)
        .with_local_identity(remote_address.clone(), 42)
        .with_handshake_response_required()
        .with_connect_timeout(Duration::from_secs(1));
    let (registration, sender_reader_handle) = dialer
        .dial_with_reader(association_address("receiver", port), sender_reader)
        .unwrap();

    let receiver_association = sender_registry.association_by_uid(11).unwrap();
    assert!(Arc::ptr_eq(
        registration.pipeline().association(),
        &receiver_association
    ));
    assert_eq!(
        receiver_association
            .lock()
            .expect("remote association lock poisoned")
            .state(),
        &AssociationState::Active {
            remote_uid: Some(11)
        }
    );

    cache.send(envelope_to("receiver", port, 57)).unwrap();
    let (stream_id, frame) = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(stream_id, RemoteStreamId::Ordinary);
    let decoded = decode_remote_envelope_frame(frame).unwrap();
    assert_eq!(decoded.message.payload, Bytes::from_static(&[57]));

    let association = registry.association_by_uid(42).unwrap();
    assert_eq!(
        association
            .lock()
            .expect("remote association lock poisoned")
            .state(),
        &AssociationState::Active {
            remote_uid: Some(42)
        }
    );
    assert_eq!(registry.association_count(), 1);
    assert_eq!(receiver_cache.route_count(), 1);
    receiver_cache
        .send(envelope_to("sender", 25521, 58))
        .unwrap();
    let (stream_id, frame) = sender_frame_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(stream_id, RemoteStreamId::Ordinary);
    let decoded = decode_remote_envelope_frame(frame).unwrap();
    assert_eq!(decoded.message.payload, Bytes::from_static(&[58]));

    listener_handle.stop();
    drop(registration);
    drop(cache);
    drop(dialer);

    await_assert(Duration::from_secs(1), Duration::from_millis(1), || {
        let actual = receiver_cache.route_count();
        if actual == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected receiver cache to be empty, found {actual}"
            ))
        }
    })
    .unwrap();
    drop(receiver_cache);

    let report = listener_handle.join().unwrap();
    let sender_report = sender_reader_handle.join().unwrap();
    assert_eq!(
        report.remote_identities,
        vec![TcpAssociationIdentity::new(remote_address, 42)]
    );
    assert_eq!(sender_report.frames, 1);
}

#[test]
fn tcp_listener_rejects_handshake_for_different_local_address() {
    let handler = Arc::new(CollectingFrameHandler::default());
    let listener = TcpAssociationListener::bind(
        ("127.0.0.1", 0),
        handler.clone() as Arc<dyn RemoteFrameHandler>,
    )
    .unwrap()
    .with_expected_streams(1)
    .with_local_address(association_address("receiver", 25520));
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || listener.accept_association());

    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let handshake = TcpAssociationHandshake::new(
        RemoteStreamId::Control,
        TcpAssociationIdentity::new(association_address("sender", 25521), 22),
        association_address("other", 25520),
    );
    stream
        .write_all(&encode_tcp_association_handshake(&handshake).unwrap())
        .unwrap();
    drop(stream);

    let error = match handle.join().unwrap() {
        Ok(_) => panic!("wrong handshake target should be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, RemoteError::InvalidFrame(_)));
    assert!(error.to_string().contains("addressed to"));
    assert!(handler.frames().is_empty());
}

#[test]
fn tcp_listener_times_out_silent_handshake_before_lane_assembly() {
    let listener = TcpAssociationListener::bind(
        ("127.0.0.1", 0),
        Arc::new(CollectingFrameHandler::default()) as Arc<dyn RemoteFrameHandler>,
    )
    .unwrap()
    .with_expected_streams(1);
    let port = listener.local_addr().unwrap().port();
    let listener = listener
        .with_local_address(association_address("receiver", port))
        .with_handshake_read_settings(
            TcpHandshakeReadSettings::new(1_024, Duration::from_millis(20)).unwrap(),
        );
    let handle = thread::spawn(move || listener.accept_association());
    let _silent = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();

    let error = match handle
        .join()
        .expect("handshake timeout thread should not panic")
    {
        Ok(_) => panic!("silent handshake peer should time out"),
        Err(error) => error,
    };

    assert!(matches!(error, crate::RemoteError::Inbound(_)));
}

#[test]
fn tcp_listener_isolates_malformed_handshakes_and_accepts_valid_peer() {
    let (frame_tx, frame_rx) = mpsc::channel();
    let handler = Arc::new(ChannelFrameHandler::new(frame_tx)) as Arc<dyn RemoteFrameHandler>;
    let cache = RemoteAssociationCache::new();
    let registry = RemoteAssociationRegistry::new();
    let installer = crate::RemoteAssociationRouteInstaller::new(cache.clone())
        .with_association_registry(registry.clone());
    let listener = TcpAssociationListener::bind(("127.0.0.1", 0), handler)
        .unwrap()
        .with_accept_poll_interval(Duration::from_millis(1))
        .with_handshake_read_settings(
            TcpHandshakeReadSettings::new(1_024, Duration::from_millis(50)).unwrap(),
        );
    let port = listener.local_addr().unwrap().port();
    let receiver_address = association_address("receiver", port);
    let listener = listener
        .with_local_identity(receiver_address.clone(), 11)
        .with_association_registry(registry.clone())
        .with_route_installer(installer);
    let listener_handle = listener.spawn_accept_loop().unwrap();

    let malformed_handshakes = [
        b"BAD!\x02\x00\x00\x00\x00".to_vec(),
        b"KAH2\xff\x00\x00\x00\x00".to_vec(),
        [b"KAH2\x02".as_slice(), &1_025_u32.to_be_bytes()].concat(),
        [
            b"KAH2\x02".as_slice(),
            &4_u32.to_be_bytes(),
            &[RemoteStreamId::Control.as_u8()],
        ]
        .concat(),
        encode_tcp_association_handshake(&TcpAssociationHandshake::new(
            RemoteStreamId::Control,
            TcpAssociationIdentity::new(association_address("wrong-target", 25521), 21),
            association_address("other", port),
        ))
        .unwrap()
        .to_vec(),
    ];
    for bytes in malformed_handshakes {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream.write_all(&bytes).unwrap();
    }

    let sender_address = association_address("sender", 25522);
    let sender_cache = RemoteAssociationCache::new();
    let sender_registry = RemoteAssociationRegistry::new();
    let sender_installer = crate::RemoteAssociationRouteInstaller::new(sender_cache.clone())
        .with_association_registry(sender_registry);
    let dialer = TcpAssociationDialer::new(sender_installer)
        .with_local_identity(sender_address.clone(), 22)
        .with_handshake_response_required()
        .with_connect_timeout(Duration::from_secs(1));
    let registration = dialer.dial(receiver_address).unwrap();

    sender_cache
        .send(envelope_to("receiver", port, 64))
        .unwrap();
    let (_, frame) = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let decoded = decode_remote_envelope_frame(frame).unwrap();
    assert_eq!(decoded.message.payload, Bytes::from_static(&[64]));
    assert_eq!(registry.association_count(), 1);
    assert_eq!(cache.route_count(), 1);

    registration.close_owned_route("malformed handshake test done");
    await_assert(Duration::from_secs(1), Duration::from_millis(1), || {
        let routes = cache.route_count();
        (routes == 0)
            .then_some(())
            .ok_or_else(|| format!("expected receiver route cleanup, found {routes}"))
    })
    .unwrap();
    listener_handle.stop();
    drop(registration);
    drop(sender_cache);
    drop(dialer);
    drop(cache);

    let report = listener_handle.join().unwrap();
    assert_eq!(report.accepted_associations, 1);
    assert_eq!(
        report.remote_identities,
        vec![TcpAssociationIdentity::new(sender_address, 22)]
    );
}

#[test]
fn tcp_listener_completes_interleaved_peer_without_mixing_partial_lanes() {
    let (frame_tx, frame_rx) = mpsc::channel();
    let handler = Arc::new(ChannelFrameHandler::new(frame_tx)) as Arc<dyn RemoteFrameHandler>;
    let listener = TcpAssociationListener::bind(("127.0.0.1", 0), handler)
        .unwrap()
        .with_accept_poll_interval(Duration::from_millis(1));
    let port = listener.local_addr().unwrap().port();
    let receiver_address = association_address("receiver", port);
    let listener = listener.with_local_identity(receiver_address.clone(), 11);
    let listener_handle = listener.spawn_accept_loop().unwrap();

    let mut partial_lane = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let partial_handshake = TcpAssociationHandshake::new(
        RemoteStreamId::Control,
        TcpAssociationIdentity::new(association_address("partial", 25521), 21),
        receiver_address.clone(),
    );
    partial_lane
        .write_all(&encode_tcp_association_handshake(&partial_handshake).unwrap())
        .unwrap();

    let cache = RemoteAssociationCache::new();
    let installer = crate::RemoteAssociationRouteInstaller::new(cache.clone());
    let complete_identity = association_address("complete", 25522);
    let dialer = TcpAssociationDialer::new(installer)
        .with_local_identity(complete_identity.clone(), 22)
        .with_handshake_response_required()
        .with_connect_timeout(Duration::from_secs(1));
    let registration = dialer.dial(receiver_address).unwrap();

    cache.send(envelope_to("receiver", port, 61)).unwrap();
    let (stream_id, frame) = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(stream_id, RemoteStreamId::Ordinary);
    let decoded = decode_remote_envelope_frame(frame).unwrap();
    assert_eq!(decoded.message.payload, Bytes::from_static(&[61]));

    listener_handle.stop();
    drop(partial_lane);
    drop(registration);
    drop(cache);
    drop(dialer);

    let report = listener_handle.join().unwrap();
    assert_eq!(report.accepted_associations, 1);
    assert_eq!(
        report.remote_identities,
        vec![TcpAssociationIdentity::new(complete_identity, 22)]
    );
}

#[test]
fn tcp_listener_expires_partial_peer_and_releases_pending_capacity() {
    let (frame_tx, frame_rx) = mpsc::channel();
    let handler = Arc::new(ChannelFrameHandler::new(frame_tx)) as Arc<dyn RemoteFrameHandler>;
    let listener = TcpAssociationListener::bind(("127.0.0.1", 0), handler)
        .unwrap()
        .with_accept_poll_interval(Duration::from_millis(1))
        .with_association_assembly_settings(
            TcpAssociationAssemblySettings::new(Duration::from_millis(200), 1).unwrap(),
        );
    let port = listener.local_addr().unwrap().port();
    let receiver_address = association_address("receiver", port);
    let listener = listener.with_local_identity(receiver_address.clone(), 11);
    let listener_handle = listener.spawn_accept_loop().unwrap();

    let mut partial_lane = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    partial_lane
        .write_all(
            &encode_tcp_association_handshake(&TcpAssociationHandshake::new(
                RemoteStreamId::Control,
                TcpAssociationIdentity::new(association_address("partial", 25521), 21),
                receiver_address.clone(),
            ))
            .unwrap(),
        )
        .unwrap();
    thread::sleep(Duration::from_millis(20));

    let rejected_cache = RemoteAssociationCache::new();
    let rejected_dialer = TcpAssociationDialer::new(crate::RemoteAssociationRouteInstaller::new(
        rejected_cache.clone(),
    ))
    .with_local_identity(association_address("over-limit", 25522), 22)
    .with_handshake_response_required()
    .with_handshake_read_settings(
        TcpHandshakeReadSettings::new(1_024, Duration::from_millis(100)).unwrap(),
    )
    .with_connect_timeout(Duration::from_secs(1));
    assert!(rejected_dialer.dial(receiver_address.clone()).is_err());
    assert_eq!(rejected_cache.route_count(), 0);

    thread::sleep(Duration::from_millis(220));
    let accepted_cache = RemoteAssociationCache::new();
    let accepted_dialer = TcpAssociationDialer::new(crate::RemoteAssociationRouteInstaller::new(
        accepted_cache.clone(),
    ))
    .with_local_identity(association_address("after-timeout", 25523), 23)
    .with_handshake_response_required()
    .with_connect_timeout(Duration::from_secs(1));
    let registration = accepted_dialer.dial(receiver_address).unwrap();

    accepted_cache
        .send(envelope_to("receiver", port, 62))
        .unwrap();
    let (_, frame) = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let decoded = decode_remote_envelope_frame(frame).unwrap();
    assert_eq!(decoded.message.payload, Bytes::from_static(&[62]));

    listener_handle.stop();
    drop(partial_lane);
    drop(registration);
    drop(accepted_cache);
    drop(accepted_dialer);
    drop(rejected_cache);
    drop(rejected_dialer);

    let report = listener_handle.join().unwrap();
    assert_eq!(report.accepted_associations, 1);
}

#[test]
fn tcp_dialed_reader_completion_closes_owned_route_and_association() {
    let receiver_cache = RemoteAssociationCache::new();
    let receiver_registry = RemoteAssociationRegistry::new();
    let receiver_installer = crate::RemoteAssociationRouteInstaller::new(receiver_cache.clone())
        .with_association_registry(receiver_registry.clone());
    let listener = TcpAssociationListener::bind(
        ("127.0.0.1", 0),
        Arc::new(RejectingFrameHandler) as Arc<dyn RemoteFrameHandler>,
    )
    .unwrap();
    let port = listener.local_addr().unwrap().port();
    let receiver_address = association_address("receiver", port);
    let listener = listener
        .with_local_identity(receiver_address.clone(), 11)
        .with_association_registry(receiver_registry)
        .with_route_installer(receiver_installer);
    let accept_handle =
        thread::spawn(move || listener.accept_association().unwrap().spawn_lane_readers());

    let sender_cache = RemoteAssociationCache::new();
    let sender_registry = RemoteAssociationRegistry::new();
    let sender_installer = crate::RemoteAssociationRouteInstaller::new(sender_cache.clone())
        .with_association_registry(sender_registry);
    let dialer = TcpAssociationDialer::new(sender_installer)
        .with_local_identity(association_address("sender", 25521), 22)
        .with_handshake_response_required()
        .with_connect_timeout(Duration::from_secs(1));
    let sender_reader = TcpAssociationStreamReader::new(
        Arc::new(CollectingFrameHandler::default()) as Arc<dyn RemoteFrameHandler>,
    );
    let (registration, sender_reader_handle) = dialer
        .dial_with_reader(receiver_address, sender_reader)
        .unwrap();
    let receiver_reader_handle = accept_handle.join().unwrap();

    sender_cache
        .send(envelope_to("receiver", port, 63))
        .unwrap();

    await_assert(Duration::from_secs(1), Duration::from_millis(1), || {
        let routes = sender_cache.route_count();
        if routes == 0 {
            Ok(())
        } else {
            Err(format!("expected dialed route removal, found {routes}"))
        }
    })
    .unwrap();

    assert_eq!(sender_cache.route_count(), 0);
    assert!(matches!(
        registration
            .pipeline()
            .association()
            .lock()
            .expect("sender association poisoned")
            .state(),
        AssociationState::Closed { .. }
    ));
    assert_eq!(receiver_cache.route_count(), 0);
    assert!(receiver_reader_handle.join().is_err());
    sender_reader_handle.join().unwrap();
}
