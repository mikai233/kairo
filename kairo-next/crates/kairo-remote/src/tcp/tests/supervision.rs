use std::io::Write;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::*;
use crate::{
    RemoteAssociationRegistry, RemoteByteSink, RemoteError, RemoteStreamId,
    TcpAssociationReadReport, TcpAssociationReaderFailure, TcpAssociationReaderSupervisionDecision,
};

#[test]
fn tcp_lane_reader_supervision_records_lane_restart_decision() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let handler = Arc::new(CollectingFrameHandler::default());
    let remote_listener = TcpAssociationListener::from_listener(
        listener,
        handler.clone() as Arc<dyn RemoteFrameHandler>,
    )
    .with_expected_streams(1)
    .with_local_address(association_address("receiver", port));
    let handle = thread::spawn(move || {
        let accepted = remote_listener.accept_association().unwrap();
        let mut supervisor = TcpAssociationReaderSupervisor::default();
        accepted
            .spawn_lane_readers()
            .join_with_supervisor(&mut supervisor)
    });

    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let handshake = TcpAssociationHandshake::new(
        RemoteStreamId::Control,
        TcpAssociationIdentity::new(association_address("sender", 25521), 22),
        association_address("receiver", port),
    );
    stream
        .write_all(&encode_tcp_association_handshake(&handshake).unwrap())
        .unwrap();
    stream.write_all(b"not-a-stream").unwrap();
    drop(stream);

    let report = handle.join().unwrap();
    assert_eq!(report.read, TcpAssociationReadReport::default());
    assert_eq!(report.supervision.len(), 1);
    let TcpAssociationReaderSupervisionDecision::RestartInboundStreams {
        restart_count,
        failure: TcpAssociationReaderFailure::Lane { stream_id, reason },
    } = &report.supervision[0]
    else {
        panic!("expected lane restart decision");
    };
    assert_eq!(*restart_count, 1);
    assert_eq!(*stream_id, RemoteStreamId::Control);
    assert!(reason.contains("invalid"));
    assert!(handler.frames().is_empty());
}

#[test]
fn tcp_listener_report_includes_reader_supervision_decisions() {
    let handler = Arc::new(CollectingFrameHandler::default());
    let listener = TcpAssociationListener::bind(
        ("127.0.0.1", 0),
        handler.clone() as Arc<dyn RemoteFrameHandler>,
    )
    .unwrap()
    .with_expected_streams(1)
    .with_accept_poll_interval(Duration::from_millis(1));
    let port = listener.local_addr().unwrap().port();
    let registry = RemoteAssociationRegistry::new();
    let listener = listener
        .with_local_address(association_address("receiver", port))
        .with_association_registry(registry.clone());
    let listener_handle = listener.spawn_accept_loop().unwrap();

    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let handshake = TcpAssociationHandshake::new(
        RemoteStreamId::Ordinary,
        TcpAssociationIdentity::new(association_address("sender", 25521), 22),
        association_address("receiver", port),
    );
    stream
        .write_all(&encode_tcp_association_handshake(&handshake).unwrap())
        .unwrap();
    stream.write_all(b"not-a-stream").unwrap();
    drop(stream);

    let deadline = Instant::now() + Duration::from_secs(1);
    while registry.association_count() == 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    listener_handle.stop();
    let report = listener_handle.join().unwrap();
    assert_eq!(report.accepted_associations, 1);
    assert_eq!(report.read, TcpAssociationReadReport::default());
    assert_eq!(report.supervision.len(), 1);
    assert!(matches!(
        &report.supervision[0],
        TcpAssociationReaderSupervisionDecision::RestartInboundStreams {
            restart_count: 1,
            failure: TcpAssociationReaderFailure::Lane {
                stream_id: RemoteStreamId::Ordinary,
                ..
            }
        }
    ));
    assert!(handler.frames().is_empty());
}

#[test]
fn tcp_stream_reader_propagates_invalid_stream_frames() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let handler = Arc::new(CollectingFrameHandler::default());
    let remote_listener = TcpAssociationListener::from_listener(
        listener,
        handler.clone() as Arc<dyn RemoteFrameHandler>,
    )
    .with_expected_streams(1);
    let (accepted_tx, accepted_rx) = std::sync::mpsc::channel();
    let handle = thread::spawn(move || {
        let accepted = remote_listener.accept_association().unwrap();
        accepted_tx.send(()).unwrap();
        accepted.drain()
    });

    let stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let sink = TcpRemoteByteSink::from_stream("invalid", stream);
    sink.send_bytes(bytes::Bytes::from_static(b"not-a-stream"))
        .unwrap();
    drop(sink);

    let error = handle
        .join()
        .unwrap()
        .expect_err("invalid stream should fail");
    assert!(matches!(error, RemoteError::InvalidFrame(_)));
    assert!(handler.frames().is_empty());
}
