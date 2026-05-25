use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use kairo_serialization::{ActorRefWireData, Manifest, RemoteEnvelope, SerializedMessage};

use super::*;
use crate::{
    RemoteAssociationAddress, RemoteAssociationCache, RemoteByteSink, RemoteError,
    RemoteFrameHandler, RemoteOutbound, RemoteStreamDecoder, RemoteStreamFrame, RemoteStreamId,
    RemoteStreamWriter, decode_remote_envelope_frame,
};

#[derive(Default)]
struct CollectingFrameHandler {
    frames: Mutex<Vec<(RemoteStreamId, Bytes)>>,
}

impl CollectingFrameHandler {
    fn frames(&self) -> Vec<(RemoteStreamId, Bytes)> {
        self.frames.lock().expect("frame handler poisoned").clone()
    }
}

struct ChannelFrameHandler {
    tx: Mutex<mpsc::Sender<(RemoteStreamId, Bytes)>>,
}

impl ChannelFrameHandler {
    fn new(tx: mpsc::Sender<(RemoteStreamId, Bytes)>) -> Self {
        Self { tx: Mutex::new(tx) }
    }
}

impl RemoteFrameHandler for ChannelFrameHandler {
    fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> crate::Result<()> {
        self.tx
            .lock()
            .expect("channel frame handler poisoned")
            .send((stream_id, frame))
            .map_err(|error| RemoteError::Inbound(error.to_string()))
    }
}

impl RemoteFrameHandler for CollectingFrameHandler {
    fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> crate::Result<()> {
        self.frames
            .lock()
            .expect("frame handler poisoned")
            .push((stream_id, frame));
        Ok(())
    }
}

fn address(port: u16) -> RemoteAssociationAddress {
    RemoteAssociationAddress::new("kairo", "remote", "127.0.0.1", Some(port)).unwrap()
}

fn association_address(system: &str, port: u16) -> RemoteAssociationAddress {
    RemoteAssociationAddress::new("kairo", system, "127.0.0.1", Some(port)).unwrap()
}

fn envelope(port: u16, value: u8) -> RemoteEnvelope {
    envelope_to("remote", port, value)
}

fn envelope_to(system: &str, port: u16, value: u8) -> RemoteEnvelope {
    RemoteEnvelope::new(
        ActorRefWireData::new(format!("kairo://{system}@127.0.0.1:{port}/user/target")).unwrap(),
        None,
        SerializedMessage::new(
            777,
            Manifest::new("kairo.remote.test.TcpAssociation"),
            1,
            Bytes::from(vec![value]),
        ),
    )
}

fn decode_stream(bytes: Bytes) -> Vec<RemoteStreamFrame> {
    let mut decoder = RemoteStreamDecoder::new();
    let frames = decoder.push(bytes).unwrap();
    decoder.finish().unwrap();
    frames
}

#[test]
fn tcp_byte_sink_writes_bytes_to_stream() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        accepted_tx.send(()).unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).unwrap();
        bytes
    });

    let sink = TcpRemoteByteSink::connect(&address(port), Some(Duration::from_secs(1))).unwrap();
    accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    sink.send_bytes(Bytes::from_static(b"hello")).unwrap();
    drop(sink);

    assert_eq!(handle.join().unwrap(), b"hello");
}

#[test]
fn tcp_association_dialer_populates_cache_with_stream_routes() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut streams = Vec::new();
        for _ in 0..3 {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_millis(100)))
                .unwrap();
            streams.push(stream);
        }
        accepted_tx.send(()).unwrap();

        let mut chunks = Vec::new();
        for mut stream in streams {
            let mut bytes = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                match stream.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => bytes.extend_from_slice(&buffer[..read]),
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                        ) =>
                    {
                        break;
                    }
                    Err(error) => panic!("tcp read failed: {error}"),
                }
            }
            if !bytes.is_empty() {
                chunks.push(Bytes::from(bytes));
            }
        }
        chunks
    });

    let cache = RemoteAssociationCache::new();
    let installer = crate::RemoteAssociationRouteInstaller::new(cache.clone());
    let dialer = TcpAssociationDialer::new(installer).with_connect_timeout(Duration::from_secs(1));
    let registration = dialer.dial(address(port)).unwrap();
    accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    cache.send(envelope(port, 9)).unwrap();
    drop(registration);
    drop(cache);

    let frames: Vec<RemoteStreamFrame> = handle
        .join()
        .unwrap()
        .into_iter()
        .flat_map(decode_stream)
        .collect();

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].stream_id(), RemoteStreamId::Ordinary);
    let decoded = decode_remote_envelope_frame(frames[0].payload().clone()).unwrap();
    assert_eq!(decoded.message.payload, Bytes::from_static(&[9]));
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
    assert_eq!(report.read.streams, 3);
    assert_eq!(report.read.frames, 1);
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
    let listener = listener.with_local_address(association_address("receiver", port));
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let accepted = listener.accept_association().unwrap();
        accepted_tx
            .send(accepted.remote_address().cloned())
            .unwrap();
        accepted.drain().unwrap()
    });

    let cache = RemoteAssociationCache::new();
    let installer = crate::RemoteAssociationRouteInstaller::new(cache.clone());
    let dialer = TcpAssociationDialer::new(installer)
        .with_local_address(remote_address.clone())
        .with_connect_timeout(Duration::from_secs(1));
    let registration = dialer.dial(association_address("receiver", port)).unwrap();

    assert_eq!(
        accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Some(remote_address)
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
        association_address("sender", 25521),
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
fn tcp_stream_reader_propagates_invalid_stream_frames() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let handler = Arc::new(CollectingFrameHandler::default());
    let remote_listener = TcpAssociationListener::from_listener(
        listener,
        handler.clone() as Arc<dyn RemoteFrameHandler>,
    )
    .with_expected_streams(1);
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let accepted = remote_listener.accept_association().unwrap();
        accepted_tx.send(()).unwrap();
        accepted.drain()
    });

    let stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let sink = TcpRemoteByteSink::from_stream("invalid", stream);
    sink.send_bytes(Bytes::from_static(b"not-a-stream"))
        .unwrap();
    drop(sink);

    let error = handle
        .join()
        .unwrap()
        .expect_err("invalid stream should fail");
    assert!(matches!(error, RemoteError::InvalidFrame(_)));
    assert!(handler.frames().is_empty());
}

#[test]
fn tcp_association_requires_port() {
    let address = RemoteAssociationAddress::new("kairo", "remote", "127.0.0.1", None).unwrap();

    let error = TcpRemoteByteSink::connect(&address, Some(Duration::from_millis(1)))
        .expect_err("tcp association without port should fail");

    assert!(matches!(error, RemoteError::InvalidRemoteRef(_, _)));
}

#[test]
fn tcp_stream_reader_accepts_single_encoded_lane_stream() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let handler = Arc::new(CollectingFrameHandler::default());
    let remote_listener = TcpAssociationListener::from_listener(
        listener,
        handler.clone() as Arc<dyn RemoteFrameHandler>,
    )
    .with_expected_streams(1);
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let accepted = remote_listener.accept_association().unwrap();
        accepted_tx.send(()).unwrap();
        accepted.drain().unwrap()
    });

    let stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let sink =
        Arc::new(TcpRemoteByteSink::from_stream("single", stream)) as Arc<dyn RemoteByteSink>;
    let writer = RemoteStreamWriter::new(RemoteStreamId::Control, sink);
    writer
        .send_frame_payload(Bytes::from_static(b"control-frame"))
        .unwrap();
    drop(writer);

    let report = handle.join().unwrap();
    assert_eq!(report.streams, 1);
    assert_eq!(report.frames, 1);
    assert_eq!(
        handler.frames(),
        vec![(
            RemoteStreamId::Control,
            Bytes::from_static(b"control-frame")
        )]
    );
}
