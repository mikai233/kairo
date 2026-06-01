use std::io::Read;
use std::net::TcpListener;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use bytes::Bytes;

use super::*;
use crate::{
    RemoteAssociationCache, RemoteByteSink, RemoteError, RemoteOutbound, RemoteStreamDecoder,
    RemoteStreamFrame, RemoteStreamId, RemoteStreamWriter, decode_remote_envelope_frame,
};

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

fn decode_stream(bytes: Bytes) -> Vec<RemoteStreamFrame> {
    let mut decoder = RemoteStreamDecoder::new();
    let frames = decoder.push(bytes).unwrap();
    decoder.finish().unwrap();
    frames
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
