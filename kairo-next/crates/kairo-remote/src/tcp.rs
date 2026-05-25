use std::io::Write;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;

use crate::{
    RemoteAssociationAddress, RemoteAssociationRouteInstaller, RemoteAssociationRouteRegistration,
    RemoteByteSink, RemoteError, Result,
};

#[derive(Clone)]
pub struct TcpAssociationDialer {
    installer: RemoteAssociationRouteInstaller,
    connect_timeout: Option<Duration>,
}

impl TcpAssociationDialer {
    pub fn new(installer: RemoteAssociationRouteInstaller) -> Self {
        Self {
            installer,
            connect_timeout: None,
        }
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    pub fn installer(&self) -> &RemoteAssociationRouteInstaller {
        &self.installer
    }

    pub fn dial(
        &self,
        address: RemoteAssociationAddress,
    ) -> Result<RemoteAssociationRouteRegistration> {
        let control = TcpRemoteByteSink::connect(&address, self.connect_timeout)?;
        let ordinary = TcpRemoteByteSink::connect(&address, self.connect_timeout)?;
        let large = TcpRemoteByteSink::connect(&address, self.connect_timeout)?;

        Ok(self.installer.insert_stream_pipeline(
            address,
            Arc::new(control),
            Arc::new(ordinary),
            Arc::new(large),
        ))
    }
}

#[derive(Debug)]
pub struct TcpRemoteByteSink {
    peer: String,
    stream: Mutex<TcpStream>,
}

impl TcpRemoteByteSink {
    pub fn connect(address: &RemoteAssociationAddress, timeout: Option<Duration>) -> Result<Self> {
        let socket_addr = resolve_socket_addr(address)?;
        let stream = match timeout {
            Some(timeout) => TcpStream::connect_timeout(&socket_addr, timeout),
            None => TcpStream::connect(socket_addr),
        }
        .map_err(|error| tcp_outbound_failure(address, error))?;
        stream
            .set_nodelay(true)
            .map_err(|error| tcp_outbound_failure(address, error))?;
        Ok(Self::from_stream(address.to_string(), stream))
    }

    pub fn from_stream(peer: impl Into<String>, stream: TcpStream) -> Self {
        Self {
            peer: peer.into(),
            stream: Mutex::new(stream),
        }
    }

    pub fn peer(&self) -> &str {
        &self.peer
    }
}

impl RemoteByteSink for TcpRemoteByteSink {
    fn send_bytes(&self, bytes: Bytes) -> Result<()> {
        self.stream
            .lock()
            .expect("tcp remote byte sink mutex poisoned")
            .write_all(&bytes)
            .map_err(|error| {
                RemoteError::Outbound(format!("tcp write to {} failed: {error}", self.peer))
            })
    }
}

fn resolve_socket_addr(address: &RemoteAssociationAddress) -> Result<SocketAddr> {
    let Some(port) = address.port() else {
        return Err(RemoteError::InvalidRemoteRef(
            address.to_string(),
            "tcp association requires a port".to_string(),
        ));
    };
    (address.host(), port)
        .to_socket_addrs()
        .map_err(|error| tcp_outbound_failure(address, error))?
        .next()
        .ok_or_else(|| {
            RemoteError::Outbound(format!(
                "tcp association {} resolved to no socket addresses",
                address
            ))
        })
}

fn tcp_outbound_failure(
    address: &RemoteAssociationAddress,
    error: impl std::error::Error,
) -> RemoteError {
    RemoteError::Outbound(format!("tcp association {address} failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::io::{ErrorKind, Read};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    use kairo_serialization::{ActorRefWireData, Manifest, RemoteEnvelope, SerializedMessage};

    use super::*;
    use crate::{
        RemoteAssociationCache, RemoteOutbound, RemoteStreamDecoder, RemoteStreamFrame,
        RemoteStreamId, decode_remote_envelope_frame,
    };

    fn address(port: u16) -> RemoteAssociationAddress {
        RemoteAssociationAddress::new("kairo", "remote", "127.0.0.1", Some(port)).unwrap()
    }

    fn envelope(port: u16, value: u8) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new(format!("kairo://remote@127.0.0.1:{port}/user/target")).unwrap(),
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

        let sink =
            TcpRemoteByteSink::connect(&address(port), Some(Duration::from_secs(1))).unwrap();
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
                                ErrorKind::TimedOut | ErrorKind::WouldBlock
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
        let installer = RemoteAssociationRouteInstaller::new(cache.clone());
        let dialer =
            TcpAssociationDialer::new(installer).with_connect_timeout(Duration::from_secs(1));
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
    fn tcp_association_requires_port() {
        let address = RemoteAssociationAddress::new("kairo", "remote", "127.0.0.1", None).unwrap();

        let error = TcpRemoteByteSink::connect(&address, Some(Duration::from_millis(1)))
            .expect_err("tcp association without port should fail");

        assert!(matches!(error, RemoteError::InvalidRemoteRef(_, _)));
    }
}
