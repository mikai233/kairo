use std::io::Write;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;

use crate::{RemoteAssociationAddress, RemoteByteSink, RemoteError, RemoteStreamId, Result};

use super::{TcpAssociationHandshake, encode_tcp_association_handshake};

#[derive(Debug)]
pub struct TcpRemoteByteSink {
    peer: String,
    stream: Mutex<TcpStream>,
}

impl TcpRemoteByteSink {
    pub fn connect(address: &RemoteAssociationAddress, timeout: Option<Duration>) -> Result<Self> {
        Self::connect_stream(address, timeout)
            .map(|stream| Self::from_stream(address.to_string(), stream))
    }

    pub fn connect_handshaken(
        address: &RemoteAssociationAddress,
        local_address: &RemoteAssociationAddress,
        stream_id: RemoteStreamId,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let mut stream = Self::connect_stream(address, timeout)?;
        let handshake =
            TcpAssociationHandshake::new(stream_id, local_address.clone(), address.clone());
        stream
            .write_all(&encode_tcp_association_handshake(&handshake)?)
            .map_err(|error| tcp_outbound_failure(address, error))?;
        Ok(Self::from_stream(address.to_string(), stream))
    }

    fn connect_stream(
        address: &RemoteAssociationAddress,
        timeout: Option<Duration>,
    ) -> Result<TcpStream> {
        let socket_addr = resolve_socket_addr(address)?;
        let stream = match timeout {
            Some(timeout) => TcpStream::connect_timeout(&socket_addr, timeout),
            None => TcpStream::connect(socket_addr),
        }
        .map_err(|error| tcp_outbound_failure(address, error))?;
        stream
            .set_nodelay(true)
            .map_err(|error| tcp_outbound_failure(address, error))?;
        Ok(stream)
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
