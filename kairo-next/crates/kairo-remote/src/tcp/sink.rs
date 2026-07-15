#![deny(missing_docs)]

use std::io::{self, Write};
use std::net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;

use crate::{RemoteAssociationAddress, RemoteByteSink, RemoteError, RemoteStreamId, Result};

use super::{TcpAssociationHandshake, TcpAssociationIdentity, encode_tcp_association_handshake};

/// Thread-safe byte sink backed by one connected TCP lane stream.
#[derive(Debug)]
pub struct TcpRemoteByteSink {
    peer: String,
    stream: TcpStream,
    write_lock: Mutex<()>,
}

impl TcpRemoteByteSink {
    /// Connects an unhandshaken TCP stream to `address`.
    ///
    /// # Errors
    ///
    /// Returns an error when the address has no port, cannot be resolved, cannot
    /// be connected within `timeout`, or cannot enable low-latency writes.
    pub fn connect(address: &RemoteAssociationAddress, timeout: Option<Duration>) -> Result<Self> {
        Self::connect_stream(address, timeout)
            .map(|stream| Self::from_stream(address.to_string(), stream))
    }

    /// Connects a TCP lane and writes its association handshake.
    ///
    /// # Errors
    ///
    /// Returns an error for address resolution, connection setup, handshake
    /// encoding, or handshake write failure.
    pub fn connect_handshaken(
        address: &RemoteAssociationAddress,
        local_identity: &TcpAssociationIdentity,
        stream_id: RemoteStreamId,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let stream = Self::connect_handshaken_stream(address, local_identity, stream_id, timeout)?;
        Ok(Self::from_stream(address.to_string(), stream))
    }

    pub(crate) fn connect_handshaken_stream(
        address: &RemoteAssociationAddress,
        local_identity: &TcpAssociationIdentity,
        stream_id: RemoteStreamId,
        timeout: Option<Duration>,
    ) -> Result<TcpStream> {
        let mut stream = Self::connect_stream(address, timeout)?;
        let handshake =
            TcpAssociationHandshake::new(stream_id, local_identity.clone(), address.clone());
        stream
            .write_all(&encode_tcp_association_handshake(&handshake)?)
            .map_err(|error| tcp_outbound_failure(address, error))?;
        Ok(stream)
    }

    pub(crate) fn connect_stream(
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

    /// Wraps an already-connected stream as a remote byte sink.
    pub fn from_stream(peer: impl Into<String>, stream: TcpStream) -> Self {
        Self {
            peer: peer.into(),
            stream,
            write_lock: Mutex::new(()),
        }
    }

    /// Returns the peer label used in transport diagnostics.
    pub fn peer(&self) -> &str {
        &self.peer
    }
}

impl Drop for TcpRemoteByteSink {
    fn drop(&mut self) {
        let _ = self.stream.shutdown(Shutdown::Both);
    }
}

impl RemoteByteSink for TcpRemoteByteSink {
    fn send_bytes(&self, bytes: Bytes) -> Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .expect("tcp remote byte sink write lock poisoned");
        (&self.stream).write_all(&bytes).map_err(|error| {
            RemoteError::Outbound(format!("tcp write to {} failed: {error}", self.peer))
        })
    }

    fn close(&self) -> Result<()> {
        let result = self.stream.shutdown(Shutdown::Both);
        map_tcp_shutdown_result(&self.peer, result)
    }
}

fn map_tcp_shutdown_result(peer: &str, result: io::Result<()>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
        Err(error) => Err(RemoteError::Outbound(format!(
            "tcp close to {peer} failed: {error}"
        ))),
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
    use super::*;

    #[test]
    fn tcp_remote_byte_sink_close_treats_not_connected_as_idempotent_shutdown() {
        let result = map_tcp_shutdown_result(
            "kairo://peer@127.0.0.1:25520",
            Err(io::Error::from(io::ErrorKind::NotConnected)),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn tcp_remote_byte_sink_close_reports_other_shutdown_errors() {
        let error = map_tcp_shutdown_result(
            "kairo://peer@127.0.0.1:25520",
            Err(io::Error::new(io::ErrorKind::ConnectionReset, "reset")),
        )
        .expect_err("non-idempotent shutdown errors should propagate");

        assert!(matches!(error, RemoteError::Outbound(_)));
        assert!(
            error
                .to_string()
                .contains("tcp close to kairo://peer@127.0.0.1:25520 failed: reset")
        );
    }
}
