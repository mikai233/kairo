use std::io::Read;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use bytes::Bytes;

use crate::{
    RemoteAssociationAddress, RemoteError, RemoteFrameHandler, Result, StreamFrameInbound,
};

use super::{
    TcpAssociationHandshake, TcpAssociationIdentity, read_tcp_association_handshake,
    validate_tcp_association_handshakes,
};

const DEFAULT_EXPECTED_LANE_STREAMS: usize = 3;
const DEFAULT_READ_CHUNK_LEN: usize = 8 * 1024;
const DEFAULT_ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone)]
pub struct TcpAssociationStreamReader {
    handler: Arc<dyn RemoteFrameHandler>,
    read_chunk_len: usize,
}

impl TcpAssociationStreamReader {
    pub fn new(handler: Arc<dyn RemoteFrameHandler>) -> Self {
        Self {
            handler,
            read_chunk_len: DEFAULT_READ_CHUNK_LEN,
        }
    }

    pub fn with_read_chunk_len(mut self, read_chunk_len: usize) -> Self {
        self.read_chunk_len = read_chunk_len.max(1);
        self
    }

    pub fn read_stream(
        &self,
        peer: impl Into<String>,
        mut stream: TcpStream,
    ) -> Result<TcpAssociationReadReport> {
        let peer = peer.into();
        let mut inbound = StreamFrameInbound::new(self.handler.clone());
        let mut buffer = vec![0_u8; self.read_chunk_len];
        let mut frames = 0_usize;

        loop {
            let read = stream
                .read(&mut buffer)
                .map_err(|error| tcp_inbound_failure(&peer, error))?;
            if read == 0 {
                break;
            }
            frames += inbound.push_bytes(Bytes::copy_from_slice(&buffer[..read]))?;
        }
        inbound.finish()?;

        Ok(TcpAssociationReadReport { streams: 1, frames })
    }
}

pub struct TcpAssociationListener {
    listener: TcpListener,
    reader: TcpAssociationStreamReader,
    expected_streams: usize,
    accept_poll_interval: Duration,
    local_address: Option<RemoteAssociationAddress>,
}

impl TcpAssociationListener {
    pub fn bind(address: impl ToSocketAddrs, handler: Arc<dyn RemoteFrameHandler>) -> Result<Self> {
        let listener = TcpListener::bind(address)
            .map_err(|error| RemoteError::Inbound(format!("tcp bind failed: {error}")))?;
        Ok(Self::from_listener(listener, handler))
    }

    pub fn from_listener(listener: TcpListener, handler: Arc<dyn RemoteFrameHandler>) -> Self {
        Self {
            listener,
            reader: TcpAssociationStreamReader::new(handler),
            expected_streams: DEFAULT_EXPECTED_LANE_STREAMS,
            accept_poll_interval: DEFAULT_ACCEPT_POLL_INTERVAL,
            local_address: None,
        }
    }

    pub fn with_expected_streams(mut self, expected_streams: usize) -> Self {
        self.expected_streams = expected_streams.max(1);
        self
    }

    pub fn with_local_address(mut self, local_address: RemoteAssociationAddress) -> Self {
        self.local_address = Some(local_address);
        self
    }

    pub fn with_read_chunk_len(mut self, read_chunk_len: usize) -> Self {
        self.reader = self.reader.with_read_chunk_len(read_chunk_len);
        self
    }

    pub fn with_accept_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.accept_poll_interval = if poll_interval.is_zero() {
            DEFAULT_ACCEPT_POLL_INTERVAL
        } else {
            poll_interval
        };
        self
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|error| RemoteError::Inbound(format!("tcp local address failed: {error}")))
    }

    pub fn accept_association(&self) -> Result<TcpAcceptedAssociation> {
        let mut streams = Vec::with_capacity(self.expected_streams);
        let mut handshakes = Vec::with_capacity(self.expected_streams);
        for _ in 0..self.expected_streams {
            let (mut stream, peer) = self
                .listener
                .accept()
                .map_err(|error| RemoteError::Inbound(format!("tcp accept failed: {error}")))?;
            stream
                .set_nodelay(true)
                .map_err(|error| tcp_inbound_failure(&peer.to_string(), error))?;
            self.read_handshake(&mut stream, &mut handshakes)?;
            streams.push(TcpAcceptedStream { peer, stream });
        }
        let remote_identity = self.validate_handshakes(&handshakes)?;
        Ok(TcpAcceptedAssociation {
            reader: self.reader.clone(),
            remote_identity,
            streams,
        })
    }

    pub fn spawn_accept_loop(self) -> Result<TcpAssociationListenerHandle> {
        let stop = Arc::new(AtomicBool::new(false));
        self.listener
            .set_nonblocking(true)
            .map_err(|error| RemoteError::Inbound(format!("tcp nonblocking failed: {error}")))?;
        let thread_stop = Arc::clone(&stop);
        let join = thread::spawn(move || self.run_accept_loop(thread_stop));
        Ok(TcpAssociationListenerHandle { stop, join })
    }

    fn run_accept_loop(self, stop: Arc<AtomicBool>) -> Result<TcpAssociationListenerReport> {
        let mut accepted_associations = 0_usize;
        let mut reader_handles = Vec::new();
        let mut first_error = None;

        while !stop.load(Ordering::SeqCst) {
            match self.try_accept_association(&stop) {
                Ok(Some(accepted)) => {
                    accepted_associations += 1;
                    reader_handles.push(accepted.spawn_lane_readers());
                }
                Ok(None) => thread::sleep(self.accept_poll_interval),
                Err(error) => {
                    first_error.get_or_insert(error);
                    break;
                }
            }
        }

        let mut read = TcpAssociationReadReport {
            streams: 0,
            frames: 0,
        };
        for handle in reader_handles {
            match handle.join() {
                Ok(report) => {
                    read.streams += report.streams;
                    read.frames += report.frames;
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(TcpAssociationListenerReport {
                accepted_associations,
                read,
            })
        }
    }

    fn try_accept_association(&self, stop: &AtomicBool) -> Result<Option<TcpAcceptedAssociation>> {
        let mut streams = Vec::with_capacity(self.expected_streams);
        let mut handshakes = Vec::with_capacity(self.expected_streams);
        while streams.len() < self.expected_streams {
            match self.listener.accept() {
                Ok((stream, peer)) => {
                    stream
                        .set_nonblocking(false)
                        .map_err(|error| tcp_inbound_failure(&peer.to_string(), error))?;
                    stream
                        .set_nodelay(true)
                        .map_err(|error| tcp_inbound_failure(&peer.to_string(), error))?;
                    let mut stream = stream;
                    self.read_handshake(&mut stream, &mut handshakes)?;
                    streams.push(TcpAcceptedStream { peer, stream });
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if streams.is_empty() {
                        return Ok(None);
                    }
                    if stop.load(Ordering::SeqCst) {
                        return Err(RemoteError::Inbound(
                            "tcp accept stopped before all association lane streams arrived"
                                .to_string(),
                        ));
                    }
                    thread::sleep(self.accept_poll_interval);
                }
                Err(error) => {
                    return Err(RemoteError::Inbound(format!("tcp accept failed: {error}")));
                }
            }
        }
        let remote_identity = self.validate_handshakes(&handshakes)?;
        Ok(Some(TcpAcceptedAssociation {
            reader: self.reader.clone(),
            remote_identity,
            streams,
        }))
    }

    fn read_handshake(
        &self,
        stream: &mut TcpStream,
        handshakes: &mut Vec<TcpAssociationHandshake>,
    ) -> Result<()> {
        if self.local_address.is_some() {
            handshakes.push(read_tcp_association_handshake(stream)?);
        }
        Ok(())
    }

    fn validate_handshakes(
        &self,
        handshakes: &[TcpAssociationHandshake],
    ) -> Result<Option<TcpAssociationIdentity>> {
        match &self.local_address {
            Some(local_address) => validate_tcp_association_handshakes(
                local_address,
                self.expected_streams,
                handshakes,
            ),
            None => Ok(None),
        }
    }
}

pub struct TcpAssociationListenerHandle {
    stop: Arc<AtomicBool>,
    join: JoinHandle<Result<TcpAssociationListenerReport>>,
}

impl TcpAssociationListenerHandle {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    pub fn join(self) -> Result<TcpAssociationListenerReport> {
        self.join
            .join()
            .map_err(|_| RemoteError::Inbound("tcp association listener panicked".to_string()))?
    }
}

pub struct TcpAcceptedAssociation {
    reader: TcpAssociationStreamReader,
    remote_identity: Option<TcpAssociationIdentity>,
    streams: Vec<TcpAcceptedStream>,
}

impl TcpAcceptedAssociation {
    pub fn remote_address(&self) -> Option<&RemoteAssociationAddress> {
        self.remote_identity
            .as_ref()
            .map(TcpAssociationIdentity::address)
    }

    pub fn remote_uid(&self) -> Option<u64> {
        self.remote_identity
            .as_ref()
            .map(TcpAssociationIdentity::uid)
    }

    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    pub fn drain(self) -> Result<TcpAssociationReadReport> {
        let mut report = TcpAssociationReadReport {
            streams: 0,
            frames: 0,
        };
        for accepted in self.streams {
            let stream_report = self
                .reader
                .read_stream(accepted.peer.to_string(), accepted.stream)?;
            report.streams += stream_report.streams;
            report.frames += stream_report.frames;
        }
        Ok(report)
    }

    pub fn spawn_lane_readers(self) -> TcpAssociationReaderHandle {
        let joins = self
            .streams
            .into_iter()
            .map(|accepted| {
                let reader = self.reader.clone();
                thread::spawn(move || {
                    reader.read_stream(accepted.peer.to_string(), accepted.stream)
                })
            })
            .collect();
        TcpAssociationReaderHandle { joins }
    }
}

struct TcpAcceptedStream {
    peer: SocketAddr,
    stream: TcpStream,
}

pub struct TcpAssociationReaderHandle {
    joins: Vec<JoinHandle<Result<TcpAssociationReadReport>>>,
}

impl TcpAssociationReaderHandle {
    pub fn join(self) -> Result<TcpAssociationReadReport> {
        let mut report = TcpAssociationReadReport {
            streams: 0,
            frames: 0,
        };
        let mut first_error = None;
        for join in self.joins {
            match join.join() {
                Ok(Ok(stream_report)) => {
                    report.streams += stream_report.streams;
                    report.frames += stream_report.frames;
                }
                Ok(Err(error)) => {
                    first_error.get_or_insert(error);
                }
                Err(_) => {
                    first_error.get_or_insert_with(|| {
                        RemoteError::Inbound("tcp lane reader panicked".to_string())
                    });
                }
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(report)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpAssociationReadReport {
    pub streams: usize,
    pub frames: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpAssociationListenerReport {
    pub accepted_associations: usize,
    pub read: TcpAssociationReadReport,
}

fn tcp_inbound_failure(peer: &str, error: impl std::error::Error) -> RemoteError {
    RemoteError::Inbound(format!("tcp stream from {peer} failed: {error}"))
}
