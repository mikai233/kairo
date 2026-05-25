use std::io::Read;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use bytes::Bytes;

use crate::{RemoteError, RemoteFrameHandler, Result, StreamFrameInbound};

const DEFAULT_EXPECTED_LANE_STREAMS: usize = 3;
const DEFAULT_READ_CHUNK_LEN: usize = 8 * 1024;

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
        }
    }

    pub fn with_expected_streams(mut self, expected_streams: usize) -> Self {
        self.expected_streams = expected_streams.max(1);
        self
    }

    pub fn with_read_chunk_len(mut self, read_chunk_len: usize) -> Self {
        self.reader = self.reader.with_read_chunk_len(read_chunk_len);
        self
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|error| RemoteError::Inbound(format!("tcp local address failed: {error}")))
    }

    pub fn accept_association(&self) -> Result<TcpAcceptedAssociation> {
        let mut streams = Vec::with_capacity(self.expected_streams);
        for _ in 0..self.expected_streams {
            let (stream, peer) = self
                .listener
                .accept()
                .map_err(|error| RemoteError::Inbound(format!("tcp accept failed: {error}")))?;
            stream
                .set_nodelay(true)
                .map_err(|error| tcp_inbound_failure(&peer.to_string(), error))?;
            streams.push(TcpAcceptedStream { peer, stream });
        }
        Ok(TcpAcceptedAssociation {
            reader: self.reader.clone(),
            streams,
        })
    }
}

pub struct TcpAcceptedAssociation {
    reader: TcpAssociationStreamReader,
    streams: Vec<TcpAcceptedStream>,
}

impl TcpAcceptedAssociation {
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

fn tcp_inbound_failure(peer: &str, error: impl std::error::Error) -> RemoteError {
    RemoteError::Inbound(format!("tcp stream from {peer} failed: {error}"))
}
