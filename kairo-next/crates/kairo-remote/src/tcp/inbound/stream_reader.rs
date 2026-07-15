#![deny(missing_docs)]

use std::io::Read;
use std::net::TcpStream;
use std::sync::Arc;

use bytes::Bytes;

use crate::{RemoteFrameHandler, Result, StreamFrameInbound};

use super::DEFAULT_READ_CHUNK_LEN;
use super::error::tcp_inbound_failure;
use super::reports::TcpAssociationReadReport;

/// Blocking reader that decodes a TCP lane stream and dispatches remote frames.
#[derive(Clone)]
pub struct TcpAssociationStreamReader {
    handler: Arc<dyn RemoteFrameHandler>,
    read_chunk_len: usize,
}

impl TcpAssociationStreamReader {
    /// Creates a reader that dispatches decoded frames to `handler`.
    pub fn new(handler: Arc<dyn RemoteFrameHandler>) -> Self {
        Self {
            handler,
            read_chunk_len: DEFAULT_READ_CHUNK_LEN,
        }
    }

    /// Sets the per-read buffer size, clamped to at least one byte.
    pub fn with_read_chunk_len(mut self, read_chunk_len: usize) -> Self {
        self.read_chunk_len = read_chunk_len.max(1);
        self
    }

    /// Clones this reader's settings with a different frame handler.
    pub fn with_handler(&self, handler: Arc<dyn RemoteFrameHandler>) -> Self {
        Self {
            handler,
            read_chunk_len: self.read_chunk_len,
        }
    }

    /// Reads one stream to EOF, dispatching every complete frame.
    ///
    /// # Errors
    ///
    /// Returns an error for TCP reads, invalid or truncated stream framing, or
    /// frame-handler failure.
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
