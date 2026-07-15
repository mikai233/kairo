#![deny(missing_docs)]

use bytes::Bytes;

use crate::{RemoteError, Result};

const REMOTE_STREAM_MAGIC: [u8; 4] = *b"KAIR";
const REMOTE_STREAM_HEADER_LEN: usize = REMOTE_STREAM_MAGIC.len() + 1;
const REMOTE_STREAM_FRAME_HEADER_LEN: usize = 4;
const DEFAULT_MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

/// Stable identifier for an independent remote transport stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RemoteStreamId {
    /// Lifecycle and system protocol traffic.
    Control,
    /// Normal user and extension traffic.
    Ordinary,
    /// User or extension traffic above the configured large-frame threshold.
    Large,
}

impl RemoteStreamId {
    /// Stable wire identifier for the control stream.
    pub const CONTROL_ID: u8 = 1;
    /// Stable wire identifier for the ordinary stream.
    pub const ORDINARY_ID: u8 = 2;
    /// Stable wire identifier for the large stream.
    pub const LARGE_ID: u8 = 3;

    /// Returns the stable wire identifier for this stream.
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Control => Self::CONTROL_ID,
            Self::Ordinary => Self::ORDINARY_ID,
            Self::Large => Self::LARGE_ID,
        }
    }

    /// Decodes a stable stream identifier from its wire value.
    pub fn try_from_u8(value: u8) -> Result<Self> {
        match value {
            Self::CONTROL_ID => Ok(Self::Control),
            Self::ORDINARY_ID => Ok(Self::Ordinary),
            Self::LARGE_ID => Ok(Self::Large),
            other => Err(RemoteError::InvalidFrame(format!(
                "unknown remote stream id {other}"
            ))),
        }
    }
}

/// One decoded length-prefixed frame associated with its remote stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteStreamFrame {
    stream_id: RemoteStreamId,
    payload: Bytes,
}

impl RemoteStreamFrame {
    /// Creates a decoded stream frame.
    pub fn new(stream_id: RemoteStreamId, payload: Bytes) -> Self {
        Self { stream_id, payload }
    }

    /// Returns the stream that carried the frame.
    pub fn stream_id(&self) -> RemoteStreamId {
        self.stream_id
    }

    /// Borrows the frame payload.
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// Consumes the frame and returns its payload.
    pub fn into_payload(self) -> Bytes {
        self.payload
    }
}

/// Stateful encoder for one remote stream.
///
/// The stream header is emitted with the first frame only; later frames contain
/// just their length prefix and payload.
#[derive(Debug, Clone)]
pub struct RemoteStreamEncoder {
    stream_id: RemoteStreamId,
    header_written: bool,
}

impl RemoteStreamEncoder {
    /// Creates an encoder for `stream_id`.
    pub fn new(stream_id: RemoteStreamId) -> Self {
        Self {
            stream_id,
            header_written: false,
        }
    }

    /// Returns the stream encoded by this instance.
    pub fn stream_id(&self) -> RemoteStreamId {
        self.stream_id
    }

    /// Returns whether the stream header has already been emitted.
    pub fn header_written(&self) -> bool {
        self.header_written
    }

    /// Encodes one frame, prepending the stream header on the first call.
    pub fn encode_frame(&mut self, payload: &Bytes) -> Result<Bytes> {
        let mut bytes = Vec::with_capacity(
            if self.header_written {
                0
            } else {
                REMOTE_STREAM_HEADER_LEN
            } + REMOTE_STREAM_FRAME_HEADER_LEN
                + payload.len(),
        );
        if !self.header_written {
            bytes.extend_from_slice(&encode_remote_stream_header(self.stream_id));
            self.header_written = true;
        }
        bytes.extend_from_slice(&encode_remote_stream_frame(payload)?);
        Ok(Bytes::from(bytes))
    }
}

/// Incrementally decodes one remote stream from arbitrary byte chunks.
#[derive(Debug, Clone)]
pub struct RemoteStreamDecoder {
    max_frame_len: usize,
    stream_id: Option<RemoteStreamId>,
    buffer: Vec<u8>,
}

impl RemoteStreamDecoder {
    /// Creates a decoder with the default 16 MiB maximum frame length.
    pub fn new() -> Self {
        Self::with_max_frame_len(DEFAULT_MAX_FRAME_LEN)
    }

    /// Creates a decoder that rejects declared frames larger than
    /// `max_frame_len` before buffering their payloads.
    pub fn with_max_frame_len(max_frame_len: usize) -> Self {
        Self {
            max_frame_len,
            stream_id: None,
            buffer: Vec::new(),
        }
    }

    /// Returns the decoded stream identifier, if the header has arrived.
    pub fn stream_id(&self) -> Option<RemoteStreamId> {
        self.stream_id
    }

    /// Adds a byte chunk and returns every frame completed by the new bytes.
    pub fn push(&mut self, chunk: Bytes) -> Result<Vec<RemoteStreamFrame>> {
        self.buffer.extend_from_slice(&chunk);
        self.decode_available()
    }

    /// Finishes decoding, rejecting a partial header or frame.
    pub fn finish(self) -> Result<()> {
        if self.buffer.is_empty() {
            Ok(())
        } else if self.stream_id.is_none() && self.buffer.len() < REMOTE_STREAM_HEADER_LEN {
            Err(RemoteError::InvalidFrame(
                "truncated remote stream header".to_string(),
            ))
        } else {
            Err(RemoteError::InvalidFrame(
                "truncated remote stream frame".to_string(),
            ))
        }
    }

    fn decode_available(&mut self) -> Result<Vec<RemoteStreamFrame>> {
        let mut frames = Vec::new();

        if self.stream_id.is_none() {
            if self.buffer.len() < REMOTE_STREAM_HEADER_LEN {
                return Ok(frames);
            }
            let stream_id = decode_remote_stream_header(&self.buffer[..REMOTE_STREAM_HEADER_LEN])?;
            self.buffer.drain(..REMOTE_STREAM_HEADER_LEN);
            self.stream_id = Some(stream_id);
        }

        let stream_id = self.stream_id.expect("stream id is decoded");
        loop {
            if self.buffer.len() < REMOTE_STREAM_FRAME_HEADER_LEN {
                return Ok(frames);
            }
            let frame_len = frame_len(&self.buffer[..REMOTE_STREAM_FRAME_HEADER_LEN])?;
            if frame_len > self.max_frame_len {
                return Err(RemoteError::InvalidFrame(format!(
                    "remote stream frame length {frame_len} exceeds max {}",
                    self.max_frame_len
                )));
            }
            let needed = REMOTE_STREAM_FRAME_HEADER_LEN + frame_len;
            if self.buffer.len() < needed {
                return Ok(frames);
            }
            let payload =
                Bytes::copy_from_slice(&self.buffer[REMOTE_STREAM_FRAME_HEADER_LEN..needed]);
            self.buffer.drain(..needed);
            frames.push(RemoteStreamFrame::new(stream_id, payload));
        }
    }
}

impl Default for RemoteStreamDecoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Encodes the magic and stable identifier header for one remote stream.
pub fn encode_remote_stream_header(stream_id: RemoteStreamId) -> Bytes {
    let mut bytes = Vec::with_capacity(REMOTE_STREAM_HEADER_LEN);
    bytes.extend_from_slice(&REMOTE_STREAM_MAGIC);
    bytes.push(stream_id.as_u8());
    Bytes::from(bytes)
}

/// Validates and decodes a complete remote stream header.
pub fn decode_remote_stream_header(bytes: &[u8]) -> Result<RemoteStreamId> {
    if bytes.len() < REMOTE_STREAM_HEADER_LEN {
        return Err(RemoteError::InvalidFrame(
            "truncated remote stream header".to_string(),
        ));
    }
    if bytes[..REMOTE_STREAM_MAGIC.len()] != REMOTE_STREAM_MAGIC {
        return Err(RemoteError::InvalidFrame(
            "invalid remote stream magic".to_string(),
        ));
    }
    RemoteStreamId::try_from_u8(bytes[REMOTE_STREAM_MAGIC.len()])
}

/// Encodes one length-prefixed remote stream frame without a stream header.
pub fn encode_remote_stream_frame(payload: &Bytes) -> Result<Bytes> {
    let frame_len = u32::try_from(payload.len()).map_err(|_| {
        RemoteError::InvalidFrame("remote stream frame exceeds u32 length".to_string())
    })?;
    let mut bytes = Vec::with_capacity(REMOTE_STREAM_FRAME_HEADER_LEN + payload.len());
    bytes.extend_from_slice(&frame_len.to_be_bytes());
    bytes.extend_from_slice(payload);
    Ok(Bytes::from(bytes))
}

fn frame_len(bytes: &[u8]) -> Result<usize> {
    if bytes.len() < REMOTE_STREAM_FRAME_HEADER_LEN {
        return Err(RemoteError::InvalidFrame(
            "truncated remote stream frame header".to_string(),
        ));
    }
    let mut len = [0; REMOTE_STREAM_FRAME_HEADER_LEN];
    len.copy_from_slice(&bytes[..REMOTE_STREAM_FRAME_HEADER_LEN]);
    Ok(u32::from_be_bytes(len) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(payload: &'static [u8]) -> Bytes {
        encode_remote_stream_frame(&Bytes::from_static(payload)).unwrap()
    }

    #[test]
    fn stream_header_uses_explicit_magic_and_ids() {
        let header = encode_remote_stream_header(RemoteStreamId::Control);

        assert_eq!(header, Bytes::from_static(b"KAIR\x01"));
        assert_eq!(
            decode_remote_stream_header(&header).unwrap(),
            RemoteStreamId::Control
        );
        assert_eq!(RemoteStreamId::Ordinary.as_u8(), 2);
        assert_eq!(RemoteStreamId::Large.as_u8(), 3);
    }

    #[test]
    fn stream_encoder_writes_connection_header_once() {
        let mut encoder = RemoteStreamEncoder::new(RemoteStreamId::Ordinary);

        let first = encoder.encode_frame(&Bytes::from_static(b"abc")).unwrap();
        let second = encoder.encode_frame(&Bytes::from_static(b"x")).unwrap();

        assert_eq!(&first[..5], b"KAIR\x02");
        assert_eq!(&first[5..9], &[0, 0, 0, 3]);
        assert_eq!(&first[9..], b"abc");
        assert_eq!(&second[..4], &[0, 0, 0, 1]);
        assert_eq!(&second[4..], b"x");
    }

    #[test]
    fn stream_decoder_buffers_chunks_until_complete_frames_arrive() {
        let mut decoder = RemoteStreamDecoder::new();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&encode_remote_stream_header(RemoteStreamId::Control));
        bytes.extend_from_slice(&frame(b"one"));
        bytes.extend_from_slice(&frame(b"two"));

        assert!(
            decoder
                .push(Bytes::copy_from_slice(&bytes[..3]))
                .unwrap()
                .is_empty()
        );
        assert!(
            decoder
                .push(Bytes::copy_from_slice(&bytes[3..8]))
                .unwrap()
                .is_empty()
        );
        let frames = decoder.push(Bytes::copy_from_slice(&bytes[8..12])).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0],
            RemoteStreamFrame::new(RemoteStreamId::Control, Bytes::from_static(b"one"))
        );

        let frames = decoder.push(Bytes::copy_from_slice(&bytes[12..])).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].stream_id(), RemoteStreamId::Control);
        assert_eq!(frames[0].payload(), &Bytes::from_static(b"two"));
        decoder.finish().unwrap();
    }

    #[test]
    fn stream_decoder_decodes_multiple_frames_from_one_chunk() {
        let mut encoder = RemoteStreamEncoder::new(RemoteStreamId::Large);
        let mut bytes = encoder
            .encode_frame(&Bytes::from_static(b"large-a"))
            .unwrap()
            .to_vec();
        bytes.extend_from_slice(
            &encoder
                .encode_frame(&Bytes::from_static(b"large-b"))
                .unwrap(),
        );

        let frames = RemoteStreamDecoder::new().push(Bytes::from(bytes)).unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].stream_id(), RemoteStreamId::Large);
        assert_eq!(frames[0].payload(), &Bytes::from_static(b"large-a"));
        assert_eq!(frames[1].payload(), &Bytes::from_static(b"large-b"));
    }

    #[test]
    fn stream_decoder_rejects_invalid_magic() {
        let error = RemoteStreamDecoder::new()
            .push(Bytes::from_static(b"NOPE\x02"))
            .unwrap_err();

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(error.to_string().contains("magic"));
    }

    #[test]
    fn stream_decoder_rejects_unknown_stream_id() {
        let error = RemoteStreamDecoder::new()
            .push(Bytes::from_static(b"KAIR\x09"))
            .unwrap_err();

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(error.to_string().contains("stream id"));
    }

    #[test]
    fn stream_decoder_rejects_oversized_frame_before_payload_arrives() {
        let mut decoder = RemoteStreamDecoder::with_max_frame_len(2);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&encode_remote_stream_header(RemoteStreamId::Ordinary));
        bytes.extend_from_slice(&[0, 0, 0, 3]);

        let error = decoder.push(Bytes::from(bytes)).unwrap_err();

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(error.to_string().contains("exceeds max"));
    }

    #[test]
    fn stream_decoder_finish_rejects_truncated_header_or_frame() {
        let mut header_decoder = RemoteStreamDecoder::new();
        assert!(
            header_decoder
                .push(Bytes::from_static(b"KAI"))
                .unwrap()
                .is_empty()
        );
        let error = header_decoder.finish().unwrap_err();
        assert!(error.to_string().contains("header"));

        let mut frame_decoder = RemoteStreamDecoder::new();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&encode_remote_stream_header(RemoteStreamId::Ordinary));
        bytes.extend_from_slice(&[0, 0, 0, 5, b'p']);
        assert!(frame_decoder.push(Bytes::from(bytes)).unwrap().is_empty());
        let error = frame_decoder.finish().unwrap_err();
        assert!(error.to_string().contains("frame"));
    }
}
