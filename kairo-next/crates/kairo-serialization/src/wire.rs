use bytes::Bytes;

use crate::{Result, SerializationError};

/// Small deterministic writer for Kairo system-protocol payloads.
///
/// `WireWriter` is intentionally minimal: it writes explicit length prefixes,
/// presence markers, and big-endian numeric values. It is for hand-written
/// stable system codecs, not for deriving arbitrary Rust memory layout.
#[derive(Debug, Default)]
pub struct WireWriter {
    bytes: Vec<u8>,
}

impl WireWriter {
    /// Creates an empty wire writer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Writes a UTF-8 string with a `u32` big-endian byte-length prefix.
    pub fn write_string(&mut self, value: &str) -> Result<()> {
        let bytes = value.as_bytes();
        let len = u32::try_from(bytes.len()).map_err(|_| {
            SerializationError::Message("wire string exceeds u32 length".to_string())
        })?;
        self.bytes.extend_from_slice(&len.to_be_bytes());
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    /// Writes an optional UTF-8 string with a one-byte presence marker.
    pub fn write_optional_string(&mut self, value: Option<&str>) -> Result<()> {
        match value {
            Some(value) => {
                self.bytes.push(1);
                self.write_string(value)?;
            }
            None => self.bytes.push(0),
        }
        Ok(())
    }

    /// Writes bytes with a `u32` big-endian length prefix.
    pub fn write_bytes(&mut self, value: &Bytes) -> Result<()> {
        let len = u32::try_from(value.len())
            .map_err(|_| SerializationError::Message("wire bytes exceed u32 length".to_string()))?;
        self.bytes.extend_from_slice(&len.to_be_bytes());
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    /// Writes a boolean as a one-byte `0` or `1` marker.
    pub fn write_bool(&mut self, value: bool) {
        self.write_u8(u8::from(value));
    }

    /// Writes one unsigned byte.
    pub fn write_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    /// Writes a `u16` in big-endian byte order.
    pub fn write_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    /// Writes a `u32` in big-endian byte order.
    pub fn write_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    /// Writes a `u64` in big-endian byte order.
    pub fn write_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    /// Writes a `u128` in big-endian byte order.
    pub fn write_u128(&mut self, value: u128) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    /// Writes an optional `u64` with a one-byte presence marker.
    pub fn write_optional_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.bytes.push(1);
                self.write_u64(value);
            }
            None => self.bytes.push(0),
        }
    }

    /// Finishes the writer and returns the accumulated bytes.
    pub fn finish(self) -> Bytes {
        Bytes::from(self.bytes)
    }
}

/// Sequential reader for bytes produced by [`WireWriter`].
///
/// The reader consumes values in the same order they were written and reports
/// explicit errors for early EOF, invalid markers, invalid UTF-8, or length
/// overflow.
#[derive(Debug)]
pub struct WireReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> WireReader<'a> {
    /// Creates a reader over an existing byte buffer.
    pub fn new(bytes: &'a Bytes) -> Self {
        Self {
            bytes: bytes.as_ref(),
            cursor: 0,
        }
    }

    /// Reads a length-prefixed UTF-8 string.
    pub fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|error| {
            SerializationError::Message(format!("wire string is not utf-8: {error}"))
        })
    }

    /// Reads length-prefixed bytes.
    pub fn read_bytes(&mut self) -> Result<Bytes> {
        let len = self.read_u32()? as usize;
        Ok(Bytes::copy_from_slice(self.read_exact(len)?))
    }

    /// Reads an optional UTF-8 string.
    pub fn read_optional_string(&mut self) -> Result<Option<String>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_string().map(Some),
            other => Err(SerializationError::Message(format!(
                "invalid optional string marker {other}"
            ))),
        }
    }

    /// Reads an optional `u64`.
    pub fn read_optional_u64(&mut self) -> Result<Option<u64>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_u64().map(Some),
            other => Err(SerializationError::Message(format!(
                "invalid optional u64 marker {other}"
            ))),
        }
    }

    /// Reads one unsigned byte.
    pub fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    /// Reads a boolean marker written by [`WireWriter::write_bool`].
    pub fn read_bool(&mut self) -> Result<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(SerializationError::Message(format!(
                "invalid bool marker {other}"
            ))),
        }
    }

    /// Reads a big-endian `u16`.
    pub fn read_u16(&mut self) -> Result<u16> {
        let mut bytes = [0; 2];
        bytes.copy_from_slice(self.read_exact(2)?);
        Ok(u16::from_be_bytes(bytes))
    }

    /// Reads a big-endian `u32`.
    pub fn read_u32(&mut self) -> Result<u32> {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(self.read_exact(4)?);
        Ok(u32::from_be_bytes(bytes))
    }

    /// Reads a big-endian `u64`.
    pub fn read_u64(&mut self) -> Result<u64> {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(self.read_exact(8)?);
        Ok(u64::from_be_bytes(bytes))
    }

    /// Reads a big-endian `u128`.
    pub fn read_u128(&mut self) -> Result<u128> {
        let mut bytes = [0; 16];
        bytes.copy_from_slice(self.read_exact(16)?);
        Ok(u128::from_be_bytes(bytes))
    }

    /// Reads exactly `len` bytes from the current cursor.
    pub fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.cursor.checked_add(len).ok_or_else(|| {
            SerializationError::Message("wire payload length overflow".to_string())
        })?;
        let Some(bytes) = self.bytes.get(self.cursor..end) else {
            return Err(SerializationError::Message(
                "wire payload ended early".to_string(),
            ));
        };
        self.cursor = end;
        Ok(bytes)
    }
}
