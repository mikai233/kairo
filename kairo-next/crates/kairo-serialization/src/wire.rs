use bytes::Bytes;

use crate::{Result, SerializationError};

#[derive(Debug, Default)]
pub struct WireWriter {
    bytes: Vec<u8>,
}

impl WireWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn write_string(&mut self, value: &str) -> Result<()> {
        let bytes = value.as_bytes();
        let len = u32::try_from(bytes.len()).map_err(|_| {
            SerializationError::Message("wire string exceeds u32 length".to_string())
        })?;
        self.bytes.extend_from_slice(&len.to_be_bytes());
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

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

    pub fn write_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_u128(&mut self, value: u128) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_optional_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.bytes.push(1);
                self.write_u64(value);
            }
            None => self.bytes.push(0),
        }
    }

    pub fn finish(self) -> Bytes {
        Bytes::from(self.bytes)
    }
}

#[derive(Debug)]
pub struct WireReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> WireReader<'a> {
    pub fn new(bytes: &'a Bytes) -> Self {
        Self {
            bytes: bytes.as_ref(),
            cursor: 0,
        }
    }

    pub fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|error| {
            SerializationError::Message(format!("wire string is not utf-8: {error}"))
        })
    }

    pub fn read_optional_string(&mut self) -> Result<Option<String>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_string().map(Some),
            other => Err(SerializationError::Message(format!(
                "invalid optional string marker {other}"
            ))),
        }
    }

    pub fn read_optional_u64(&mut self) -> Result<Option<u64>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_u64().map(Some),
            other => Err(SerializationError::Message(format!(
                "invalid optional u64 marker {other}"
            ))),
        }
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(self.read_exact(4)?);
        Ok(u32::from_be_bytes(bytes))
    }

    pub fn read_u64(&mut self) -> Result<u64> {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(self.read_exact(8)?);
        Ok(u64::from_be_bytes(bytes))
    }

    pub fn read_u128(&mut self) -> Result<u128> {
        let mut bytes = [0; 16];
        bytes.copy_from_slice(self.read_exact(16)?);
        Ok(u128::from_be_bytes(bytes))
    }

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
