use crate::{Result, SerializationError};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Manifest(String);

impl Manifest {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn try_new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(SerializationError::InvalidManifest(value));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&'static str> for Manifest {
    fn from(value: &'static str) -> Self {
        Self::new(value)
    }
}
