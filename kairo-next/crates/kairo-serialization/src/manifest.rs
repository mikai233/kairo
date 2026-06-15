use crate::{Result, SerializationError};

/// Stable remote message manifest.
///
/// Manifests are user- or system-chosen wire names. They are intentionally
/// separate from Rust type names so payload compatibility can survive refactors
/// and rolling upgrades.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Manifest(String);

impl Manifest {
    /// Creates a manifest without validation.
    ///
    /// Prefer [`Self::try_new`] for user input or registry construction.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Creates a validated non-empty manifest.
    pub fn try_new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(SerializationError::InvalidManifest(value));
        }
        Ok(Self(value))
    }

    /// Returns the manifest string exactly as stored.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&'static str> for Manifest {
    fn from(value: &'static str) -> Self {
        Self::new(value)
    }
}
