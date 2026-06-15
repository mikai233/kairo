use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;

/// Error returned while loading, parsing, validating, or projecting
/// format-neutral Kairo configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// A configuration file could not be read from disk.
    ReadFailed {
        /// Path that failed to load.
        path: PathBuf,
        /// Underlying I/O failure description.
        reason: String,
    },
    /// TOML input could not be parsed as a document table.
    ParseFailed {
        /// Parser failure description.
        reason: String,
    },
    /// A known configuration key had the wrong TOML type.
    InvalidType {
        /// Dot-separated configuration path.
        path: String,
        /// Human-readable expected type description.
        expected: String,
    },
    /// A known configuration key had an unsupported value.
    InvalidValue {
        /// Dot-separated configuration path.
        path: String,
        /// Human-readable validation failure.
        reason: String,
    },
    /// An unrecognized key was found in a strict configuration section.
    UnknownKey {
        /// Dot-separated unknown configuration path.
        path: String,
    },
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadFailed { path, reason } => {
                write!(f, "failed to read `{}`: {reason}", path.display())
            }
            Self::ParseFailed { reason } => write!(f, "failed to parse TOML: {reason}"),
            Self::InvalidType { path, expected } => {
                write!(f, "`{path}` must be {expected}")
            }
            Self::InvalidValue { path, reason } => write!(f, "`{path}` is invalid: {reason}"),
            Self::UnknownKey { path } => write!(f, "unknown configuration key `{path}`"),
        }
    }
}

impl std::error::Error for ConfigError {}
