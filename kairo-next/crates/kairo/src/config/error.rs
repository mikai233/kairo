use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    ReadFailed { path: PathBuf, reason: String },
    ParseFailed { reason: String },
    InvalidType { path: String, expected: String },
    InvalidValue { path: String, reason: String },
    UnknownKey { path: String },
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
