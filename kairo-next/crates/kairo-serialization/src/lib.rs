//! Stable serialization contracts for remote and persistent messages.

mod codec;
mod envelope;
mod errors;
mod manifest;
mod message;
mod registry;

pub use codec::{DynCodec, MessageCodec};
pub use envelope::{RemoteEnvelope, SerializedMessage};
pub use errors::{Result, SerializationError};
pub use manifest::Manifest;
pub use message::{RemoteMessage, SerializerId};
pub use registry::{Registry, SerializationRegistry};

#[cfg(test)]
mod tests;
