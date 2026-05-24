//! Stable serialization contracts for remote and persistent messages.

mod actor_ref;
mod codec;
mod envelope;
mod errors;
mod manifest;
mod message;
mod registry;
mod wire;

pub use actor_ref::{ActorRefResolver, ActorRefWireData};
pub use codec::{DynCodec, MessageCodec};
pub use envelope::{RemoteEnvelope, SerializedMessage};
pub use errors::{Result, SerializationError};
pub use manifest::Manifest;
pub use message::{RemoteMessage, SerializerId};
pub use registry::{Registry, SerializationRegistry};
pub use wire::{WireReader, WireWriter};

#[cfg(test)]
mod tests;
