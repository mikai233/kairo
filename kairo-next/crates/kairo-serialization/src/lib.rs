#![deny(missing_docs)]

//! Stable serialization contracts for messages that cross remote boundaries.
//!
//! Local actor messages do not need this crate. A local `ActorRef<M>` only
//! requires `M: Send + 'static`. Serialization becomes part of the contract
//! when a message is sent through remoting, persisted by a system protocol, or
//! otherwise written to a compatibility-sensitive wire format.
//!
//! Remote-capable messages declare stable [`RemoteMessage`] metadata, and a
//! caller registers an explicit [`MessageCodec`] for each message type. The
//! wire contract is the tuple of serializer id, manifest, version, and bytes;
//! it must not depend on Rust type names, enum discriminants, or memory layout.
//! During a rolling upgrade, keep the serializer id and manifest stable, bump
//! [`RemoteMessage::VERSION`] for the new schema, and make
//! [`MessageCodec::decode`] handle every wire version that may coexist. Forward
//! compatibility is also codec-owned: an older codec must explicitly accept a
//! newer version when mixed-version traffic in that direction is required.
//! The remoting transport preserves this metadata but does not negotiate a
//! schema or silently downgrade payloads.
//!
//! ```
//! use bytes::Bytes;
//! use kairo_serialization::{
//!     Registry, RemoteMessage, SerializationError,
//! };
//!
//! #[derive(Debug, PartialEq, Eq)]
//! struct Greeting(String);
//!
//! impl RemoteMessage for Greeting {
//!     const MANIFEST: &'static str = "kairo.example.Greeting";
//!     const VERSION: u16 = 1;
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut registry = Registry::new();
//! registry.register_with::<Greeting, _, _>(
//!     1001,
//!     |message| Ok(Bytes::from(message.0.clone())),
//!     |payload, version| {
//!         if version != Greeting::VERSION {
//!             return Err(SerializationError::Message(format!(
//!                 "unsupported Greeting version {version}"
//!             )));
//!         }
//!
//!         String::from_utf8(payload.to_vec())
//!             .map(Greeting)
//!             .map_err(|error| SerializationError::Message(error.to_string()))
//!     },
//! )?;
//!
//! let serialized = registry.serialize(&Greeting("hello".to_string()))?;
//! assert_eq!(serialized.serializer_id, 1001);
//! assert_eq!(serialized.manifest.as_str(), Greeting::MANIFEST);
//! assert_eq!(serialized.version, Greeting::VERSION);
//!
//! let decoded: Greeting = registry.deserialize(serialized)?;
//! assert_eq!(decoded, Greeting("hello".to_string()));
//! # Ok(())
//! # }
//! ```

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
