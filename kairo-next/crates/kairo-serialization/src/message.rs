/// Stable serializer identifier carried on the wire.
///
/// Serializer ids are allocated by codecs, not inferred from Rust type
/// information. A remote payload is resolved by the pair of serializer id and
/// [`RemoteMessage::MANIFEST`].
pub type SerializerId = u32;

/// Stable metadata required for messages that can cross remote boundaries.
///
/// Local-only actor messages do not need to implement this trait. Implement it
/// only when a message is serialized for remoting or another compatibility
/// sensitive system protocol. The manifest and version are explicit wire
/// contracts and must not be derived from Rust type names, enum discriminants,
/// or memory layout.
pub trait RemoteMessage: Send + 'static {
    /// Stable, non-empty wire manifest for this message type.
    const MANIFEST: &'static str;
    /// Current wire schema version emitted for this message type.
    ///
    /// Codecs receive the version during decode so they can accept older wire
    /// forms during rolling upgrades.
    const VERSION: u16;
}
