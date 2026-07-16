#![deny(missing_docs)]

/// Ordered, owned identity of a pubsub topic.
///
/// Topic names are application-defined strings and are not interpreted as
/// actor paths. Empty names are retained rather than implicitly normalized.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicName(String);

impl TopicName {
    /// Creates a topic name from its exact string representation.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the exact topic-name string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
