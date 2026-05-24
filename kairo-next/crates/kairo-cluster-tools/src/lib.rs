//! Higher-level cluster utilities built on top of `kairo-cluster`.

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicName(String);

impl TopicName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
