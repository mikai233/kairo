use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Address {
    protocol: String,
    system: String,
    host: Option<String>,
    port: Option<u16>,
}

impl Address {
    pub fn local(system: impl Into<String>) -> Self {
        Self {
            protocol: "kairo".to_string(),
            system: system.into(),
            host: None,
            port: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorPath {
    value: String,
}

impl ActorPath {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl Display for ActorPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}
