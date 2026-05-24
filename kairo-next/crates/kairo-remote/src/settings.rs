#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSettings {
    pub canonical_hostname: String,
    pub canonical_port: u16,
}

impl RemoteSettings {
    pub fn new(canonical_hostname: impl Into<String>, canonical_port: u16) -> Self {
        Self {
            canonical_hostname: canonical_hostname.into(),
            canonical_port,
        }
    }
}
