use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSettings {
    pub canonical_hostname: String,
    pub canonical_port: u16,
    pub connect_timeout: Option<Duration>,
}

impl RemoteSettings {
    pub fn new(canonical_hostname: impl Into<String>, canonical_port: u16) -> Self {
        Self {
            canonical_hostname: canonical_hostname.into(),
            canonical_port,
            connect_timeout: None,
        }
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    pub fn connect_timeout_or_default(&self) -> Duration {
        self.connect_timeout.unwrap_or(Duration::from_secs(1))
    }
}
