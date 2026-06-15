use std::time::Duration;

/// Runtime settings for remote actor addressing and TCP dialing.
///
/// The canonical host and port are the address advertised in actor-ref wire
/// data. The optional connect timeout is used by socket-backed association
/// dialers while preserving a small runtime default when not configured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSettings {
    /// Canonical host name or IP address advertised for this actor system.
    pub canonical_hostname: String,
    /// Canonical TCP port advertised for this actor system.
    pub canonical_port: u16,
    /// Optional TCP connect timeout used by remote dialers.
    pub connect_timeout: Option<Duration>,
}

impl RemoteSettings {
    /// Creates remote settings with the default TCP connect timeout.
    pub fn new(canonical_hostname: impl Into<String>, canonical_port: u16) -> Self {
        Self {
            canonical_hostname: canonical_hostname.into(),
            canonical_port,
            connect_timeout: None,
        }
    }

    /// Sets an explicit TCP connect timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Returns the configured connect timeout or the remote runtime default.
    pub fn connect_timeout_or_default(&self) -> Duration {
        self.connect_timeout.unwrap_or(Duration::from_secs(1))
    }
}
