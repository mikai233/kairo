use crate::{Result, SerializationError};

/// Stable actor-reference data carried in serialized remote envelopes.
///
/// The full path string remains the canonical value, while parsed protocol,
/// system, host, and port fields let remote and cluster code inspect address
/// parts without reparsing the path at every boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorRefWireData {
    path: String,
    protocol: String,
    system: String,
    host: Option<String>,
    port: Option<u16>,
}

impl ActorRefWireData {
    /// Parses a canonical actor-ref path into wire data.
    ///
    /// Addressed paths must include both host and port. Local paths still use
    /// the same `protocol://system/...` shape but omit the `@host:port` part.
    pub fn new(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        let (protocol, system, host, port) = parse_actor_ref_path(&path)?;
        Ok(Self {
            path,
            protocol,
            system,
            host,
            port,
        })
    }

    /// Builds wire data from already separated path and address parts.
    ///
    /// `host` and `port` must either both be present or both be absent, and
    /// the separated address parts must match the canonical actor-ref path.
    pub fn from_parts(
        protocol: impl Into<String>,
        system: impl Into<String>,
        host: Option<String>,
        port: Option<u16>,
        path: impl Into<String>,
    ) -> Result<Self> {
        let protocol = protocol.into();
        let system = system.into();
        let path = path.into();
        if host.is_some() != port.is_some() {
            return Err(SerializationError::InvalidActorRefPath(path));
        }
        let parsed = parse_actor_ref_path(&path)?;
        if parsed != (protocol.clone(), system.clone(), host.clone(), port) {
            return Err(SerializationError::InvalidActorRefPath(path));
        }
        Ok(Self {
            path,
            protocol,
            system,
            host,
            port,
        })
    }

    /// Returns the canonical actor-ref path string.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the actor path protocol, such as `kairo`.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// Returns the actor-system name from the path authority.
    pub fn system(&self) -> &str {
        &self.system
    }

    /// Returns the remote host when the path is addressed.
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    /// Returns the remote port when the path is addressed.
    pub fn port(&self) -> Option<u16> {
        self.port
    }
}

/// Runtime bridge between typed actor refs and stable wire data.
///
/// Actor crates and remote providers implement this trait so serialization
/// stays independent from the concrete actor-reference type.
pub trait ActorRefResolver {
    /// Concrete actor-reference type used by the owner.
    type Ref;

    /// Converts a concrete actor ref into stable wire data.
    fn actor_ref_to_wire_data(&self, actor_ref: &Self::Ref) -> Result<ActorRefWireData>;
    /// Resolves stable wire data back into a concrete actor ref.
    fn resolve_actor_ref(&self, wire: &ActorRefWireData) -> Result<Self::Ref>;
}

fn parse_actor_ref_path(path: &str) -> Result<(String, String, Option<String>, Option<u16>)> {
    let Some((protocol, rest)) = path.split_once("://") else {
        return Err(SerializationError::InvalidActorRefPath(path.to_string()));
    };
    let Some((authority, actor_path)) = rest.split_once('/') else {
        return Err(SerializationError::InvalidActorRefPath(path.to_string()));
    };
    if protocol.is_empty() || authority.is_empty() || actor_path.is_empty() {
        return Err(SerializationError::InvalidActorRefPath(path.to_string()));
    }

    let (system, host, port) = parse_authority(authority)
        .ok_or_else(|| SerializationError::InvalidActorRefPath(path.to_string()))?;
    Ok((protocol.to_string(), system, host, port))
}

fn parse_authority(authority: &str) -> Option<(String, Option<String>, Option<u16>)> {
    let Some((system, host_port)) = authority.split_once('@') else {
        return Some((authority.to_string(), None, None));
    };
    if system.is_empty() || host_port.is_empty() {
        return None;
    }
    let (host, port) = if let Some((host, port)) = host_port.rsplit_once(':') {
        if host.is_empty() {
            return None;
        }
        (host.to_string(), Some(port.parse().ok()?))
    } else {
        return None;
    };
    Some((system.to_string(), Some(host), port))
}
