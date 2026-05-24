use crate::{Result, SerializationError};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorRefWireData {
    path: String,
    protocol: String,
    system: String,
    host: Option<String>,
    port: Option<u16>,
}

impl ActorRefWireData {
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
        if protocol.is_empty() || system.is_empty() || path.is_empty() {
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

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    pub fn system(&self) -> &str {
        &self.system
    }

    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    pub fn port(&self) -> Option<u16> {
        self.port
    }
}

pub trait ActorRefResolver {
    type Ref;

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
        (host_port.to_string(), None)
    };
    Some((system.to_string(), Some(host), port))
}
