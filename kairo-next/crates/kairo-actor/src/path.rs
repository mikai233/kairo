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

    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    pub fn system(&self) -> &str {
        &self.system
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}", self.protocol, self.system)?;
        if let Some(host) = &self.host {
            write!(f, "@{host}")?;
            if let Some(port) = self.port {
                write!(f, ":{port}")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PathSegment {
    name: String,
    uid: Option<u64>,
}

impl PathSegment {
    fn new(name: impl Into<String>, uid: Option<u64>) -> Self {
        Self {
            name: name.into(),
            uid,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorPath {
    value: String,
    address: Address,
    segments: Vec<PathSegment>,
}

impl ActorPath {
    pub fn is_valid_actor_name(name: &str) -> bool {
        is_valid_path_element(name, false)
    }

    pub fn new(value: impl Into<String>) -> Self {
        let value = value.into();
        let (address, segments) =
            parse_path(&value).unwrap_or_else(|| (Address::local("unknown"), Vec::new()));
        Self {
            value,
            address,
            segments,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn address(&self) -> &Address {
        &self.address
    }

    pub fn name(&self) -> Option<&str> {
        self.segments.last().map(|segment| segment.name.as_str())
    }

    pub fn uid(&self) -> Option<u64> {
        self.segments.last().and_then(|segment| segment.uid)
    }

    pub fn parent(&self) -> Option<Self> {
        if self.segments.is_empty() {
            return None;
        }
        Some(Self::from_parts(
            self.address.clone(),
            self.segments[..self.segments.len() - 1].to_vec(),
        ))
    }

    pub(crate) fn root(address: Address, name: impl Into<String>) -> Self {
        Self::from_parts(address, vec![PathSegment::new(name, None)])
    }

    pub(crate) fn is_valid_internal_name(name: &str) -> bool {
        is_valid_path_element(name, true)
    }

    pub(crate) fn child(&self, name: impl Into<String>, uid: Option<u64>) -> Self {
        let mut segments = self.segments.clone();
        segments.push(PathSegment::new(name, uid));
        Self::from_parts(self.address.clone(), segments)
    }

    fn from_parts(address: Address, segments: Vec<PathSegment>) -> Self {
        let mut value = address.to_string();
        for segment in &segments {
            value.push('/');
            value.push_str(&segment.name);
            if let Some(uid) = segment.uid {
                value.push('#');
                value.push_str(&uid.to_string());
            }
        }
        Self {
            value,
            address,
            segments,
        }
    }
}

impl Display for ActorPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}

fn parse_path(value: &str) -> Option<(Address, Vec<PathSegment>)> {
    let (protocol, rest) = value.split_once("://")?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (system, host, port) = parse_authority(authority);
    let address = Address {
        protocol: protocol.to_string(),
        system: system.to_string(),
        host,
        port,
    };
    let segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(parse_segment)
        .collect();
    Some((address, segments))
}

fn parse_authority(authority: &str) -> (&str, Option<String>, Option<u16>) {
    let Some((system, host_port)) = authority.split_once('@') else {
        return (authority, None, None);
    };
    let (host, port) = if let Some((host, port)) = host_port.rsplit_once(':') {
        (host, port.parse().ok())
    } else {
        (host_port, None)
    };
    (system, Some(host.to_string()), port)
}

fn parse_segment(segment: &str) -> PathSegment {
    let Some((name, uid)) = segment.rsplit_once('#') else {
        return PathSegment::new(segment, None);
    };
    let uid = uid.parse().ok();
    PathSegment::new(name, uid)
}

fn is_valid_path_element(name: &str, allow_reserved: bool) -> bool {
    if name.is_empty() {
        return false;
    }

    let bytes = name.as_bytes();
    if bytes[0] == b'$' && !allow_reserved {
        return false;
    }

    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if is_valid_path_byte(byte) {
            index += 1;
        } else if byte == b'%'
            && index + 2 < bytes.len()
            && bytes[index + 1].is_ascii_hexdigit()
            && bytes[index + 2].is_ascii_hexdigit()
        {
            index += 3;
        } else {
            return false;
        }
    }
    true
}

fn is_valid_path_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"-_.*$+:@&=,!~';".contains(&byte)
}
