use std::io::Read;
use std::time::Duration;

use bytes::Bytes;
use kairo_serialization::{WireReader, WireWriter};

use crate::{RemoteAssociationAddress, RemoteError, RemoteStreamId, Result};

const TCP_HANDSHAKE_MAGIC: [u8; 4] = *b"KAH2";
const TCP_HANDSHAKE_VERSION: u8 = 2;
const TCP_HANDSHAKE_PREFIX_LEN: usize = TCP_HANDSHAKE_MAGIC.len() + 1 + 4;
pub const DEFAULT_TCP_HANDSHAKE_MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const DEFAULT_TCP_HANDSHAKE_READ_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpHandshakeReadSettings {
    max_payload_bytes: usize,
    read_timeout: Duration,
}

impl TcpHandshakeReadSettings {
    pub fn new(max_payload_bytes: usize, read_timeout: Duration) -> Result<Self> {
        if max_payload_bytes == 0 {
            return Err(RemoteError::InvalidTcpHandshakeSettings(
                "tcp handshake maximum payload bytes must be greater than zero".to_string(),
            ));
        }
        if read_timeout.is_zero() {
            return Err(RemoteError::InvalidTcpHandshakeSettings(
                "tcp handshake read timeout must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            max_payload_bytes,
            read_timeout,
        })
    }

    pub fn max_payload_bytes(self) -> usize {
        self.max_payload_bytes
    }

    pub fn read_timeout(self) -> Duration {
        self.read_timeout
    }
}

impl Default for TcpHandshakeReadSettings {
    fn default() -> Self {
        Self {
            max_payload_bytes: DEFAULT_TCP_HANDSHAKE_MAX_PAYLOAD_BYTES,
            read_timeout: DEFAULT_TCP_HANDSHAKE_READ_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TcpAssociationIdentity {
    address: RemoteAssociationAddress,
    uid: u64,
}

impl TcpAssociationIdentity {
    pub fn new(address: RemoteAssociationAddress, uid: u64) -> Self {
        Self { address, uid }
    }

    pub fn address(&self) -> &RemoteAssociationAddress {
        &self.address
    }

    pub fn uid(&self) -> u64 {
        self.uid
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpAssociationHandshake {
    stream_id: RemoteStreamId,
    from: TcpAssociationIdentity,
    to: RemoteAssociationAddress,
}

impl TcpAssociationHandshake {
    pub fn new(
        stream_id: RemoteStreamId,
        from: TcpAssociationIdentity,
        to: RemoteAssociationAddress,
    ) -> Self {
        Self {
            stream_id,
            from,
            to,
        }
    }

    pub fn stream_id(&self) -> RemoteStreamId {
        self.stream_id
    }

    pub fn from(&self) -> &TcpAssociationIdentity {
        &self.from
    }

    pub fn to(&self) -> &RemoteAssociationAddress {
        &self.to
    }
}

pub fn encode_tcp_association_handshake(handshake: &TcpAssociationHandshake) -> Result<Bytes> {
    let mut payload = WireWriter::new();
    payload.write_u8(handshake.stream_id.as_u8());
    write_identity(&mut payload, &handshake.from)?;
    write_address(&mut payload, &handshake.to)?;
    let payload = payload.finish();
    let len = u32::try_from(payload.len()).map_err(|_| {
        RemoteError::InvalidFrame("tcp association handshake exceeds u32 length".to_string())
    })?;

    let mut bytes = Vec::with_capacity(TCP_HANDSHAKE_PREFIX_LEN + payload.len());
    bytes.extend_from_slice(&TCP_HANDSHAKE_MAGIC);
    bytes.push(TCP_HANDSHAKE_VERSION);
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(&payload);
    Ok(Bytes::from(bytes))
}

pub fn read_tcp_association_handshake_with_limit(
    reader: &mut impl Read,
    max_payload_bytes: usize,
) -> Result<TcpAssociationHandshake> {
    let mut prefix = [0_u8; TCP_HANDSHAKE_PREFIX_LEN];
    reader
        .read_exact(&mut prefix)
        .map_err(|error| RemoteError::Inbound(format!("tcp handshake read failed: {error}")))?;
    if prefix[..TCP_HANDSHAKE_MAGIC.len()] != TCP_HANDSHAKE_MAGIC {
        return Err(RemoteError::InvalidFrame(
            "invalid tcp association handshake magic".to_string(),
        ));
    }
    if prefix[TCP_HANDSHAKE_MAGIC.len()] != TCP_HANDSHAKE_VERSION {
        return Err(RemoteError::InvalidFrame(format!(
            "unsupported tcp association handshake version {}",
            prefix[TCP_HANDSHAKE_MAGIC.len()]
        )));
    }
    let mut len = [0_u8; 4];
    len.copy_from_slice(&prefix[TCP_HANDSHAKE_MAGIC.len() + 1..]);
    let len = u32::from_be_bytes(len) as usize;
    if len > max_payload_bytes {
        return Err(RemoteError::InvalidFrame(format!(
            "tcp association handshake payload length {len} exceeds limit {max_payload_bytes}"
        )));
    }
    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload).map_err(|error| {
        RemoteError::Inbound(format!("tcp handshake payload read failed: {error}"))
    })?;
    decode_tcp_association_handshake_payload(Bytes::from(payload))
}

pub fn validate_tcp_association_handshakes(
    local_address: &RemoteAssociationAddress,
    expected_streams: usize,
    handshakes: &[TcpAssociationHandshake],
) -> Result<Option<TcpAssociationIdentity>> {
    if handshakes.is_empty() {
        return Ok(None);
    }
    if handshakes.len() != expected_streams {
        return Err(RemoteError::InvalidFrame(format!(
            "tcp association expected {expected_streams} handshakes but received {}",
            handshakes.len()
        )));
    }

    let remote = handshakes[0].from.clone();
    let mut seen = Vec::with_capacity(handshakes.len());
    for handshake in handshakes {
        if handshake.to() != local_address {
            return Err(RemoteError::InvalidFrame(format!(
                "tcp association handshake addressed to {}, expected {}",
                handshake.to(),
                local_address
            )));
        }
        if handshake.from() != &remote {
            return Err(RemoteError::InvalidFrame(format!(
                "tcp association mixed remote identities {}#{} and {}#{}",
                remote.address(),
                remote.uid(),
                handshake.from().address(),
                handshake.from().uid()
            )));
        }
        if seen.contains(&handshake.stream_id()) {
            return Err(RemoteError::InvalidFrame(format!(
                "tcp association duplicated {:?} lane handshake",
                handshake.stream_id()
            )));
        }
        seen.push(handshake.stream_id());
    }
    Ok(Some(remote))
}

fn decode_tcp_association_handshake_payload(bytes: Bytes) -> Result<TcpAssociationHandshake> {
    let mut reader = WireReader::new(&bytes);
    let stream_id = RemoteStreamId::try_from_u8(reader.read_u8()?)?;
    let from = read_identity(&mut reader)?;
    let to = read_address(&mut reader)?;
    reader.ensure_finished()?;
    Ok(TcpAssociationHandshake::new(stream_id, from, to))
}

fn write_identity(writer: &mut WireWriter, identity: &TcpAssociationIdentity) -> Result<()> {
    write_address(writer, identity.address())?;
    writer.write_u64(identity.uid());
    Ok(())
}

fn read_identity(reader: &mut WireReader<'_>) -> Result<TcpAssociationIdentity> {
    let address = read_address(reader)?;
    let uid = reader.read_u64()?;
    Ok(TcpAssociationIdentity::new(address, uid))
}

fn write_address(writer: &mut WireWriter, address: &RemoteAssociationAddress) -> Result<()> {
    writer.write_string(address.protocol())?;
    writer.write_string(address.system())?;
    writer.write_string(address.host())?;
    writer.write_optional_u64(address.port().map(u64::from));
    Ok(())
}

fn read_address(reader: &mut WireReader<'_>) -> Result<RemoteAssociationAddress> {
    let protocol = reader.read_string()?;
    let system = reader.read_string()?;
    let host = reader.read_string()?;
    let port = reader.read_optional_u64()?.map(|port| {
        u16::try_from(port).map_err(|_| {
            RemoteError::InvalidFrame(format!("tcp association handshake port {port} exceeds u16"))
        })
    });
    RemoteAssociationAddress::new(protocol, system, host, port.transpose()?)
}

#[cfg(test)]
mod tests {
    use bytes::Buf;

    use super::*;

    fn address(system: &str, port: u16) -> RemoteAssociationAddress {
        RemoteAssociationAddress::new("kairo", system, "127.0.0.1", Some(port)).unwrap()
    }

    #[test]
    fn tcp_handshake_round_trips_addresses_and_lane_id() {
        let handshake = TcpAssociationHandshake::new(
            RemoteStreamId::Ordinary,
            TcpAssociationIdentity::new(address("sender", 25521), 22),
            address("receiver", 25520),
        );

        let mut bytes = encode_tcp_association_handshake(&handshake)
            .unwrap()
            .reader();
        let decoded = read_tcp_association_handshake_with_limit(
            &mut bytes,
            DEFAULT_TCP_HANDSHAKE_MAX_PAYLOAD_BYTES,
        )
        .unwrap();

        assert_eq!(decoded, handshake);
    }

    #[test]
    fn tcp_handshake_rejects_trailing_payload_bytes() {
        let handshake = TcpAssociationHandshake::new(
            RemoteStreamId::Ordinary,
            TcpAssociationIdentity::new(address("sender", 25521), 22),
            address("receiver", 25520),
        );
        let encoded = encode_tcp_association_handshake(&handshake).unwrap();
        let mut payload = encoded[TCP_HANDSHAKE_PREFIX_LEN..].to_vec();
        payload.push(0xff);

        let error = decode_tcp_association_handshake_payload(Bytes::from(payload))
            .expect_err("trailing handshake payload byte should fail");

        assert!(error.to_string().contains("trailing byte"));
    }

    #[test]
    fn tcp_handshake_rejects_oversized_payload_before_reading_or_allocating_body() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&TCP_HANDSHAKE_MAGIC);
        bytes.push(TCP_HANDSHAKE_VERSION);
        bytes.extend_from_slice(&65_u32.to_be_bytes());

        let error = read_tcp_association_handshake_with_limit(&mut bytes.as_slice(), 64)
            .expect_err("oversized handshake prefix should fail without a payload body");

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(error.to_string().contains("exceeds limit 64"));
    }

    #[test]
    fn tcp_handshake_read_settings_reject_zero_resource_limits() {
        assert!(TcpHandshakeReadSettings::new(0, Duration::from_secs(1)).is_err());
        assert!(TcpHandshakeReadSettings::new(1, Duration::ZERO).is_err());
    }

    #[test]
    fn tcp_handshake_validation_rejects_wrong_local_target() {
        let handshake = TcpAssociationHandshake::new(
            RemoteStreamId::Control,
            TcpAssociationIdentity::new(address("sender", 25521), 22),
            address("other", 25520),
        );

        let error =
            validate_tcp_association_handshakes(&address("receiver", 25520), 1, &[handshake])
                .expect_err("wrong target should be rejected");

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(error.to_string().contains("addressed to"));
    }

    #[test]
    fn tcp_handshake_validation_rejects_duplicate_lanes() {
        let local = address("receiver", 25520);
        let remote = TcpAssociationIdentity::new(address("sender", 25521), 22);
        let handshakes = vec![
            TcpAssociationHandshake::new(RemoteStreamId::Ordinary, remote.clone(), local.clone()),
            TcpAssociationHandshake::new(RemoteStreamId::Ordinary, remote, local.clone()),
        ];

        let error = validate_tcp_association_handshakes(&local, 2, &handshakes)
            .expect_err("duplicate lane should be rejected");

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(error.to_string().contains("duplicated"));
    }

    #[test]
    fn tcp_handshake_validation_rejects_mixed_remote_uids() {
        let local = address("receiver", 25520);
        let remote = address("sender", 25521);
        let handshakes = vec![
            TcpAssociationHandshake::new(
                RemoteStreamId::Control,
                TcpAssociationIdentity::new(remote.clone(), 22),
                local.clone(),
            ),
            TcpAssociationHandshake::new(
                RemoteStreamId::Ordinary,
                TcpAssociationIdentity::new(remote, 23),
                local.clone(),
            ),
        ];

        let error = validate_tcp_association_handshakes(&local, 2, &handshakes)
            .expect_err("mixed remote uid should be rejected");

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(error.to_string().contains("mixed remote identities"));
    }
}
