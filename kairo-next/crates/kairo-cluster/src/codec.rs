use bytes::Bytes;
use kairo_actor::Address;
use kairo_serialization::{
    MessageCodec, Registry, SerializationError, SerializationRegistry, WireReader, WireWriter,
};

use crate::{
    Gossip, GossipEnvelope, Heartbeat, HeartbeatRsp, Join, Member, MemberStatus, Reachability,
    ReachabilityRecord, ReachabilityStatus, UniqueAddress, VectorClock, VectorClockNode, Welcome,
};

pub const HEARTBEAT_SERIALIZER_ID: u32 = 2_000;
pub const HEARTBEAT_RSP_SERIALIZER_ID: u32 = 2_001;
pub const JOIN_SERIALIZER_ID: u32 = 2_002;
pub const WELCOME_SERIALIZER_ID: u32 = 2_003;
pub const GOSSIP_ENVELOPE_SERIALIZER_ID: u32 = 2_004;

pub fn register_cluster_control_codecs(registry: &mut Registry) -> kairo_serialization::Result<()> {
    registry.register::<Heartbeat, _>(HeartbeatCodec)?;
    registry.register::<HeartbeatRsp, _>(HeartbeatRspCodec)?;
    registry.register::<Join, _>(JoinCodec)?;
    Ok(())
}

pub fn register_cluster_protocol_codecs(
    registry: &mut Registry,
) -> kairo_serialization::Result<()> {
    register_cluster_control_codecs(registry)?;
    registry.register::<Welcome, _>(WelcomeCodec)?;
    registry.register::<GossipEnvelope, _>(GossipEnvelopeCodec)?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct HeartbeatCodec;

impl MessageCodec<Heartbeat> for HeartbeatCodec {
    fn serializer_id(&self) -> u32 {
        HEARTBEAT_SERIALIZER_ID
    }

    fn encode(&self, message: &Heartbeat) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        writer.write_u64(message.sequence_nr);
        writer.write_u64(message.creation_time_nanos);
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Heartbeat> {
        ensure_version::<Heartbeat>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(Heartbeat {
            from: read_unique_address(&mut reader)?,
            sequence_nr: reader.read_u64()?,
            creation_time_nanos: reader.read_u64()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HeartbeatRspCodec;

impl MessageCodec<HeartbeatRsp> for HeartbeatRspCodec {
    fn serializer_id(&self) -> u32 {
        HEARTBEAT_RSP_SERIALIZER_ID
    }

    fn encode(&self, message: &HeartbeatRsp) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        writer.write_u64(message.sequence_nr);
        writer.write_u64(message.creation_time_nanos);
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<HeartbeatRsp> {
        ensure_version::<HeartbeatRsp>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(HeartbeatRsp {
            from: read_unique_address(&mut reader)?,
            sequence_nr: reader.read_u64()?,
            creation_time_nanos: reader.read_u64()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct JoinCodec;

impl MessageCodec<Join> for JoinCodec {
    fn serializer_id(&self) -> u32 {
        JOIN_SERIALIZER_ID
    }

    fn encode(&self, message: &Join) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.node)?;
        let role_count = u64::try_from(message.roles.len())
            .map_err(|_| SerializationError::Message("too many cluster roles".to_string()))?;
        writer.write_u64(role_count);
        for role in &message.roles {
            writer.write_string(role)?;
        }
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Join> {
        ensure_version::<Join>(version)?;
        let mut reader = WireReader::new(&payload);
        let node = read_unique_address(&mut reader)?;
        let role_count = usize::try_from(reader.read_u64()?).map_err(|_| {
            SerializationError::Message("cluster role count is too large".to_string())
        })?;
        let mut roles = Vec::with_capacity(role_count);
        for _ in 0..role_count {
            roles.push(reader.read_string()?);
        }
        Ok(Join { node, roles })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WelcomeCodec;

impl MessageCodec<Welcome> for WelcomeCodec {
    fn serializer_id(&self) -> u32 {
        WELCOME_SERIALIZER_ID
    }

    fn encode(&self, message: &Welcome) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        write_gossip(&mut writer, &message.gossip)?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Welcome> {
        ensure_version::<Welcome>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(Welcome {
            from: read_unique_address(&mut reader)?,
            gossip: read_gossip(&mut reader)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GossipEnvelopeCodec;

impl MessageCodec<GossipEnvelope> for GossipEnvelopeCodec {
    fn serializer_id(&self) -> u32 {
        GOSSIP_ENVELOPE_SERIALIZER_ID
    }

    fn encode(&self, message: &GossipEnvelope) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        write_unique_address(&mut writer, &message.to)?;
        writer.write_u64(message.sequence_nr);
        write_gossip(&mut writer, &message.gossip)?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<GossipEnvelope> {
        ensure_version::<GossipEnvelope>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(GossipEnvelope {
            from: read_unique_address(&mut reader)?,
            to: read_unique_address(&mut reader)?,
            sequence_nr: reader.read_u64()?,
            gossip: read_gossip(&mut reader)?,
        })
    }
}

fn write_gossip(writer: &mut WireWriter, gossip: &Gossip) -> kairo_serialization::Result<()> {
    write_count(writer, gossip.members().len())?;
    for member in gossip.members() {
        write_member(writer, member)?;
    }

    let mut seen: Vec<_> = gossip.seen_by().iter().collect();
    seen.sort_by_key(|node| node.ordering_key());
    write_count(writer, seen.len())?;
    for node in seen {
        write_unique_address(writer, node)?;
    }

    write_reachability(writer, gossip.reachability())?;
    write_vector_clock(writer, gossip.version())?;

    let mut tombstones: Vec<_> = gossip.tombstones().iter().collect();
    tombstones.sort_by_key(|(node, _)| node.ordering_key());
    write_count(writer, tombstones.len())?;
    for (node, timestamp) in tombstones {
        write_unique_address(writer, node)?;
        writer.write_u64(*timestamp);
    }
    Ok(())
}

fn read_gossip(reader: &mut WireReader<'_>) -> kairo_serialization::Result<Gossip> {
    let members = read_vec(reader, read_member)?;
    let seen = read_vec(reader, read_unique_address)?;
    let reachability = read_reachability(reader)?;
    let version = read_vector_clock(reader)?;
    let tombstones = read_vec(reader, |reader| {
        let node = read_unique_address(reader)?;
        let timestamp = reader.read_u64()?;
        Ok((node, timestamp))
    })?;

    Ok(Gossip::from_parts(
        members,
        seen,
        reachability,
        version,
        tombstones,
    ))
}

fn write_member(writer: &mut WireWriter, member: &Member) -> kairo_serialization::Result<()> {
    write_unique_address(writer, &member.unique_address)?;
    writer.write_u64(member_status_code(member.status));
    write_count(writer, member.roles.len())?;
    for role in &member.roles {
        writer.write_string(role)?;
    }
    writer.write_optional_u64(member.up_number);
    Ok(())
}

fn read_member(reader: &mut WireReader<'_>) -> kairo_serialization::Result<Member> {
    let unique_address = read_unique_address(reader)?;
    let status = member_status_from_code(reader.read_u64()?)?;
    let roles = read_vec(reader, |reader| reader.read_string())?;
    let up_number = reader.read_optional_u64()?;
    let mut member = Member::new(unique_address, roles).with_status(status);
    if let Some(up_number) = up_number {
        member = member.with_up_number(up_number);
    }
    Ok(member)
}

fn write_reachability(
    writer: &mut WireWriter,
    reachability: &Reachability,
) -> kairo_serialization::Result<()> {
    let mut versions: Vec<_> = reachability.versions().iter().collect();
    versions.sort_by_key(|(observer, _)| observer.ordering_key());
    write_count(writer, versions.len())?;
    for (observer, version) in versions {
        write_unique_address(writer, observer)?;
        writer.write_u64(*version);
    }

    let mut records: Vec<_> = reachability.records().iter().collect();
    records.sort_by_key(|record| {
        (
            record.observer.ordering_key(),
            record.subject.ordering_key(),
            record.version,
        )
    });
    write_count(writer, records.len())?;
    for record in records {
        write_unique_address(writer, &record.observer)?;
        write_unique_address(writer, &record.subject)?;
        writer.write_u64(reachability_status_code(record.status));
        writer.write_u64(record.version);
    }
    Ok(())
}

fn read_reachability(reader: &mut WireReader<'_>) -> kairo_serialization::Result<Reachability> {
    let versions = read_vec(reader, |reader| {
        let observer = read_unique_address(reader)?;
        let version = reader.read_u64()?;
        Ok((observer, version))
    })?;
    let records = read_vec(reader, |reader| {
        Ok(ReachabilityRecord {
            observer: read_unique_address(reader)?,
            subject: read_unique_address(reader)?,
            status: reachability_status_from_code(reader.read_u64()?)?,
            version: reader.read_u64()?,
        })
    })?;
    Ok(Reachability::from_parts(records, versions))
}

fn write_vector_clock(
    writer: &mut WireWriter,
    clock: &VectorClock,
) -> kairo_serialization::Result<()> {
    let entries: Vec<_> = clock.entries().collect();
    write_count(writer, entries.len())?;
    for (node, version) in entries {
        writer.write_string(node.as_str())?;
        writer.write_u64(version);
    }
    Ok(())
}

fn read_vector_clock(reader: &mut WireReader<'_>) -> kairo_serialization::Result<VectorClock> {
    let entries = read_vec(reader, |reader| {
        let node = VectorClockNode::new(reader.read_string()?);
        let version = reader.read_u64()?;
        Ok((node, version))
    })?;
    Ok(VectorClock::from_entries(entries))
}

fn write_unique_address(
    writer: &mut WireWriter,
    unique_address: &UniqueAddress,
) -> kairo_serialization::Result<()> {
    writer.write_string(unique_address.address.protocol())?;
    writer.write_string(unique_address.address.system())?;
    writer.write_optional_string(unique_address.address.host())?;
    writer.write_optional_u64(unique_address.address.port().map(u64::from));
    writer.write_u64(unique_address.uid);
    Ok(())
}

fn read_unique_address(reader: &mut WireReader<'_>) -> kairo_serialization::Result<UniqueAddress> {
    let protocol = reader.read_string()?;
    let system = reader.read_string()?;
    let host = reader.read_optional_string()?;
    let port = reader
        .read_optional_u64()?
        .map(u16::try_from)
        .transpose()
        .map_err(|_| SerializationError::Message("cluster address port exceeds u16".to_string()))?;
    let uid = reader.read_u64()?;
    Ok(UniqueAddress::new(
        Address::new(protocol, system, host, port),
        uid,
    ))
}

fn write_count(writer: &mut WireWriter, len: usize) -> kairo_serialization::Result<()> {
    writer.write_u64(
        u64::try_from(len).map_err(|_| {
            SerializationError::Message("collection length exceeds u64".to_string())
        })?,
    );
    Ok(())
}

fn read_count(reader: &mut WireReader<'_>) -> kairo_serialization::Result<usize> {
    usize::try_from(reader.read_u64()?)
        .map_err(|_| SerializationError::Message("collection length exceeds usize".to_string()))
}

fn read_vec<T, F>(
    reader: &mut WireReader<'_>,
    mut read_one: F,
) -> kairo_serialization::Result<Vec<T>>
where
    F: FnMut(&mut WireReader<'_>) -> kairo_serialization::Result<T>,
{
    let count = read_count(reader)?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(read_one(reader)?);
    }
    Ok(values)
}

fn member_status_code(status: MemberStatus) -> u64 {
    match status {
        MemberStatus::Joining => 0,
        MemberStatus::WeaklyUp => 1,
        MemberStatus::Up => 2,
        MemberStatus::Leaving => 3,
        MemberStatus::Exiting => 4,
        MemberStatus::Down => 5,
        MemberStatus::Removed => 6,
    }
}

fn member_status_from_code(code: u64) -> kairo_serialization::Result<MemberStatus> {
    match code {
        0 => Ok(MemberStatus::Joining),
        1 => Ok(MemberStatus::WeaklyUp),
        2 => Ok(MemberStatus::Up),
        3 => Ok(MemberStatus::Leaving),
        4 => Ok(MemberStatus::Exiting),
        5 => Ok(MemberStatus::Down),
        6 => Ok(MemberStatus::Removed),
        other => Err(SerializationError::Message(format!(
            "unknown member status code {other}"
        ))),
    }
}

fn reachability_status_code(status: ReachabilityStatus) -> u64 {
    match status {
        ReachabilityStatus::Reachable => 0,
        ReachabilityStatus::Unreachable => 1,
        ReachabilityStatus::Terminated => 2,
    }
}

fn reachability_status_from_code(code: u64) -> kairo_serialization::Result<ReachabilityStatus> {
    match code {
        0 => Ok(ReachabilityStatus::Reachable),
        1 => Ok(ReachabilityStatus::Unreachable),
        2 => Ok(ReachabilityStatus::Terminated),
        other => Err(SerializationError::Message(format!(
            "unknown reachability status code {other}"
        ))),
    }
}

fn ensure_version<M>(version: u16) -> kairo_serialization::Result<()>
where
    M: kairo_serialization::RemoteMessage,
{
    if version == M::VERSION {
        Ok(())
    } else {
        Err(SerializationError::Message(format!(
            "unsupported {} version {version}",
            M::MANIFEST
        )))
    }
}

#[cfg(test)]
mod tests {
    use kairo_serialization::{Manifest, RemoteMessage, SerializedMessage};

    use super::*;

    fn registry() -> Registry {
        let mut registry = Registry::new();
        register_cluster_protocol_codecs(&mut registry).unwrap();
        registry
    }

    fn unique(uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new("kairo", "sys", Some("127.0.0.1".to_string()), Some(25520)),
            uid,
        )
    }

    fn rich_gossip() -> Gossip {
        let node_a = unique(1);
        let node_b = unique(2);
        let node_c = unique(3);
        let members = vec![
            Member::new(node_a.clone(), vec!["backend".to_string()])
                .with_status(MemberStatus::Up)
                .with_up_number(1),
            Member::new(node_b.clone(), vec!["frontend".to_string()])
                .with_status(MemberStatus::Leaving)
                .with_up_number(2),
        ];
        let reachability = Reachability::new()
            .unreachable(node_a.clone(), node_b.clone())
            .terminated(node_b.clone(), node_c.clone());
        let version = VectorClock::new()
            .increment(VectorClockNode::new("node-a"))
            .increment(VectorClockNode::new("node-b"))
            .increment(VectorClockNode::new("node-b"));

        Gossip::from_parts(
            members,
            vec![node_a.clone(), node_b.clone()],
            reachability,
            version,
            vec![(node_c, 99)],
        )
    }

    #[test]
    fn cluster_control_codecs_round_trip_heartbeat_messages() {
        let registry = registry();
        let heartbeat = Heartbeat {
            from: unique(7),
            sequence_nr: 42,
            creation_time_nanos: 1234,
        };
        let response = HeartbeatRsp {
            from: unique(8),
            sequence_nr: 42,
            creation_time_nanos: 1234,
        };

        let serialized_heartbeat = registry.serialize(&heartbeat).unwrap();
        let serialized_response = registry.serialize(&response).unwrap();

        assert_eq!(serialized_heartbeat.serializer_id, HEARTBEAT_SERIALIZER_ID);
        assert_eq!(
            serialized_response.serializer_id,
            HEARTBEAT_RSP_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<Heartbeat>(serialized_heartbeat)
                .unwrap(),
            heartbeat
        );
        assert_eq!(
            registry
                .deserialize::<HeartbeatRsp>(serialized_response)
                .unwrap(),
            response
        );
    }

    #[test]
    fn cluster_control_codecs_round_trip_join() {
        let registry = registry();
        let join = Join {
            node: unique(9),
            roles: vec!["backend".to_string(), "blue".to_string()],
        };

        let serialized = registry.serialize(&join).unwrap();

        assert_eq!(serialized.serializer_id, JOIN_SERIALIZER_ID);
        assert_eq!(serialized.manifest.as_str(), Join::MANIFEST);
        assert_eq!(registry.deserialize::<Join>(serialized).unwrap(), join);
    }

    #[test]
    fn cluster_control_codecs_reject_unknown_versions() {
        let registry = registry();
        let wire = SerializedMessage::new(
            JOIN_SERIALIZER_ID,
            Manifest::new(Join::MANIFEST),
            Join::VERSION + 1,
            registry
                .serialize(&Join {
                    node: unique(1),
                    roles: vec![],
                })
                .unwrap()
                .payload,
        );

        let error = registry
            .deserialize::<Join>(wire)
            .expect_err("unknown version should fail");

        assert!(error.to_string().contains("unsupported"));
    }

    #[test]
    fn cluster_protocol_codecs_round_trip_welcome_with_gossip() {
        let registry = registry();
        let welcome = Welcome {
            from: unique(1),
            gossip: rich_gossip(),
        };

        let serialized = registry.serialize(&welcome).unwrap();

        assert_eq!(serialized.serializer_id, WELCOME_SERIALIZER_ID);
        assert_eq!(serialized.manifest.as_str(), Welcome::MANIFEST);
        assert_eq!(
            registry.deserialize::<Welcome>(serialized).unwrap(),
            welcome
        );
    }

    #[test]
    fn cluster_protocol_codecs_round_trip_gossip_envelope() {
        let registry = registry();
        let envelope = GossipEnvelope {
            from: unique(1),
            to: unique(2),
            sequence_nr: 77,
            gossip: rich_gossip(),
        };

        let serialized = registry.serialize(&envelope).unwrap();

        assert_eq!(serialized.serializer_id, GOSSIP_ENVELOPE_SERIALIZER_ID);
        assert_eq!(serialized.manifest.as_str(), GossipEnvelope::MANIFEST);
        assert_eq!(
            registry.deserialize::<GossipEnvelope>(serialized).unwrap(),
            envelope
        );
    }
}
