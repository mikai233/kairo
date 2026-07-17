use kairo_actor::Address;
use kairo_serialization::{RemoteMessage, SerializationError, WireReader, WireWriter};

use crate::{
    ApplicationVersion, Gossip, Member, MemberStatus, Reachability, ReachabilityRecord,
    ReachabilityStatus, UniqueAddress, VectorClock, VectorClockNode,
};

pub(super) fn write_gossip(
    writer: &mut WireWriter,
    gossip: &Gossip,
    version: u16,
) -> kairo_serialization::Result<()> {
    write_count(writer, gossip.members().len())?;
    for member in gossip.members() {
        write_member(writer, member, version)?;
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

pub(super) fn read_gossip(
    reader: &mut WireReader<'_>,
    version: u16,
) -> kairo_serialization::Result<Gossip> {
    let members = read_vec(reader, |reader| read_member(reader, version))?;
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

fn write_member(
    writer: &mut WireWriter,
    member: &Member,
    version: u16,
) -> kairo_serialization::Result<()> {
    write_unique_address(writer, &member.unique_address)?;
    writer.write_u64(member_status_code(member.status));
    write_count(writer, member.roles.len())?;
    for role in &member.roles {
        writer.write_string(role)?;
    }
    writer.write_optional_u64(member.up_number);
    if version >= 2 {
        writer.write_string(member.app_version.as_str())?;
    }
    Ok(())
}

fn read_member(reader: &mut WireReader<'_>, version: u16) -> kairo_serialization::Result<Member> {
    let unique_address = read_unique_address(reader)?;
    let status = member_status_from_code(reader.read_u64()?)?;
    let roles = read_vec(reader, |reader| reader.read_string())?;
    let up_number = reader.read_optional_u64()?;
    let app_version = if version >= 2 {
        ApplicationVersion::new(reader.read_string()?).map_err(|error| {
            SerializationError::Message(format!("invalid member application version: {error}"))
        })?
    } else {
        ApplicationVersion::default()
    };
    let mut member = Member::new(unique_address, roles)
        .with_status(status)
        .with_app_version(app_version);
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

pub(super) fn write_vector_clock(
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

pub(super) fn read_vector_clock(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<VectorClock> {
    let entries = read_vec(reader, |reader| {
        let node = VectorClockNode::new(reader.read_string()?);
        let version = reader.read_u64()?;
        Ok((node, version))
    })?;
    Ok(VectorClock::from_entries(entries))
}

pub(super) fn write_unique_address(
    writer: &mut WireWriter,
    unique_address: &UniqueAddress,
) -> kairo_serialization::Result<()> {
    write_address(writer, &unique_address.address)?;
    writer.write_u64(unique_address.uid);
    Ok(())
}

pub(super) fn read_unique_address(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<UniqueAddress> {
    let address = read_address(reader)?;
    let uid = reader.read_u64()?;
    Ok(UniqueAddress::new(address, uid))
}

pub(super) fn write_address(
    writer: &mut WireWriter,
    address: &Address,
) -> kairo_serialization::Result<()> {
    writer.write_string(address.protocol())?;
    writer.write_string(address.system())?;
    writer.write_optional_string(address.host())?;
    writer.write_optional_u64(address.port().map(u64::from));
    Ok(())
}

pub(super) fn read_address(reader: &mut WireReader<'_>) -> kairo_serialization::Result<Address> {
    let protocol = reader.read_string()?;
    let system = reader.read_string()?;
    let host = reader.read_optional_string()?;
    let port = reader
        .read_optional_u64()?
        .map(u16::try_from)
        .transpose()
        .map_err(|_| SerializationError::Message("cluster address port exceeds u16".to_string()))?;
    Ok(Address::new(protocol, system, host, port))
}

pub(super) fn write_count(writer: &mut WireWriter, len: usize) -> kairo_serialization::Result<()> {
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

pub(super) fn read_vec<T, F>(
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

pub(super) fn ensure_version<M>(version: u16) -> kairo_serialization::Result<()>
where
    M: RemoteMessage,
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

pub(super) fn ensure_supported_version<M>(
    version: u16,
    oldest: u16,
) -> kairo_serialization::Result<()>
where
    M: RemoteMessage,
{
    if (oldest..=M::VERSION).contains(&version) {
        Ok(())
    } else {
        Err(SerializationError::Message(format!(
            "unsupported {} version {version}",
            M::MANIFEST
        )))
    }
}
