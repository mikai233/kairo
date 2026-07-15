use kairo_actor::Address;
use kairo_remote::{
    RemoteAssociationCache, RemoteError, Result as RemoteResult, TcpRemoteActorRuntimeBuilder,
};
use kairo_serialization::RemoteMessage;

use crate::{
    ClusterSystemInbound, GossipEnvelope, Heartbeat, HeartbeatRsp, Join, UniqueAddress, Welcome,
};

pub const CLUSTER_SYSTEM_MANIFESTS: [&str; 5] = [
    Join::MANIFEST,
    Welcome::MANIFEST,
    GossipEnvelope::MANIFEST,
    Heartbeat::MANIFEST,
    HeartbeatRsp::MANIFEST,
];

/// Registers cluster control traffic with an ActorSystem-owned remote runtime.
///
/// The factory runs after TCP bind so it receives the effective canonical
/// node address and the runtime's shared association cache.
pub fn register_cluster_system_inbound<F>(
    builder: &mut TcpRemoteActorRuntimeBuilder,
    node_uid: u64,
    factory: F,
) -> RemoteResult<()>
where
    F: FnOnce(UniqueAddress, RemoteAssociationCache) -> ClusterSystemInbound + Send + 'static,
{
    builder.register_control_handler(&CLUSTER_SYSTEM_MANIFESTS, move |context| {
        let settings = context.settings();
        let self_node = UniqueAddress::new(
            Address::new(
                context.system().address().protocol(),
                context.system().name(),
                Some(settings.canonical_hostname.clone()),
                Some(settings.canonical_port),
            ),
            node_uid,
        );
        let inbound = factory(self_node, context.association_cache().clone());
        Ok(move |envelope| {
            inbound
                .receive(envelope)
                .map_err(|error| RemoteError::Inbound(error.to_string()))
        })
    })?;
    Ok(())
}
