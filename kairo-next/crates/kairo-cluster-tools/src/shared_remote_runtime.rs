use kairo_actor::Address;
use kairo_cluster::UniqueAddress;
use kairo_remote::{
    RemoteAssociationCache, RemoteError, Result as RemoteResult, TcpRemoteActorRuntimeBuilder,
};
use kairo_serialization::RemoteMessage;

use crate::{
    ClusterToolsSystemInbound, PubSubDelta, PubSubPathEnvelope, PubSubPublishEnvelope,
    PubSubStatus, SingletonHandOverDone, SingletonHandOverInProgress, SingletonHandOverToMe,
    SingletonTakeOverFromMe,
};

pub const CLUSTER_TOOLS_SYSTEM_MANIFESTS: [&str; 8] = [
    PubSubStatus::MANIFEST,
    PubSubDelta::MANIFEST,
    PubSubPublishEnvelope::MANIFEST,
    PubSubPathEnvelope::MANIFEST,
    SingletonHandOverToMe::MANIFEST,
    SingletonHandOverInProgress::MANIFEST,
    SingletonHandOverDone::MANIFEST,
    SingletonTakeOverFromMe::MANIFEST,
];

/// Registers cluster-tools system traffic with an ActorSystem-owned remote runtime.
///
/// The factory runs after TCP bind so it receives the effective canonical node
/// address and the association cache shared by actor remoting and clustering.
pub fn register_cluster_tools_system_inbound<M, F>(
    builder: &mut TcpRemoteActorRuntimeBuilder,
    node_uid: u64,
    factory: F,
) -> RemoteResult<()>
where
    M: RemoteMessage + Send + 'static,
    F: FnOnce(UniqueAddress, RemoteAssociationCache) -> ClusterToolsSystemInbound<M>
        + Send
        + 'static,
{
    builder.register_control_handler(&CLUSTER_TOOLS_SYSTEM_MANIFESTS, move |context| {
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

#[cfg(test)]
mod tests;
