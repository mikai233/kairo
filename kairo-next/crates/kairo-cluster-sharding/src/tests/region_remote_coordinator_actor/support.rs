use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorRef, Props};
use kairo_cluster::{MemberStatus, UniqueAddress};
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};
use kairo_testkit::{ActorSystemTestKit, TestProbe};

use crate::{
    CoordinatorDiscoverySettings, RegionCoordinatorDiscoveryConfig,
    RegionRemoteCoordinatorTransport, ShardCoordinatorRemoteTarget, ShardRegionActor,
    ShardRegionMsg, register_sharding_protocol_codecs,
};

use super::super::{cluster_member, cluster_state, remote_unique_node};

pub(super) struct RemoteCoordinatorFixture {
    coordinator_node: UniqueAddress,
    target: ShardCoordinatorRemoteTarget,
    registry: Arc<Registry>,
    remote_envelopes: TestProbe<RemoteEnvelope>,
    region_wire: ActorRefWireData,
    retry_interval: Duration,
}

impl RemoteCoordinatorFixture {
    pub(super) fn new(
        kit: &ActorSystemTestKit,
        system: &str,
        coordinator_port: u16,
        coordinator_uid: u64,
        retry_interval_millis: u64,
    ) -> Self {
        let coordinator_node =
            remote_unique_node(system, "127.0.0.1", coordinator_port, coordinator_uid);
        let target = ShardCoordinatorRemoteTarget::for_node(
            coordinator_node.clone(),
            crate::DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
        )
        .unwrap();
        let mut registry = Registry::new();
        register_sharding_protocol_codecs(&mut registry).unwrap();
        let remote_envelopes = kit
            .create_probe::<RemoteEnvelope>("remote-coordinator-envelopes")
            .unwrap();
        Self {
            coordinator_node,
            target,
            registry: Arc::new(registry),
            remote_envelopes,
            region_wire: ActorRefWireData::new(
                "kairo://local@127.0.0.1:2551/system/sharding/region",
            )
            .unwrap(),
            retry_interval: Duration::from_millis(retry_interval_millis),
        }
    }

    pub(super) fn target(&self) -> &ShardCoordinatorRemoteTarget {
        &self.target
    }

    pub(super) fn region_wire(&self) -> &ActorRefWireData {
        &self.region_wire
    }

    pub(super) fn spawn_region(
        &self,
        kit: &ActorSystemTestKit,
        name: &'static str,
    ) -> ActorRef<ShardRegionMsg<String>> {
        let discovery = self.discovery();
        let transport = self.transport();
        kit.system()
            .spawn(
                name,
                Props::new(move || {
                    ShardRegionActor::<String>::new_with_local_shards(name, 10, 10)
                        .with_coordinator_discovery(discovery.clone())
                        .with_remote_coordinator_transport(transport.clone())
                }),
            )
            .unwrap()
    }

    pub(super) fn publish_discovery(&self, region: &ActorRef<ShardRegionMsg<String>>) {
        region
            .tell(ShardRegionMsg::CoordinatorDiscoverySnapshot {
                state: cluster_state(vec![cluster_member(
                    self.coordinator_node.clone(),
                    MemberStatus::Up,
                    ["backend"],
                    1,
                )]),
            })
            .unwrap();
    }

    pub(super) fn expect_remote_message<M>(&self) -> M
    where
        M: RemoteMessage + Send + 'static,
    {
        let envelope = self
            .remote_envelopes
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        assert_eq!(envelope.recipient, self.target.recipient().clone());
        assert_eq!(envelope.sender, Some(self.region_wire.clone()));
        assert_eq!(envelope.message.manifest.as_str(), M::MANIFEST);
        self.registry.deserialize::<M>(envelope.message).unwrap()
    }

    fn discovery(&self) -> RegionCoordinatorDiscoveryConfig<String> {
        RegionCoordinatorDiscoveryConfig::<String>::new(
            CoordinatorDiscoverySettings::default().with_required_role("backend"),
            self.retry_interval,
        )
        .with_remote_coordinator(self.target.clone())
    }

    fn transport(&self) -> RegionRemoteCoordinatorTransport {
        RegionRemoteCoordinatorTransport::new(
            self.region_wire.clone(),
            self.registry.clone(),
            self.remote_envelopes.actor_ref(),
        )
    }
}
