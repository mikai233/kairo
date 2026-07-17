use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::Address;
use kairo_remote::{
    RemoteAssociationAddress, RemoteSettings, TcpRemoteActorRuntime,
    register_remote_protocol_codecs,
};
use kairo_serialization::{MessageCodec, Registry, RemoteMessage, SerializationRegistry};
use kairo_testkit::ActorSystemTestKit;

use crate::{
    ClusterMembershipMsg, ClusterMembershipWireInbound, ClusterSystemInbound,
    DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH, Join, UniqueAddress, register_cluster_protocol_codecs,
    register_cluster_system_inbound,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Business {
    value: u8,
}

impl RemoteMessage for Business {
    const MANIFEST: &'static str = "kairo.cluster.test.SharedRuntimeBusiness";
    const VERSION: u16 = 1;
}

struct BusinessCodec;

impl MessageCodec<Business> for BusinessCodec {
    fn serializer_id(&self) -> u32 {
        4_991
    }

    fn encode(&self, message: &Business) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value]))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Business> {
        Ok(Business { value: payload[0] })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OtherBusiness {
    value: u16,
}

impl RemoteMessage for OtherBusiness {
    const MANIFEST: &'static str = "kairo.cluster.test.SharedRuntimeOtherBusiness";
    const VERSION: u16 = 1;
}

struct OtherBusinessCodec;

impl MessageCodec<OtherBusiness> for OtherBusinessCodec {
    fn serializer_id(&self) -> u32 {
        4_992
    }

    fn encode(&self, message: &OtherBusiness) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::copy_from_slice(&message.value.to_be_bytes()))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<OtherBusiness> {
        Ok(OtherBusiness {
            value: u16::from_be_bytes([payload[0], payload[1]]),
        })
    }
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    registry.register::<Business, _>(BusinessCodec).unwrap();
    registry
        .register::<OtherBusiness, _>(OtherBusinessCodec)
        .unwrap();
    register_remote_protocol_codecs(&mut registry).unwrap();
    register_cluster_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

fn node_for(runtime: &TcpRemoteActorRuntime, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new(
            runtime.system().address().protocol(),
            runtime.system().name(),
            Some(runtime.settings().canonical_hostname.clone()),
            Some(runtime.settings().canonical_port),
        ),
        uid,
    )
}

fn remote_path(local_path: &str, runtime: &TcpRemoteActorRuntime) -> String {
    local_path.replacen(
        &format!("kairo://{}", runtime.system().name()),
        &format!(
            "kairo://{}@{}:{}",
            runtime.system().name(),
            runtime.settings().canonical_hostname,
            runtime.settings().canonical_port
        ),
        1,
    )
}

#[test]
fn shared_remote_runtime_carries_two_business_protocols_and_cluster_control() {
    let receiver_kit = ActorSystemTestKit::new("receiver").unwrap();
    let sender_kit = ActorSystemTestKit::new("sender").unwrap();
    let receiver_business = receiver_kit.create_probe::<Business>("business").unwrap();
    let receiver_other_business = receiver_kit
        .create_probe::<OtherBusiness>("other-business")
        .unwrap();
    let receiver_membership = receiver_kit
        .create_probe::<ClusterMembershipMsg>("membership")
        .unwrap();
    let sender_membership = sender_kit
        .create_probe::<ClusterMembershipMsg>("membership")
        .unwrap();
    let registry = registry();

    let receiver_membership_ref = receiver_membership.actor_ref();
    let receiver_registry = registry.clone();
    let mut receiver_builder = TcpRemoteActorRuntime::builder(
        receiver_kit.system().clone(),
        registry.clone(),
        RemoteSettings::new("127.0.0.1", 0),
        22,
    );
    receiver_builder.register::<Business>().unwrap();
    receiver_builder.register::<OtherBusiness>().unwrap();
    register_cluster_system_inbound(&mut receiver_builder, 2, move |self_node, _| {
        ClusterSystemInbound::new(self_node.clone()).with_membership(
            ClusterMembershipWireInbound::new(
                self_node,
                receiver_registry,
                receiver_membership_ref,
            ),
        )
    })
    .unwrap();
    let receiver_remote = receiver_builder.bind().unwrap();

    let sender_membership_ref = sender_membership.actor_ref();
    let sender_registry = registry.clone();
    let mut sender_builder = TcpRemoteActorRuntime::builder(
        sender_kit.system().clone(),
        registry,
        RemoteSettings::new("127.0.0.1", 0),
        11,
    );
    sender_builder.register::<Business>().unwrap();
    sender_builder.register::<OtherBusiness>().unwrap();
    register_cluster_system_inbound(&mut sender_builder, 1, move |self_node, _| {
        ClusterSystemInbound::new(self_node.clone()).with_membership(
            ClusterMembershipWireInbound::new(self_node, sender_registry, sender_membership_ref),
        )
    })
    .unwrap();
    let sender_remote = sender_builder.bind().unwrap();

    let receiver_address = RemoteAssociationAddress::new(
        "kairo",
        "receiver",
        receiver_remote.settings().canonical_hostname.clone(),
        Some(receiver_remote.settings().canonical_port),
    )
    .unwrap();
    sender_remote.dial(receiver_address).unwrap();

    let remote_business = sender_remote
        .resolve::<Business>(remote_path(
            receiver_business.actor_ref().path().as_str(),
            &receiver_remote,
        ))
        .unwrap();
    let receiver_node = node_for(&receiver_remote, 2);
    let sender_node = node_for(&sender_remote, 1);
    let remote_other_business = sender_remote
        .resolve::<OtherBusiness>(remote_path(
            receiver_other_business.actor_ref().path().as_str(),
            &receiver_remote,
        ))
        .unwrap();
    let remote_membership = sender_remote
        .resolve::<Join>(format!(
            "{}{}",
            receiver_node.address, DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH
        ))
        .unwrap();

    remote_business.tell(Business { value: 7 }).unwrap();
    remote_other_business
        .tell(OtherBusiness { value: 700 })
        .unwrap();
    remote_membership
        .tell(Join {
            node: sender_node.clone(),
            roles: vec!["backend".to_string()],
            app_version: crate::ApplicationVersion::default(),
        })
        .unwrap();

    receiver_business
        .expect_msg_eq(Business { value: 7 }, Duration::from_secs(1))
        .unwrap();
    receiver_other_business
        .expect_msg_eq(OtherBusiness { value: 700 }, Duration::from_secs(1))
        .unwrap();
    let membership = receiver_membership
        .expect_msg(Duration::from_secs(1))
        .unwrap();
    assert!(matches!(
        membership,
        ClusterMembershipMsg::Join { join, .. }
            if join.node == sender_node && join.roles == vec!["backend"]
    ));
    assert_eq!(sender_remote.association_cache().route_count(), 1);

    sender_remote.shutdown().unwrap();
    let report = receiver_remote.shutdown().unwrap();
    assert_eq!(report.accepted_associations, 1);
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}
