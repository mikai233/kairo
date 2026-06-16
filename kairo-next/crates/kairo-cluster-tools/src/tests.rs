use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use kairo_actor::{Actor, ActorRef, ActorResult, Address, Context, Props};
use kairo_cluster::{ClusterEvent, Member, MemberEvent, MemberStatus, UniqueAddress};
use kairo_remote::{RemoteActorRef, RemoteOutbound};
use kairo_serialization::{
    ActorRefWireData, MessageCodec, Registry, RemoteEnvelope, RemoteMessage, SerializationRegistry,
};
use kairo_testkit::ActorSystemTestKit;

use crate::{
    CurrentTopics, DistributedPubSubMediatorActor, DistributedPubSubMediatorMsg,
    DistributedPubSubPublishReport, DistributedPubSubSnapshot, LocalPubSub, LocalPubSubActor,
    LocalPubSubMsg, LocalSingletonManagerActor, LocalSingletonManagerMsg,
    LocalSingletonManagerSnapshot, LocalTopic, PubSubDeliveryFailure, PubSubDeliveryPlan,
    PubSubDeliveryTarget, PubSubDeliveryTransport, PubSubGossipActor, PubSubGossipMsg,
    PubSubGossipPeer, PubSubRegistryKey, PubSubRegistryState, PubSubRemoteTarget,
    PubSubSubscribeAck, PubSubTopicReport, SingletonManagerActor, SingletonManagerEffect,
    SingletonManagerMsg, SingletonManagerRuntime, SingletonManagerSettings,
    SingletonManagerSettingsError, SingletonManagerSnapshot, SingletonManagerState,
    SingletonOldestChange, SingletonOldestTracker, SingletonProxyActor, SingletonProxyMsg,
    SingletonProxySettings, SingletonProxySnapshot,
    SingletonProxyTarget as RemoteSingletonProxyTarget, SingletonScope, TopicName,
    TopicPublishMode,
};

mod distributed_pubsub_mediator;
mod local_pubsub;
mod local_singleton_manager;
mod local_topic;
mod pubsub_delivery;
mod pubsub_gossip;
mod pubsub_registry;
mod singleton_manager;
mod singleton_oldest;
mod singleton_proxy;

fn member(unique_address: UniqueAddress, status: MemberStatus, up_number: u64) -> Member {
    Member::new(unique_address, Vec::new())
        .with_status(status)
        .with_up_number(up_number)
}

fn member_with_roles(
    unique_address: UniqueAddress,
    status: MemberStatus,
    up_number: u64,
    roles: impl IntoIterator<Item = &'static str>,
) -> Member {
    Member::new(
        unique_address,
        roles.into_iter().map(String::from).collect(),
    )
    .with_status(status)
    .with_up_number(up_number)
}

fn node(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(Address::local(system), uid)
}

fn remote_node(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new(
            "kairo",
            system,
            Some(format!("{system}.example.test")),
            Some(2552),
        ),
        uid,
    )
}
