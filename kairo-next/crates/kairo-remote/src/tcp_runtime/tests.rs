use std::sync::{Arc, mpsc};
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{Actor, ActorResult, ActorSystem, Context, Props};
use kairo_serialization::{MessageCodec, Registry, RemoteMessage, SerializationRegistry};

use super::TcpRemoteActorSystem;
use crate::{
    AssociationState, RemoteAssociationAddress, RemoteSettings, TcpAssociationIdentity,
    register_remote_protocol_codecs,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Ping {
    value: u8,
}

impl RemoteMessage for Ping {
    const MANIFEST: &'static str = "kairo.remote.test.TcpRuntimePing";
    const VERSION: u16 = 1;
}

struct PingCodec;

impl MessageCodec<Ping> for PingCodec {
    fn serializer_id(&self) -> u32 {
        991
    }

    fn encode(&self, message: &Ping) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value]))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Ping> {
        Ok(Ping { value: payload[0] })
    }
}

struct Target {
    received: mpsc::Sender<u8>,
}

impl Actor for Target {
    type Msg = Ping;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.received
            .send(msg.value)
            .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
    }
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    registry.register::<Ping, _>(PingCodec).unwrap();
    register_remote_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

fn remote_path_for(local_path: &str, settings: &RemoteSettings) -> String {
    local_path.replacen(
        "kairo://receiver",
        &format!(
            "kairo://receiver@{}:{}",
            settings.canonical_hostname, settings.canonical_port
        ),
        1,
    )
}

#[test]
fn tcp_remote_actor_system_sends_remote_ref_to_local_actor_over_loopback() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let sender = ActorSystem::builder("sender").build().unwrap();
    let registry = registry();
    let (received_tx, received_rx) = mpsc::channel();
    let target = receiver
        .spawn(
            "target",
            Props::new(move || Target {
                received: received_tx,
            }),
        )
        .unwrap();
    let receiver_remote = TcpRemoteActorSystem::<Ping>::bind(
        receiver.clone(),
        registry.clone(),
        RemoteSettings::new("127.0.0.1", 0),
        11,
    )
    .unwrap();
    let sender_remote = TcpRemoteActorSystem::<Ping>::bind(
        sender,
        registry,
        RemoteSettings::new("127.0.0.1", 0),
        22,
    )
    .unwrap();
    let sender_identity = TcpAssociationIdentity::new(
        RemoteAssociationAddress::new(
            "kairo",
            "sender",
            sender_remote.settings().canonical_hostname.clone(),
            Some(sender_remote.settings().canonical_port),
        )
        .unwrap(),
        22,
    );
    let receiver_address = RemoteAssociationAddress::new(
        "kairo",
        "receiver",
        receiver_remote.settings().canonical_hostname.clone(),
        Some(receiver_remote.settings().canonical_port),
    )
    .unwrap();
    let local_canonical_target = receiver_remote
        .resolve_actor_ref::<Ping>(remote_path_for(
            target.path().as_str(),
            receiver_remote.settings(),
        ))
        .unwrap();
    assert!(local_canonical_target.is_local());
    local_canonical_target.tell(Ping { value: 76 }).unwrap();
    assert_eq!(
        received_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        76
    );

    let registration = sender_remote.dial(receiver_address).unwrap();
    let remote_target = sender_remote
        .resolve::<Ping>(remote_path_for(
            target.path().as_str(),
            receiver_remote.settings(),
        ))
        .unwrap();

    remote_target.tell(Ping { value: 77 }).unwrap();

    assert_eq!(
        received_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        77
    );
    let receiver_association = receiver_remote
        .association_registry()
        .association_by_uid(22);
    assert!(receiver_association.is_some());
    assert_eq!(
        receiver_association
            .unwrap()
            .lock()
            .expect("remote association lock poisoned")
            .state(),
        &AssociationState::Active {
            remote_uid: Some(22)
        }
    );

    drop(registration);
    let sender_watch = sender_remote.death_watch().clone();
    let receiver_watch = receiver_remote.death_watch().clone();
    let sender_report = sender_remote.shutdown().unwrap();
    assert!(sender_watch.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(sender_report.accepted_associations, 0);
    let receiver_report = receiver_remote.shutdown().unwrap();
    assert!(receiver_watch.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(receiver_report.accepted_associations, 1);
    assert_eq!(receiver_report.remote_identities, vec![sender_identity]);
    assert_eq!(receiver_report.read.streams, 3);
    assert_eq!(receiver_report.read.frames, 1);
}
