use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use kairo_actor::{Recipient, SendError};
use kairo_cluster::UniqueAddress;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};
use kairo_testkit::ActorSystemTestKit;

use crate::{
    BEGIN_HANDOFF_SERIALIZER_ID, BeginHandOff, HANDOFF_SERIALIZER_ID, HOST_SHARD_SERIALIZER_ID,
    HandOff, HandOffPlan, HostShard, HostShardPlan, SHARD_STARTED_SERIALIZER_ID, ShardRegionMsg,
    ShardRegionRemoteError, ShardStarted, register_sharding_protocol_codecs,
};

use super::*;

struct CollectingRecipient<M> {
    tx: mpsc::Sender<M>,
}

impl<M> Recipient<M> for CollectingRecipient<M>
where
    M: Send + 'static,
{
    fn tell(&self, message: M) -> Result<(), SendError<M>> {
        self.tx
            .send(message)
            .map_err(|error| SendError::new(error.0, "collector closed"))
    }
}

fn collector<M>() -> (CollectingRecipient<M>, Receiver<M>)
where
    M: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    (CollectingRecipient { tx }, rx)
}

#[test]
fn remote_control_outbound_sends_stable_region_control_envelopes() {
    let kit = ActorSystemTestKit::new("sharding-remote-control-outbound").unwrap();
    let registry = registry();
    let (outbound, rx) = collector::<RemoteEnvelope>();
    let host_reply = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let begin_reply = kit
        .create_probe::<crate::BeginHandOffPlan>("begin")
        .unwrap();
    let handoff_reply = kit.create_probe::<HandOffPlan>("handoff").unwrap();
    let target =
        ShardRegionRemoteControlOutbound::<String>::new(target_node(), registry.clone(), outbound)
            .with_sender(Some(coordinator()));

    target
        .tell(ShardRegionMsg::HostShard {
            shard: "12".to_string(),
            reply_to: host_reply.actor_ref(),
        })
        .unwrap();
    target
        .tell(ShardRegionMsg::BeginHandOff {
            shard: "12".to_string(),
            reply_to: begin_reply.actor_ref(),
        })
        .unwrap();
    target
        .tell(ShardRegionMsg::HandOff {
            shard: "12".to_string(),
            reply_to: handoff_reply.actor_ref(),
        })
        .unwrap();

    let host = rx.recv().unwrap();
    assert_eq!(host.recipient, region());
    assert_eq!(host.sender, Some(coordinator()));
    assert_eq!(host.message.serializer_id, HOST_SHARD_SERIALIZER_ID);
    assert_eq!(host.message.manifest.as_str(), HostShard::MANIFEST);

    let begin = rx.recv().unwrap();
    assert_eq!(begin.recipient, region());
    assert_eq!(begin.sender, Some(coordinator()));
    assert_eq!(begin.message.serializer_id, BEGIN_HANDOFF_SERIALIZER_ID);
    assert_eq!(begin.message.manifest.as_str(), BeginHandOff::MANIFEST);

    let handoff = rx.recv().unwrap();
    assert_eq!(handoff.recipient, region());
    assert_eq!(handoff.sender, Some(coordinator()));
    assert_eq!(handoff.message.serializer_id, HANDOFF_SERIALIZER_ID);
    assert_eq!(handoff.message.manifest.as_str(), HandOff::MANIFEST);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_control_inbound_decodes_command_and_sends_stable_reply() {
    let registry = registry();
    let (outbound, rx) = collector::<RemoteEnvelope>();
    let inbound = ShardRegionRemoteControlInbound::new(region(), registry.clone(), outbound);

    let decoded = inbound
        .receive(RemoteEnvelope::new(
            region(),
            Some(coordinator()),
            registry
                .serialize(&HostShard {
                    shard_id: "12".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();

    let ShardRegionRemoteControlCommand::HostShard { shard, reply } = decoded else {
        panic!("expected HostShard command");
    };
    assert_eq!(shard, "12");
    assert_eq!(reply.region(), &region());
    assert_eq!(reply.coordinator(), &coordinator());

    reply.send_shard_started(shard).unwrap();
    let envelope = rx.recv().unwrap();
    assert_eq!(envelope.recipient, coordinator());
    assert_eq!(envelope.sender, Some(region()));
    assert_eq!(envelope.message.serializer_id, SHARD_STARTED_SERIALIZER_ID);
    assert_eq!(envelope.message.manifest.as_str(), ShardStarted::MANIFEST);
}

#[test]
fn remote_control_inbound_rejects_wrong_recipient_sender_or_manifest() {
    let registry = registry();
    let (outbound, _rx) = collector::<RemoteEnvelope>();
    let inbound = ShardRegionRemoteControlInbound::new(region(), registry.clone(), outbound);

    let wrong_recipient = RemoteEnvelope::new(
        ActorRefWireData::new("kairo://remote@127.0.0.1:2552/user/not-region").unwrap(),
        Some(coordinator()),
        registry
            .serialize(&HostShard {
                shard_id: "12".to_string(),
            })
            .unwrap(),
    );
    assert!(matches!(
        inbound.receive(wrong_recipient).unwrap_err(),
        ShardRegionRemoteError::WrongRecipient { .. }
    ));

    let missing_sender = RemoteEnvelope::new(
        region(),
        None,
        registry
            .serialize(&BeginHandOff {
                shard_id: "12".to_string(),
            })
            .unwrap(),
    );
    assert!(matches!(
        inbound.receive(missing_sender).unwrap_err(),
        ShardRegionRemoteError::MissingSender(_)
    ));

    let wrong_manifest = RemoteEnvelope::new(
        region(),
        Some(coordinator()),
        registry
            .serialize(&ShardStarted {
                shard_id: "12".to_string(),
            })
            .unwrap(),
    );
    assert!(matches!(
        inbound.receive(wrong_manifest).unwrap_err(),
        ShardRegionRemoteError::UnsupportedManifest(_)
    ));
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

fn target_node() -> UniqueAddress {
    UniqueAddress::new(
        kairo_actor::Address::new("kairo", "remote", Some("127.0.0.1".to_string()), Some(2552)),
        2,
    )
}

fn coordinator() -> ActorRefWireData {
    ActorRefWireData::new("kairo://local@127.0.0.1:2551/system/sharding/coordinator").unwrap()
}

fn region() -> ActorRefWireData {
    ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/region").unwrap()
}
