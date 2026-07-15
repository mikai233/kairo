use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorRef, Address, Props};
use kairo_serialization::{Manifest, Registry, RemoteMessage, SerializedMessage};
use kairo_testkit::ActorSystemTestKit;

use super::*;
use crate::{
    ClusterEvent, ClusterEventPublisher, ClusterEventPublisherMsg, GOSSIP_ENVELOPE_SERIALIZER_ID,
    Gossip, JOIN_SERIALIZER_ID, Member, MemberStatus, SubscriptionInitialState,
    WELCOME_SERIALIZER_ID, register_cluster_protocol_codecs,
};

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_cluster_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

fn node(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(Address::local(system), uid)
}

fn member(unique_address: UniqueAddress, status: MemberStatus) -> Member {
    Member::new(unique_address, Vec::new()).with_status(status)
}

fn spawn_membership(
    kit: &ActorSystemTestKit,
    self_node: UniqueAddress,
    name: &str,
) -> ActorRef<ClusterMembershipMsg> {
    let publisher = kit
        .system()
        .spawn(
            format!("{name}-publisher"),
            Props::new({
                let self_node = self_node.clone();
                move || ClusterEventPublisher::new(self_node.clone())
            }),
        )
        .unwrap();
    let events = kit
        .create_probe::<ClusterEvent>(format!("{name}-events"))
        .unwrap();
    publisher
        .tell(ClusterEventPublisherMsg::Subscribe {
            subscriber: events.actor_ref(),
            initial_state: SubscriptionInitialState::None,
        })
        .unwrap();
    kit.system()
        .spawn(
            name,
            Props::new(move || {
                crate::ClusterMembership::new(self_node.clone(), Vec::new(), publisher.clone())
            }),
        )
        .unwrap()
}

#[test]
fn wire_outbound_serializes_join_welcome_and_gossip_for_target_node() {
    let kit = ActorSystemTestKit::new("cluster-wire-outbound").unwrap();
    let registry = registry();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let outbound_probe = kit
        .create_probe::<ClusterSerializedMembership>("wire-out")
        .unwrap();
    let outbound = ClusterMembershipWireOutbound::new(
        node_b.clone(),
        registry.clone(),
        outbound_probe.actor_ref(),
    );

    outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: node_a.clone(),
                roles: vec!["backend".to_string()],
            },
            reply_to: None,
        })
        .unwrap();
    let join_envelope = outbound_probe
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(join_envelope.target, node_b);
    assert_eq!(join_envelope.message.serializer_id, JOIN_SERIALIZER_ID);
    assert_eq!(
        registry
            .deserialize::<Join>(join_envelope.message)
            .unwrap()
            .node,
        node_a
    );

    let gossip = Gossip::from_members([member(node("a", 1), MemberStatus::Up)]);
    outbound
        .send_membership(ClusterMembershipMsg::Welcome(Box::new(Welcome {
            from: node("a", 1),
            gossip: gossip.clone(),
        })))
        .unwrap();
    let welcome_envelope = outbound_probe
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(
        welcome_envelope.message.serializer_id,
        WELCOME_SERIALIZER_ID
    );
    assert_eq!(
        registry
            .deserialize::<Welcome>(welcome_envelope.message)
            .unwrap()
            .gossip,
        gossip
    );

    outbound
        .send_membership(ClusterMembershipMsg::Gossip {
            envelope: Box::new(GossipEnvelope {
                from: node("a", 1),
                to: node("b", 2),
                sequence_nr: 7,
                gossip: Gossip::from_members([member(node("b", 2), MemberStatus::Joining)]),
            }),
            reply_to: None,
        })
        .unwrap();
    let gossip_envelope = outbound_probe
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(
        gossip_envelope.message.serializer_id,
        GOSSIP_ENVELOPE_SERIALIZER_ID
    );
    assert_eq!(
        registry
            .deserialize::<GossipEnvelope>(gossip_envelope.message)
            .unwrap()
            .sequence_nr,
        7
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn wire_inbound_delivers_join_and_routes_welcome_reply() {
    let kit = ActorSystemTestKit::new("cluster-wire-join-in").unwrap();
    let registry = registry();
    let seed = node("seed", 1);
    let joining = node("joining", 2);
    let membership = spawn_membership(&kit, seed.clone(), "membership");
    let outbound_probe = kit
        .create_probe::<ClusterSerializedMembership>("wire-out")
        .unwrap();
    let outbound = ClusterMembershipWireOutbound::new(
        joining.clone(),
        registry.clone(),
        outbound_probe.actor_ref(),
    );
    let outbound_actor = kit
        .system()
        .spawn(
            "wire-outbound",
            Props::new(move || ClusterMembershipWireOutboundActor::new(outbound.clone())),
        )
        .unwrap();
    membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
    let inbound = ClusterMembershipWireInbound::new(seed.clone(), registry.clone(), membership)
        .with_reply_route(joining.clone(), outbound_actor);

    inbound
        .receive(ClusterSerializedMembership::new(
            seed.clone(),
            registry
                .serialize(&Join {
                    node: joining.clone(),
                    roles: vec!["backend".to_string()],
                })
                .unwrap(),
        ))
        .unwrap();

    let welcome_envelope = outbound_probe.expect_msg(Duration::from_secs(1)).unwrap();
    assert_eq!(welcome_envelope.target, joining.clone());
    assert_eq!(
        welcome_envelope.message.serializer_id,
        WELCOME_SERIALIZER_ID
    );
    let welcome = registry
        .deserialize::<Welcome>(welcome_envelope.message)
        .unwrap();
    assert_eq!(welcome.from, seed);
    assert_eq!(
        welcome.gossip.member(&joining).map(|member| member.status),
        Some(MemberStatus::Joining)
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn wire_inbound_delivers_welcome_to_membership_actor() {
    let kit = ActorSystemTestKit::new("cluster-wire-welcome-in").unwrap();
    let registry = registry();
    let seed = node("seed", 1);
    let joining = node("joining", 2);
    let membership = spawn_membership(&kit, joining.clone(), "membership");
    let seed_join = kit
        .create_probe::<ClusterSeedJoinProcessMsg>("seed-join")
        .unwrap();
    let gossip_probe = kit.create_probe::<Gossip>("gossip").unwrap();
    let gossip = Gossip::from_members([
        member(seed.clone(), MemberStatus::Up),
        member(joining.clone(), MemberStatus::Joining),
    ]);
    let inbound =
        ClusterMembershipWireInbound::new(joining.clone(), registry.clone(), membership.clone())
            .with_seed_join_process(seed_join.actor_ref());

    inbound
        .receive(ClusterSerializedMembership::new(
            joining.clone(),
            registry.serialize(&Welcome { from: seed, gossip }).unwrap(),
        ))
        .unwrap();
    membership
        .tell(ClusterMembershipMsg::SendCurrentGossip {
            reply_to: gossip_probe.actor_ref(),
        })
        .unwrap();

    let current = gossip_probe.expect_msg(Duration::from_secs(1)).unwrap();
    assert!(current.has_member(&joining));
    assert!(current.seen_by().contains(&joining));
    assert!(matches!(
        seed_join.expect_msg(Duration::from_secs(1)).unwrap(),
        ClusterSeedJoinProcessMsg::Welcome { from } if from == node("seed", 1).address
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn wire_inbound_rejects_wrong_target_and_unknown_manifest() {
    let kit = ActorSystemTestKit::new("cluster-wire-reject").unwrap();
    let registry = registry();
    let self_node = node("self", 1);
    let membership = spawn_membership(&kit, self_node.clone(), "membership");
    let inbound = ClusterMembershipWireInbound::new(self_node, registry, membership);

    let wrong_target = inbound
        .receive(ClusterSerializedMembership::new(
            node("other", 99),
            SerializedMessage::new(
                JOIN_SERIALIZER_ID,
                Manifest::new(Join::MANIFEST),
                Join::VERSION,
                bytes::Bytes::new(),
            ),
        ))
        .expect_err("wrong target should fail");
    assert!(matches!(
        wrong_target,
        ClusterMembershipWireError::WrongTarget { .. }
    ));

    let unknown = inbound
        .receive_message(SerializedMessage::new(
            9_999,
            Manifest::new("kairo.cluster.unknown"),
            1,
            bytes::Bytes::new(),
        ))
        .expect_err("unknown manifest should fail");
    assert!(matches!(
        unknown,
        ClusterMembershipWireError::UnsupportedManifest(_)
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
