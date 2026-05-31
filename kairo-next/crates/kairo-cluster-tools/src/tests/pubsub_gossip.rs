use super::*;

#[test]
fn pubsub_gossip_actor_sends_status_to_peers_on_tick() {
    let kit = ActorSystemTestKit::new("pubsub-gossip-tick").unwrap();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let peer_b = kit.create_probe::<PubSubGossipMsg>("peer-b").unwrap();
    let peer_c = kit.create_probe::<PubSubGossipMsg>("peer-c").unwrap();
    let actor_node = node_a.clone();
    let gossip = kit
        .system()
        .spawn(
            "gossip",
            Props::new(move || PubSubGossipActor::new(actor_node)),
        )
        .unwrap();

    gossip
        .tell(PubSubGossipMsg::RegisterTopic {
            topic: TopicName::new("orders"),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::AddPeer {
            peer: PubSubGossipPeer::new(node_b.clone(), peer_b.actor_ref()),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::AddPeer {
            peer: PubSubGossipPeer::new(node_c, peer_c.actor_ref()),
        })
        .unwrap();

    gossip.tell(PubSubGossipMsg::GossipTick).unwrap();
    match peer_b.expect_msg(Duration::from_millis(500)).unwrap() {
        PubSubGossipMsg::Status {
            from,
            versions,
            reply,
        } => {
            assert_eq!(from, node_a);
            assert!(!reply);
            assert_eq!(versions.get(&node("a", 1).ordering_key()), Some(&1));
        }
        _ => panic!("expected status gossip"),
    }
    peer_c.expect_no_msg(Duration::from_millis(30)).unwrap();

    gossip.tell(PubSubGossipMsg::GossipTick).unwrap();
    match peer_c.expect_msg(Duration::from_millis(500)).unwrap() {
        PubSubGossipMsg::Status { reply, .. } => assert!(!reply),
        _ => panic!("expected status gossip"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn pubsub_gossip_actor_replies_to_status_with_delta_and_status_when_needed() {
    let kit = ActorSystemTestKit::new("pubsub-gossip-status").unwrap();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let peer_b = kit.create_probe::<PubSubGossipMsg>("peer-b").unwrap();
    let actor_node = node_a.clone();
    let gossip = kit
        .system()
        .spawn(
            "gossip",
            Props::new(move || PubSubGossipActor::new(actor_node)),
        )
        .unwrap();
    let orders = TopicName::new("orders");

    gossip
        .tell(PubSubGossipMsg::AddPeer {
            peer: PubSubGossipPeer::new(node_b.clone(), peer_b.actor_ref()),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::RegisterTopic {
            topic: orders.clone(),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::Status {
            from: node_b.clone(),
            versions: BTreeMap::new(),
            reply: false,
        })
        .unwrap();

    match peer_b.expect_msg(Duration::from_millis(500)).unwrap() {
        PubSubGossipMsg::Delta { from, delta } => {
            assert_eq!(from, node_a.clone());
            assert_eq!(delta.buckets.len(), 1);
            assert!(
                delta.buckets[0]
                    .entries
                    .contains_key(&PubSubRegistryKey::topic(orders))
            );
        }
        _ => panic!("expected delta reply"),
    }

    gossip
        .tell(PubSubGossipMsg::Status {
            from: node_b.clone(),
            versions: BTreeMap::from([(node_a.ordering_key(), 1), (node_b.ordering_key(), 1)]),
            reply: false,
        })
        .unwrap();
    match peer_b.expect_msg(Duration::from_millis(500)).unwrap() {
        PubSubGossipMsg::Status { from, reply, .. } => {
            assert_eq!(from, node("a", 1));
            assert!(reply);
        }
        _ => panic!("expected status reply"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn pubsub_gossip_actor_merges_delta_from_known_peer() {
    let kit = ActorSystemTestKit::new("pubsub-gossip-delta").unwrap();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let peer_b = kit.create_probe::<PubSubGossipMsg>("peer-b").unwrap();
    let registry_probe = kit.create_probe::<PubSubRegistryState>("registry").unwrap();
    let count_probe = kit.create_probe::<u64>("delta-count").unwrap();
    let actor_node = node_a;
    let gossip = kit
        .system()
        .spawn(
            "gossip",
            Props::new(move || PubSubGossipActor::new(actor_node)),
        )
        .unwrap();
    let jobs = TopicName::new("jobs");
    let mut remote_registry = PubSubRegistryState::new(node_b.clone());
    remote_registry.register_local_group(jobs.clone(), "workers");

    gossip
        .tell(PubSubGossipMsg::AddPeer {
            peer: PubSubGossipPeer::new(node_b.clone(), peer_b.actor_ref()),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::Delta {
            from: node_b.clone(),
            delta: remote_registry.collect_delta(&BTreeMap::new(), 10),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::GetRegistry {
            reply_to: registry_probe.actor_ref(),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::GetDeltaCount {
            reply_to: count_probe.actor_ref(),
        })
        .unwrap();

    let registry = registry_probe
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(
        registry.one_per_group_targets(&jobs).get("workers"),
        Some(&node_b)
    );
    assert_eq!(
        count_probe.expect_msg(Duration::from_millis(500)).unwrap(),
        1
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn pubsub_gossip_actor_ignores_delta_from_unknown_peer_and_removes_left_peer() {
    let kit = ActorSystemTestKit::new("pubsub-gossip-unknown").unwrap();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let peer_b = kit.create_probe::<PubSubGossipMsg>("peer-b").unwrap();
    let registry_probe = kit.create_probe::<PubSubRegistryState>("registry").unwrap();
    let peers_probe = kit.create_probe::<Vec<UniqueAddress>>("peers").unwrap();
    let actor_node = node_a.clone();
    let gossip = kit
        .system()
        .spawn(
            "gossip",
            Props::new(move || PubSubGossipActor::new(actor_node)),
        )
        .unwrap();
    let jobs = TopicName::new("jobs");
    let mut remote_registry = PubSubRegistryState::new(node_b.clone());
    remote_registry.register_local_topic(jobs.clone());
    let delta = remote_registry.collect_delta(&BTreeMap::new(), 10);

    gossip
        .tell(PubSubGossipMsg::Delta {
            from: node_b.clone(),
            delta: delta.clone(),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::GetRegistry {
            reply_to: registry_probe.actor_ref(),
        })
        .unwrap();
    assert!(
        registry_probe
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .broadcast_targets(&jobs, true)
            .is_empty()
    );

    gossip
        .tell(PubSubGossipMsg::AddPeer {
            peer: PubSubGossipPeer::new(node_b.clone(), peer_b.actor_ref()),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::Delta {
            from: node_b.clone(),
            delta,
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::RemovePeer {
            node: node_b.clone(),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::GetRegistry {
            reply_to: registry_probe.actor_ref(),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::GetPeers {
            reply_to: peers_probe.actor_ref(),
        })
        .unwrap();

    assert!(
        registry_probe
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .broadcast_targets(&jobs, true)
            .is_empty()
    );
    assert!(
        peers_probe
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .is_empty()
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
