#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::sync::Arc;

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Recipient};
use kairo_cluster::UniqueAddress;

use crate::{PubSubRegistryDelta, PubSubRegistryState, TopicName};

type PubSubGossipRecipient = Arc<dyn Recipient<PubSubGossipMsg> + Send + Sync>;

const DEFAULT_MAX_DELTA_ENTRIES: usize = 1000;

/// Transport-neutral gossip destination for one exact peer incarnation.
///
/// The recipient may be a local actor ref or a remote wire adapter; membership
/// code must add and remove peers explicitly because transport reachability is
/// not a source of cluster truth.
#[derive(Clone)]
pub struct PubSubGossipPeer {
    node: UniqueAddress,
    recipient: PubSubGossipRecipient,
}

impl PubSubGossipPeer {
    /// Creates a peer from any typed gossip recipient.
    pub fn new(
        node: UniqueAddress,
        recipient: impl Recipient<PubSubGossipMsg> + Send + Sync + 'static,
    ) -> Self {
        Self {
            node,
            recipient: Arc::new(recipient),
        }
    }

    /// Creates a peer from an already type-erased shared recipient.
    pub fn from_arc(node: UniqueAddress, recipient: PubSubGossipRecipient) -> Self {
        Self { node, recipient }
    }

    /// Returns the exact peer cluster incarnation.
    pub fn node(&self) -> &UniqueAddress {
        &self.node
    }
}

/// Actor protocol for version-vector-style pubsub registry gossip.
pub enum PubSubGossipMsg {
    /// Adds or replaces the recipient for one membership-approved peer.
    AddPeer {
        /// Peer identity and transport-neutral recipient.
        peer: PubSubGossipPeer,
    },
    /// Removes a peer and its registry bucket.
    RemovePeer {
        /// Exact peer incarnation to remove.
        node: UniqueAddress,
    },
    /// Records local direct-topic presence.
    RegisterTopic {
        /// Topic to advertise.
        topic: TopicName,
    },
    /// Records a local direct-topic removal tombstone.
    UnregisterTopic {
        /// Topic to stop advertising directly.
        topic: TopicName,
    },
    /// Records local named-group presence.
    RegisterGroup {
        /// Topic containing the group.
        topic: TopicName,
        /// Group to advertise.
        group: String,
    },
    /// Records a local named-group removal tombstone.
    UnregisterGroup {
        /// Topic containing the group.
        topic: TopicName,
        /// Group to stop advertising.
        group: String,
    },
    /// Records local logical-path presence.
    RegisterPath {
        /// Address-independent path to advertise.
        path: String,
    },
    /// Records a local logical-path removal tombstone.
    UnregisterPath {
        /// Address-independent path to stop advertising.
        path: String,
    },
    /// Sets the actor that receives accepted remote deltas.
    ///
    /// The composed mediator uses this to keep delivery routing synchronized
    /// with the gossip actor's registry copy.
    SetDeltaSink {
        /// Sink for known-node, non-empty deltas after filtering.
        sink: ActorRef<PubSubRegistryDelta>,
    },
    /// Selects one peer round-robin and sends the local version status.
    GossipTick,
    /// Advertises known owner versions and requests missing updates.
    Status {
        /// Exact sending peer incarnation.
        from: UniqueAddress,
        /// Highest bucket version known by the sender for each owner key.
        versions: BTreeMap<String, u64>,
        /// Whether this status is the one-shot reply to a peer's status.
        reply: bool,
    },
    /// Supplies bounded registry updates to a known peer.
    Delta {
        /// Exact sending peer incarnation.
        from: UniqueAddress,
        /// Node-owned bucket updates.
        delta: PubSubRegistryDelta,
    },
    /// Returns a clone of the current registry state.
    GetRegistry {
        /// Recipient for the registry snapshot.
        reply_to: ActorRef<PubSubRegistryState>,
    },
    /// Returns the number of accepted non-empty remote delta batches.
    GetDeltaCount {
        /// Recipient for the monotonic count.
        reply_to: ActorRef<u64>,
    },
    /// Returns current peer incarnations in deterministic order.
    GetPeers {
        /// Recipient for the peer snapshot.
        reply_to: ActorRef<Vec<UniqueAddress>>,
    },
}

/// Actor that converges pubsub registry buckets with membership-approved peers.
///
/// Each tick contacts one peer in deterministic round-robin order. Status
/// exchange sends bounded deltas in both directions without status ping-pong;
/// incoming deltas are accepted only from known peers and retain buckets only
/// for the local node or currently known peers.
pub struct PubSubGossipActor {
    registry: PubSubRegistryState,
    peers: BTreeMap<String, PubSubGossipPeer>,
    max_delta_entries: usize,
    next_peer_index: usize,
    delta_count: u64,
    delta_sink: Option<ActorRef<PubSubRegistryDelta>>,
}

impl PubSubGossipActor {
    /// Creates gossip state for `self_node` with the default delta limit.
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            registry: PubSubRegistryState::new(self_node),
            peers: BTreeMap::new(),
            max_delta_entries: DEFAULT_MAX_DELTA_ENTRIES,
            next_peer_index: 0,
            delta_count: 0,
            delta_sink: None,
        }
    }

    /// Sets the maximum entries emitted in one delta, clamped to at least one.
    pub fn with_max_delta_entries(mut self, max_delta_entries: usize) -> Self {
        self.max_delta_entries = max_delta_entries.max(1);
        self
    }

    /// Returns the maximum entries emitted in one delta.
    pub fn max_delta_entries(&self) -> usize {
        self.max_delta_entries
    }

    /// Returns the actor's current registry state.
    pub fn registry(&self) -> &PubSubRegistryState {
        &self.registry
    }

    /// Returns the number of membership-approved peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Returns the number of accepted non-empty remote delta batches.
    pub fn delta_count(&self) -> u64 {
        self.delta_count
    }

    fn add_peer(&mut self, peer: PubSubGossipPeer) {
        if peer.node.address == self.registry.self_node().address {
            return;
        }
        self.peers.insert(node_key(&peer.node), peer);
    }

    fn remove_peer(&mut self, node: &UniqueAddress) {
        self.peers.remove(&node_key(node));
        self.registry.remove_node(node);
        if !self.peers.is_empty() {
            self.next_peer_index %= self.peers.len();
        } else {
            self.next_peer_index = 0;
        }
    }

    fn gossip_tick(&mut self) {
        let Some(peer) = self.select_peer() else {
            return;
        };
        let _ = peer.recipient.tell(PubSubGossipMsg::Status {
            from: self.registry.self_node().clone(),
            versions: self.registry.versions(),
            reply: false,
        });
    }

    fn handle_status(&mut self, from: UniqueAddress, versions: BTreeMap<String, u64>, reply: bool) {
        let Some(peer) = self.peer(&from) else {
            return;
        };

        let delta = self
            .registry
            .collect_delta(&versions, self.max_delta_entries);
        if !delta.buckets.is_empty() {
            let _ = peer.recipient.tell(PubSubGossipMsg::Delta {
                from: self.registry.self_node().clone(),
                delta,
            });
        }

        if !reply && self.other_has_newer_versions(&versions) {
            let _ = peer.recipient.tell(PubSubGossipMsg::Status {
                from: self.registry.self_node().clone(),
                versions: self.registry.versions(),
                reply: true,
            });
        }
    }

    fn handle_delta(&mut self, from: UniqueAddress, delta: PubSubRegistryDelta) {
        if self.peer(&from).is_none() {
            return;
        }
        let known_delta = PubSubRegistryDelta {
            buckets: delta
                .buckets
                .into_iter()
                .filter(|bucket| self.is_known_node(&bucket.owner))
                .collect(),
        };
        if !known_delta.buckets.is_empty() {
            self.delta_count += 1;
            self.registry.merge_delta(known_delta.clone());
            if let Some(sink) = &self.delta_sink {
                let _ = sink.tell(known_delta);
            }
        }
    }

    fn select_peer(&mut self) -> Option<PubSubGossipPeer> {
        if self.peers.is_empty() {
            return None;
        }
        let index = self.next_peer_index % self.peers.len();
        self.next_peer_index = (index + 1) % self.peers.len();
        self.peers.values().nth(index).cloned()
    }

    fn peer(&self, node: &UniqueAddress) -> Option<&PubSubGossipPeer> {
        self.peers.get(&node_key(node))
    }

    fn is_known_node(&self, node: &UniqueAddress) -> bool {
        node == self.registry.self_node() || self.peers.contains_key(&node_key(node))
    }

    fn other_has_newer_versions(&self, versions: &BTreeMap<String, u64>) -> bool {
        let local_versions = self.registry.versions();
        versions.iter().any(|(owner, version)| {
            self.is_known_node_key(owner)
                && *version > local_versions.get(owner).copied().unwrap_or(0)
        })
    }

    fn is_known_node_key(&self, owner: &str) -> bool {
        owner == self.registry.self_node().ordering_key() || self.peers.contains_key(owner)
    }
}

impl Actor for PubSubGossipActor {
    type Msg = PubSubGossipMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            PubSubGossipMsg::AddPeer { peer } => self.add_peer(peer),
            PubSubGossipMsg::RemovePeer { node } => self.remove_peer(&node),
            PubSubGossipMsg::RegisterTopic { topic } => {
                self.registry.register_local_topic(topic);
            }
            PubSubGossipMsg::UnregisterTopic { topic } => {
                self.registry.unregister_local_topic(topic);
            }
            PubSubGossipMsg::RegisterGroup { topic, group } => {
                self.registry.register_local_group(topic, group);
            }
            PubSubGossipMsg::UnregisterGroup { topic, group } => {
                self.registry.unregister_local_group(topic, group);
            }
            PubSubGossipMsg::RegisterPath { path } => {
                self.registry.register_local_path(path);
            }
            PubSubGossipMsg::UnregisterPath { path } => {
                self.registry.unregister_local_path(path);
            }
            PubSubGossipMsg::SetDeltaSink { sink } => self.delta_sink = Some(sink),
            PubSubGossipMsg::GossipTick => self.gossip_tick(),
            PubSubGossipMsg::Status {
                from,
                versions,
                reply,
            } => self.handle_status(from, versions, reply),
            PubSubGossipMsg::Delta { from, delta } => self.handle_delta(from, delta),
            PubSubGossipMsg::GetRegistry { reply_to } => {
                let _ = reply_to.tell(self.registry.clone());
            }
            PubSubGossipMsg::GetDeltaCount { reply_to } => {
                let _ = reply_to.tell(self.delta_count);
            }
            PubSubGossipMsg::GetPeers { reply_to } => {
                let _ = reply_to.tell(self.peers.values().map(|peer| peer.node.clone()).collect());
            }
        }
        Ok(())
    }
}

fn node_key(node: &UniqueAddress) -> String {
    node.ordering_key()
}
