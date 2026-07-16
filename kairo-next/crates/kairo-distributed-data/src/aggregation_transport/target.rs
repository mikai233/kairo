use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use kairo_actor::{Recipient, SendError};
use kairo_serialization::ActorRefWireData;

use crate::{ReplicaId, ReplicatorRead, ReplicatorRemoteEnvelopeOutbound, ReplicatorWrite};

pub(super) type WriteRecipient = Arc<dyn Recipient<ReplicatorWrite> + Send + Sync>;
pub(super) type ReadRecipient = Arc<dyn Recipient<ReplicatorRead> + Send + Sync>;
pub(super) type SenderAwareWriteRecipient = Arc<dyn SenderAwareRecipient<ReplicatorWrite>>;
pub(super) type SenderAwareReadRecipient = Arc<dyn SenderAwareRecipient<ReplicatorRead>>;

/// Recipient extension that preserves the aggregation actor's stable wire identity.
///
/// Remote targets use the sender to route acknowledgements and read results
/// directly to the child aggregation actor. Targets without this extension can
/// still receive ordinary sender-less requests.
pub trait SenderAwareRecipient<M: Send + 'static>: Send + Sync {
    /// Delivers `message` while attaching `sender` as its reply destination.
    fn tell_with_sender(&self, message: M, sender: &ActorRefWireData) -> Result<(), SendError<M>>;
}

/// Shared, thread-safe mapping from replica identities to delivery targets.
///
/// Clones observe the same target set, allowing membership-derived route
/// updates to become visible to already-running aggregation sessions.
#[derive(Clone, Default)]
pub struct AggregationTargetRegistry {
    targets: Arc<RwLock<BTreeMap<ReplicaId, AggregationTarget>>>,
}

impl AggregationTargetRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically replaces every registered target.
    pub fn set_targets(&self, targets: impl IntoIterator<Item = AggregationTarget>) {
        let mut guard = self.targets.write().expect("aggregation targets poisoned");
        *guard = targets
            .into_iter()
            .map(|target| (target.replica.clone(), target))
            .collect();
    }

    /// Inserts or replaces the target for its replica identity.
    pub fn insert_target(&self, target: AggregationTarget) {
        self.targets
            .write()
            .expect("aggregation targets poisoned")
            .insert(target.replica.clone(), target);
    }

    /// Removes the target for `replica`, if present.
    pub fn remove_target(&self, replica: &ReplicaId) {
        self.targets
            .write()
            .expect("aggregation targets poisoned")
            .remove(replica);
    }

    /// Returns the number of registered replica targets.
    pub fn target_count(&self) -> usize {
        self.targets
            .read()
            .expect("aggregation targets poisoned")
            .len()
    }

    pub(super) fn get(&self, replica: &ReplicaId) -> Option<AggregationTarget> {
        self.targets
            .read()
            .expect("aggregation targets poisoned")
            .get(replica)
            .cloned()
    }
}

/// Read and write recipients associated with one remote replica.
///
/// The ordinary recipients are always present. Optional sender-aware
/// recipients are preferred when a publishing call supplies a sender.
#[derive(Clone)]
pub struct AggregationTarget {
    pub(super) replica: ReplicaId,
    pub(super) write_recipient: WriteRecipient,
    pub(super) read_recipient: ReadRecipient,
    pub(super) sender_aware_write_recipient: Option<SenderAwareWriteRecipient>,
    pub(super) sender_aware_read_recipient: Option<SenderAwareReadRecipient>,
}

impl AggregationTarget {
    /// Creates a target that accepts sender-less write and read requests.
    pub fn new(
        replica: ReplicaId,
        write_recipient: impl Recipient<ReplicatorWrite> + Send + Sync + 'static,
        read_recipient: impl Recipient<ReplicatorRead> + Send + Sync + 'static,
    ) -> Self {
        Self {
            replica,
            write_recipient: Arc::new(write_recipient),
            read_recipient: Arc::new(read_recipient),
            sender_aware_write_recipient: None,
            sender_aware_read_recipient: None,
        }
    }

    /// Creates a target with both ordinary and sender-aware recipients.
    pub fn new_sender_aware(
        replica: ReplicaId,
        write_recipient: impl Recipient<ReplicatorWrite> + Send + Sync + 'static,
        read_recipient: impl Recipient<ReplicatorRead> + Send + Sync + 'static,
        sender_aware_write_recipient: impl SenderAwareRecipient<ReplicatorWrite> + 'static,
        sender_aware_read_recipient: impl SenderAwareRecipient<ReplicatorRead> + 'static,
    ) -> Self {
        Self {
            replica,
            write_recipient: Arc::new(write_recipient),
            read_recipient: Arc::new(read_recipient),
            sender_aware_write_recipient: Some(Arc::new(sender_aware_write_recipient)),
            sender_aware_read_recipient: Some(Arc::new(sender_aware_read_recipient)),
        }
    }

    /// Creates a sender-aware target backed by remote-envelope outbound routes.
    pub fn remote_envelope(
        replica: ReplicaId,
        write_recipient: ReplicatorRemoteEnvelopeOutbound,
        read_recipient: ReplicatorRemoteEnvelopeOutbound,
    ) -> Self {
        Self::new_sender_aware(
            replica,
            write_recipient.clone(),
            read_recipient.clone(),
            write_recipient,
            read_recipient,
        )
    }

    /// Replaces the sender-aware write recipient.
    pub fn with_sender_aware_write(
        mut self,
        recipient: impl SenderAwareRecipient<ReplicatorWrite> + 'static,
    ) -> Self {
        self.sender_aware_write_recipient = Some(Arc::new(recipient));
        self
    }

    /// Replaces the sender-aware read recipient.
    pub fn with_sender_aware_read(
        mut self,
        recipient: impl SenderAwareRecipient<ReplicatorRead> + 'static,
    ) -> Self {
        self.sender_aware_read_recipient = Some(Arc::new(recipient));
        self
    }

    /// Returns whether sender-aware write delivery is available.
    pub fn supports_sender_aware_write(&self) -> bool {
        self.sender_aware_write_recipient.is_some()
    }

    /// Returns whether sender-aware read delivery is available.
    pub fn supports_sender_aware_read(&self) -> bool {
        self.sender_aware_read_recipient.is_some()
    }

    /// Returns whether both request kinds support sender-aware delivery.
    pub fn sender_aware(&self) -> bool {
        self.supports_sender_aware_write() && self.supports_sender_aware_read()
    }

    /// Creates a target from shared recipient trait objects.
    ///
    /// This constructor preserves optional sender-aware recipients and is
    /// useful when route ownership already stores erased recipients.
    pub fn from_arcs_with_sender_aware(
        replica: ReplicaId,
        write_recipient: WriteRecipient,
        read_recipient: ReadRecipient,
        sender_aware_write_recipient: Option<SenderAwareWriteRecipient>,
        sender_aware_read_recipient: Option<SenderAwareReadRecipient>,
    ) -> Self {
        Self {
            replica,
            write_recipient,
            read_recipient,
            sender_aware_write_recipient,
            sender_aware_read_recipient,
        }
    }

    /// Creates a sender-less target from shared recipient trait objects.
    pub fn from_arcs(
        replica: ReplicaId,
        write_recipient: WriteRecipient,
        read_recipient: ReadRecipient,
    ) -> Self {
        Self {
            replica,
            write_recipient,
            read_recipient,
            sender_aware_write_recipient: None,
            sender_aware_read_recipient: None,
        }
    }

    /// Returns the replica identity resolved by this target.
    pub fn replica(&self) -> &ReplicaId {
        &self.replica
    }
}

impl SenderAwareRecipient<ReplicatorWrite> for ReplicatorRemoteEnvelopeOutbound {
    fn tell_with_sender(
        &self,
        message: ReplicatorWrite,
        sender: &ActorRefWireData,
    ) -> Result<(), SendError<ReplicatorWrite>> {
        self.clone().with_sender(Some(sender.clone())).tell(message)
    }
}

impl SenderAwareRecipient<ReplicatorRead> for ReplicatorRemoteEnvelopeOutbound {
    fn tell_with_sender(
        &self,
        message: ReplicatorRead,
        sender: &ActorRefWireData,
    ) -> Result<(), SendError<ReplicatorRead>> {
        self.clone().with_sender(Some(sender.clone())).tell(message)
    }
}
