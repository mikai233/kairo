//! Backend-neutral observability adapters.

use std::sync::atomic::{AtomicU64, Ordering};

/// Dependency-free counters for configured diagnostic categories.
///
/// The counters implement the remote and cluster diagnostic observer traits
/// when those facade features are enabled. Applications can periodically export
/// [`DiagnosticCounterSnapshot`] values to their logging or metrics backend of
/// choice without Kairo selecting that backend.
#[derive(Debug, Default)]
pub struct DiagnosticCounters {
    remote_serialization_failures: AtomicU64,
    remote_delivery_failures: AtomicU64,
    association_quarantine_events: AtomicU64,
    association_close_events: AtomicU64,
    cluster_gossip_state_changes: AtomicU64,
}

/// A point-in-time view of [`DiagnosticCounters`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiagnosticCounterSnapshot {
    pub remote_serialization_failures: u64,
    pub remote_delivery_failures: u64,
    pub association_quarantine_events: u64,
    pub association_close_events: u64,
    pub cluster_gossip_state_changes: u64,
}

impl DiagnosticCounters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> DiagnosticCounterSnapshot {
        DiagnosticCounterSnapshot {
            remote_serialization_failures: self
                .remote_serialization_failures
                .load(Ordering::Relaxed),
            remote_delivery_failures: self.remote_delivery_failures.load(Ordering::Relaxed),
            association_quarantine_events: self
                .association_quarantine_events
                .load(Ordering::Relaxed),
            association_close_events: self.association_close_events.load(Ordering::Relaxed),
            cluster_gossip_state_changes: self.cluster_gossip_state_changes.load(Ordering::Relaxed),
        }
    }

    #[cfg(any(feature = "remote", feature = "cluster"))]
    fn increment(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(feature = "remote")]
impl kairo_remote::RemoteInboundDiagnostics for DiagnosticCounters {
    fn record(&self, diagnostic: kairo_remote::RemoteInboundDiagnostic) {
        match diagnostic {
            kairo_remote::RemoteInboundDiagnostic::SerializationFailure { .. } => {
                Self::increment(&self.remote_serialization_failures);
            }
            kairo_remote::RemoteInboundDiagnostic::DeliveryFailure { .. } => {
                Self::increment(&self.remote_delivery_failures);
            }
        }
    }
}

#[cfg(feature = "remote")]
impl kairo_remote::RemoteAssociationDiagnostics for DiagnosticCounters {
    fn record(&self, diagnostic: kairo_remote::RemoteAssociationDiagnostic) {
        match diagnostic {
            kairo_remote::RemoteAssociationDiagnostic::Quarantined { .. } => {
                Self::increment(&self.association_quarantine_events);
            }
            kairo_remote::RemoteAssociationDiagnostic::Closed { .. } => {
                Self::increment(&self.association_close_events);
            }
        }
    }
}

#[cfg(feature = "cluster")]
impl kairo_cluster::ClusterDiagnostics for DiagnosticCounters {
    fn record(&self, diagnostic: kairo_cluster::ClusterDiagnostic) {
        match diagnostic {
            kairo_cluster::ClusterDiagnostic::GossipStateChanged { .. } => {
                Self::increment(&self.cluster_gossip_state_changes);
            }
        }
    }
}
