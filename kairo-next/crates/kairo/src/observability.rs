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

/// Dependency-free text exporter for configured diagnostic categories.
///
/// The sink receives one stable, single-line string per diagnostic event. Use a
/// closure to bridge those lines into `log`, `tracing`, stdout/stderr, a file,
/// or a test collector without making Kairo depend on any of those backends.
#[derive(Debug)]
pub struct DiagnosticTextSink<F> {
    sink: F,
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

impl<F> DiagnosticTextSink<F>
where
    F: Fn(String) + Send + Sync + 'static,
{
    pub fn new(sink: F) -> Self {
        Self { sink }
    }

    pub fn into_inner(self) -> F {
        self.sink
    }

    #[cfg(any(feature = "remote", feature = "cluster"))]
    fn emit(&self, line: String) {
        (self.sink)(line);
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
impl<F> kairo_remote::RemoteInboundDiagnostics for DiagnosticTextSink<F>
where
    F: Fn(String) + Send + Sync + 'static,
{
    fn record(&self, diagnostic: kairo_remote::RemoteInboundDiagnostic) {
        match diagnostic {
            kairo_remote::RemoteInboundDiagnostic::SerializationFailure {
                recipient,
                sender,
                serializer_id,
                manifest,
                version,
                reason,
            } => self.emit(format!(
                "remote.serialization_failure recipient={} sender={} serializer_id={} manifest={} version={} reason={}",
                recipient.path(),
                sender.as_ref().map_or("-", |sender| sender.path()),
                serializer_id,
                manifest,
                version,
                reason
            )),
            kairo_remote::RemoteInboundDiagnostic::DeliveryFailure {
                recipient,
                sender,
                reason,
            } => self.emit(format!(
                "remote.delivery_failure recipient={} sender={} reason={}",
                recipient.path(),
                sender.as_ref().map_or("-", |sender| sender.path()),
                reason
            )),
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

#[cfg(feature = "remote")]
impl<F> kairo_remote::RemoteAssociationDiagnostics for DiagnosticTextSink<F>
where
    F: Fn(String) + Send + Sync + 'static,
{
    fn record(&self, diagnostic: kairo_remote::RemoteAssociationDiagnostic) {
        match diagnostic {
            kairo_remote::RemoteAssociationDiagnostic::Quarantined {
                remote,
                remote_uid,
                reason,
            } => self.emit(format!(
                "remote.association_quarantined remote={} remote_uid={} reason={}",
                remote,
                remote_uid.map_or_else(|| "-".to_string(), |uid| uid.to_string()),
                reason
            )),
            kairo_remote::RemoteAssociationDiagnostic::Closed { remote, reason } => {
                self.emit(format!(
                    "remote.association_closed remote={} reason={}",
                    remote, reason
                ))
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

#[cfg(feature = "cluster")]
impl<F> kairo_cluster::ClusterDiagnostics for DiagnosticTextSink<F>
where
    F: Fn(String) + Send + Sync + 'static,
{
    fn record(&self, diagnostic: kairo_cluster::ClusterDiagnostic) {
        match diagnostic {
            kairo_cluster::ClusterDiagnostic::GossipStateChanged {
                previous,
                current,
                events,
            } => self.emit(format!(
                "cluster.gossip_state_changed previous_members={} current_members={} events={}",
                previous.members().len(),
                current.members().len(),
                events.len()
            )),
        }
    }
}
