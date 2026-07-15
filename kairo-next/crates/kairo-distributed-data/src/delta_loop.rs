#![deny(missing_docs)]

//! Periodic delta collection, publication, and retained-entry cleanup.
//!
//! One tick first advances the propagation log's collection counter, then
//! publishes the selected per-replica batch, and finally performs scheduled
//! cleanup. Cleanup cadence is independently configurable from the log's peer
//! selection divisor, while both default to five ticks.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{
    CrdtDataCodec, DeltaPropagation, DeltaPropagationLog, DeltaPropagationTransport,
    DeltaTransportReport, PruningTable, ReplicaId, ReplicatedData, ReplicatorKey,
};

const DEFAULT_CLEANUP_EVERY_TICKS: u64 = 5;

/// Batch publication boundary used by the delta propagation loop.
pub trait DeltaPropagationSink<Delta>: Send + Sync
where
    Delta: ReplicatedData + Send + 'static,
{
    /// Publishes the selected per-replica propagations and reports delivery attempts.
    fn publish(
        &self,
        propagations: BTreeMap<ReplicaId, DeltaPropagation<Delta>>,
    ) -> DeltaTransportReport;
}

impl<Delta, Codec> DeltaPropagationSink<Delta> for DeltaPropagationTransport<Codec>
where
    Delta: ReplicatedData + Send + 'static,
    Codec: CrdtDataCodec<Delta> + Send + Sync,
{
    fn publish(
        &self,
        propagations: BTreeMap<ReplicaId, DeltaPropagation<Delta>>,
    ) -> DeltaTransportReport {
        DeltaPropagationTransport::publish(self, propagations)
    }
}

#[derive(Clone)]
/// Reusable driver for one periodic delta propagation tick.
pub struct DeltaPropagationLoop<Delta>
where
    Delta: ReplicatedData + Send + 'static,
{
    sink: Arc<dyn DeltaPropagationSink<Delta>>,
    cleanup_every_ticks: u64,
}

impl<Delta> DeltaPropagationLoop<Delta>
where
    Delta: ReplicatedData + Send + 'static,
{
    /// Creates a loop backed by an owned publication sink.
    pub fn new(sink: impl DeltaPropagationSink<Delta> + 'static) -> Self {
        Self {
            sink: Arc::new(sink),
            cleanup_every_ticks: DEFAULT_CLEANUP_EVERY_TICKS,
        }
    }

    /// Creates a loop backed by a shared publication sink.
    pub fn from_arc(sink: Arc<dyn DeltaPropagationSink<Delta>>) -> Self {
        Self {
            sink,
            cleanup_every_ticks: DEFAULT_CLEANUP_EVERY_TICKS,
        }
    }

    /// Sets the number of collection attempts between retained-entry cleanups.
    ///
    /// The value is clamped to at least one. This cadence is intentionally
    /// independent from [`DeltaPropagationLog::with_gossip_interval_divisor`].
    pub fn with_cleanup_every_ticks(mut self, ticks: u64) -> Self {
        self.cleanup_every_ticks = ticks.max(1);
        self
    }

    /// Returns the configured retained-entry cleanup cadence in ticks.
    pub fn cleanup_every_ticks(&self) -> u64 {
        self.cleanup_every_ticks
    }

    /// Collects, publishes, and conditionally cleans one propagation tick.
    ///
    /// Collection advances the propagation count even when there are no target
    /// replicas or delta payloads. The selected batch, including an empty one,
    /// is passed to the sink before cleanup is considered.
    pub fn run_tick(&self, log: &mut DeltaPropagationLog<Delta>) -> DeltaPropagationTickReport {
        self.run_tick_with_pruning(log, |_| PruningTable::new())
    }

    /// Runs one tick and attaches each key's current pruning metadata.
    ///
    /// The resolver is evaluated after collection and before publication. A
    /// composed replicator supplies pruning from its current full-state
    /// envelope so a delta never outruns removed-replica lifecycle markers.
    pub fn run_tick_with_pruning(
        &self,
        log: &mut DeltaPropagationLog<Delta>,
        mut pruning_for_key: impl FnMut(&ReplicatorKey) -> PruningTable,
    ) -> DeltaPropagationTickReport {
        let mut propagations = log.collect_propagations();
        for propagation in propagations.values_mut() {
            propagation.attach_pruning(&mut pruning_for_key);
        }
        let transport = self.sink.publish(propagations);
        let propagation_count = log.propagation_count();
        let cleaned_delta_entries = propagation_count.is_multiple_of(self.cleanup_every_ticks);
        if cleaned_delta_entries {
            log.cleanup_delta_entries();
        }
        DeltaPropagationTickReport {
            propagation_count,
            cleaned_delta_entries,
            transport,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Observable outcome of one requested delta propagation tick.
pub struct DeltaPropagationTickReport {
    propagation_count: u64,
    cleaned_delta_entries: bool,
    transport: DeltaTransportReport,
}

impl DeltaPropagationTickReport {
    /// Creates a report for an actor that has no propagation loop configured.
    ///
    /// A skipped tick neither publishes nor advances the supplied diagnostic
    /// propagation count.
    pub fn skipped(propagation_count: u64) -> Self {
        Self {
            propagation_count,
            cleaned_delta_entries: false,
            transport: DeltaTransportReport::empty(),
        }
    }

    /// Returns the log collection count after this tick, or at skip time.
    pub fn propagation_count(&self) -> u64 {
        self.propagation_count
    }

    /// Reports whether retained delta entries were cleaned on this tick.
    pub fn cleaned_delta_entries(&self) -> bool {
        self.cleaned_delta_entries
    }

    /// Returns delivery diagnostics produced by the publication sink.
    pub fn transport(&self) -> &DeltaTransportReport {
        &self.transport
    }
}
