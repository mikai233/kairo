use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{
    CrdtDataCodec, DeltaPropagation, DeltaPropagationLog, DeltaPropagationTransport,
    DeltaTransportReport, ReplicaId, ReplicatedData,
};

const DEFAULT_CLEANUP_EVERY_TICKS: u64 = 5;

pub trait DeltaPropagationSink<Delta>: Send + Sync
where
    Delta: ReplicatedData + Send + 'static,
{
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
    pub fn new(sink: impl DeltaPropagationSink<Delta> + 'static) -> Self {
        Self {
            sink: Arc::new(sink),
            cleanup_every_ticks: DEFAULT_CLEANUP_EVERY_TICKS,
        }
    }

    pub fn from_arc(sink: Arc<dyn DeltaPropagationSink<Delta>>) -> Self {
        Self {
            sink,
            cleanup_every_ticks: DEFAULT_CLEANUP_EVERY_TICKS,
        }
    }

    pub fn with_cleanup_every_ticks(mut self, ticks: u64) -> Self {
        self.cleanup_every_ticks = ticks.max(1);
        self
    }

    pub fn cleanup_every_ticks(&self) -> u64 {
        self.cleanup_every_ticks
    }

    pub fn run_tick(&self, log: &mut DeltaPropagationLog<Delta>) -> DeltaPropagationTickReport {
        let propagations = log.collect_propagations();
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
pub struct DeltaPropagationTickReport {
    propagation_count: u64,
    cleaned_delta_entries: bool,
    transport: DeltaTransportReport,
}

impl DeltaPropagationTickReport {
    pub fn skipped(propagation_count: u64) -> Self {
        Self {
            propagation_count,
            cleaned_delta_entries: false,
            transport: DeltaTransportReport::empty(),
        }
    }

    pub fn propagation_count(&self) -> u64 {
        self.propagation_count
    }

    pub fn cleaned_delta_entries(&self) -> bool {
        self.cleaned_delta_entries
    }

    pub fn transport(&self) -> &DeltaTransportReport {
        &self.transport
    }
}
