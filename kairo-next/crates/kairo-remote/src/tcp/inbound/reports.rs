#![deny(missing_docs)]

use crate::tcp::{TcpAssociationIdentity, TcpAssociationReaderSupervisionDecision};

/// Counts streams and frames consumed from one or more TCP associations.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TcpAssociationReadReport {
    /// Number of streams read to completion.
    pub streams: usize,
    /// Number of decoded remote frames dispatched to handlers.
    pub frames: usize,
}

/// Read totals paired with any lane-reader supervision decisions.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TcpAssociationSupervisedReadReport {
    /// Aggregate stream and frame totals.
    pub read: TcpAssociationReadReport,
    /// Decisions recorded for failed or stopped lane readers.
    pub supervision: Vec<TcpAssociationReaderSupervisionDecision>,
}

/// Final report returned by a stopped TCP association accept loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpAssociationListenerReport {
    /// Number of complete associations accepted by the loop.
    pub accepted_associations: usize,
    /// Handshaken peer identities, in acceptance order.
    pub remote_identities: Vec<TcpAssociationIdentity>,
    /// Aggregate stream and frame totals from joined lane readers.
    pub read: TcpAssociationReadReport,
    /// Decisions recorded for failed or stopped lane readers.
    pub supervision: Vec<TcpAssociationReaderSupervisionDecision>,
}
