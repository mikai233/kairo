use crate::tcp::{TcpAssociationIdentity, TcpAssociationReaderSupervisionDecision};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TcpAssociationReadReport {
    pub streams: usize,
    pub frames: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TcpAssociationSupervisedReadReport {
    pub read: TcpAssociationReadReport,
    pub supervision: Vec<TcpAssociationReaderSupervisionDecision>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpAssociationListenerReport {
    pub accepted_associations: usize,
    pub remote_identities: Vec<TcpAssociationIdentity>,
    pub read: TcpAssociationReadReport,
    pub supervision: Vec<TcpAssociationReaderSupervisionDecision>,
}
