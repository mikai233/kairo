mod dialer;
mod handshake;
mod inbound;
mod sink;
mod supervision;

pub use self::dialer::TcpAssociationDialer;
pub use self::handshake::{
    TcpAssociationHandshake, TcpAssociationIdentity, encode_tcp_association_handshake,
    read_tcp_association_handshake, validate_tcp_association_handshakes,
};
pub use self::inbound::{
    TcpAcceptedAssociation, TcpAssociationListener, TcpAssociationListenerHandle,
    TcpAssociationListenerReport, TcpAssociationReadReport, TcpAssociationReaderHandle,
    TcpAssociationStreamReader, TcpAssociationSupervisedReadReport,
};
pub use self::sink::TcpRemoteByteSink;
pub use self::supervision::{
    TcpAssociationReaderFailure, TcpAssociationReaderRestartSettings,
    TcpAssociationReaderSupervisionDecision, TcpAssociationReaderSupervisor,
};

#[cfg(test)]
mod tests;
