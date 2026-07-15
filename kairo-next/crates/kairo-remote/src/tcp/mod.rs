mod dialer;
mod handshake;
mod inbound;
mod sink;
mod supervision;

pub use self::dialer::TcpAssociationDialer;
pub use self::handshake::{
    DEFAULT_TCP_HANDSHAKE_MAX_PAYLOAD_BYTES, DEFAULT_TCP_HANDSHAKE_READ_TIMEOUT,
    TcpAssociationHandshake, TcpAssociationIdentity, TcpHandshakeReadSettings,
    encode_tcp_association_handshake, read_tcp_association_handshake_with_limit,
    validate_tcp_association_handshakes,
};
pub use self::inbound::{
    TcpAcceptedAssociation, TcpAssociationFrameHandlerFactory, TcpAssociationListener,
    TcpAssociationListenerHandle, TcpAssociationListenerReport, TcpAssociationReadReport,
    TcpAssociationReaderHandle, TcpAssociationStreamReader, TcpAssociationSupervisedReadReport,
};
pub use self::sink::TcpRemoteByteSink;
pub use self::supervision::{
    TcpAssociationReaderFailure, TcpAssociationReaderRestartSettings,
    TcpAssociationReaderSupervisionDecision, TcpAssociationReaderSupervisor,
};

#[cfg(test)]
mod tests;
