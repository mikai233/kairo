mod accepted;
mod assembly;
mod error;
mod listener;
mod reports;
mod stream_reader;

pub use self::accepted::{TcpAcceptedAssociation, TcpAssociationReaderHandle};
pub use self::assembly::{
    DEFAULT_TCP_LANE_ARRIVAL_TIMEOUT, DEFAULT_TCP_MAX_PENDING_ASSOCIATIONS,
    TcpAssociationAssemblySettings,
};
pub use self::listener::{
    TcpAssociationFrameHandlerFactory, TcpAssociationListener, TcpAssociationListenerHandle,
};
pub use self::reports::{
    TcpAssociationListenerReport, TcpAssociationReadReport, TcpAssociationSupervisedReadReport,
};
pub use self::stream_reader::TcpAssociationStreamReader;

const DEFAULT_EXPECTED_LANE_STREAMS: usize = 3;
const DEFAULT_READ_CHUNK_LEN: usize = 8 * 1024;
