use crate::{RemoteError, RemoteStreamId};

pub(super) fn tcp_inbound_failure(peer: &str, error: impl std::error::Error) -> RemoteError {
    RemoteError::Inbound(format!("tcp stream from {peer} failed: {error}"))
}

pub(super) fn missing_lane_error(stream_id: RemoteStreamId) -> RemoteError {
    RemoteError::InvalidFrame(format!(
        "tcp association missing {:?} lane for reverse route",
        stream_id
    ))
}
