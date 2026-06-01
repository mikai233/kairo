use std::net::{SocketAddr, TcpStream};
use std::thread::{self, JoinHandle};

use crate::tcp::{
    TcpAssociationIdentity, TcpAssociationReaderFailure, TcpAssociationReaderSupervisionDecision,
    TcpAssociationReaderSupervisor,
};
use crate::{RemoteAssociationAddress, RemoteError, RemoteStreamId, Result};

use super::reports::{TcpAssociationReadReport, TcpAssociationSupervisedReadReport};
use super::stream_reader::TcpAssociationStreamReader;

pub struct TcpAcceptedAssociation {
    pub(super) reader: TcpAssociationStreamReader,
    pub(super) remote_identity: Option<TcpAssociationIdentity>,
    pub(super) streams: Vec<TcpAcceptedStream>,
}

impl TcpAcceptedAssociation {
    pub fn remote_identity(&self) -> Option<&TcpAssociationIdentity> {
        self.remote_identity.as_ref()
    }

    pub fn remote_address(&self) -> Option<&RemoteAssociationAddress> {
        self.remote_identity
            .as_ref()
            .map(TcpAssociationIdentity::address)
    }

    pub fn remote_uid(&self) -> Option<u64> {
        self.remote_identity
            .as_ref()
            .map(TcpAssociationIdentity::uid)
    }

    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    pub fn drain(self) -> Result<TcpAssociationReadReport> {
        let mut report = TcpAssociationReadReport::default();
        for accepted in self.streams {
            let stream_report = self
                .reader
                .read_stream(accepted.peer.to_string(), accepted.stream)?;
            report.streams += stream_report.streams;
            report.frames += stream_report.frames;
        }
        Ok(report)
    }

    pub fn spawn_lane_readers(self) -> TcpAssociationReaderHandle {
        let joins = self
            .streams
            .into_iter()
            .map(|accepted| {
                let reader = self.reader.clone();
                TcpAssociationReaderJoin {
                    stream_id: accepted.stream_id,
                    join: thread::spawn(move || {
                        reader.read_stream(accepted.peer.to_string(), accepted.stream)
                    }),
                }
            })
            .collect();
        TcpAssociationReaderHandle { joins }
    }
}

pub(super) struct TcpAcceptedStream {
    pub(super) peer: SocketAddr,
    pub(super) stream_id: Option<RemoteStreamId>,
    pub(super) stream: TcpStream,
}

pub struct TcpAssociationReaderHandle {
    joins: Vec<TcpAssociationReaderJoin>,
}

struct TcpAssociationReaderJoin {
    stream_id: Option<RemoteStreamId>,
    join: JoinHandle<Result<TcpAssociationReadReport>>,
}

impl TcpAssociationReaderHandle {
    pub(crate) fn spawn_streams(
        reader: TcpAssociationStreamReader,
        streams: Vec<(String, TcpStream)>,
    ) -> Self {
        let joins = streams
            .into_iter()
            .map(|(peer, stream)| {
                let reader = reader.clone();
                TcpAssociationReaderJoin {
                    stream_id: None,
                    join: thread::spawn(move || reader.read_stream(peer, stream)),
                }
            })
            .collect();
        Self { joins }
    }

    pub fn join(self) -> Result<TcpAssociationReadReport> {
        let mut supervisor = TcpAssociationReaderSupervisor::default();
        let report = self.join_with_supervisor(&mut supervisor);
        if let Some(decision) = report.supervision.first() {
            return Err(RemoteError::Inbound(format!(
                "tcp association reader failed: {}",
                reader_decision_reason(decision)
            )));
        }
        Ok(report.read)
    }

    pub fn join_with_supervisor(
        self,
        supervisor: &mut TcpAssociationReaderSupervisor,
    ) -> TcpAssociationSupervisedReadReport {
        let mut report = TcpAssociationReadReport::default();
        let mut supervision = Vec::new();
        for reader_join in self.joins {
            match reader_join.join.join() {
                Ok(Ok(stream_report)) => {
                    report.streams += stream_report.streams;
                    report.frames += stream_report.frames;
                }
                Ok(Err(error)) => {
                    let reason = error.to_string();
                    supervision.push(
                        supervisor.record_failure(reader_failure(reader_join.stream_id, reason)),
                    );
                }
                Err(_) => {
                    let reason = "tcp lane reader panicked".to_string();
                    supervision.push(
                        supervisor.record_failure(reader_failure(reader_join.stream_id, reason)),
                    );
                }
            }
        }
        TcpAssociationSupervisedReadReport {
            read: report,
            supervision,
        }
    }
}

fn reader_failure(
    stream_id: Option<RemoteStreamId>,
    reason: String,
) -> TcpAssociationReaderFailure {
    match stream_id {
        Some(stream_id) => TcpAssociationReaderFailure::lane(stream_id, reason),
        None => TcpAssociationReaderFailure::association(reason),
    }
}

fn reader_decision_reason(decision: &TcpAssociationReaderSupervisionDecision) -> &str {
    match decision {
        TcpAssociationReaderSupervisionDecision::RestartInboundStreams { failure, .. }
        | TcpAssociationReaderSupervisionDecision::StopInboundStreams { failure, .. }
        | TcpAssociationReaderSupervisionDecision::IgnoreWhileStopped { failure } => {
            failure.reason()
        }
    }
}
