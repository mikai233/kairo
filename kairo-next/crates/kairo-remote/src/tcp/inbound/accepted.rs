#![deny(missing_docs)]

use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::association_routes::RemoteAssociationRouteLifecycle;
use crate::tcp::{
    TcpAssociationIdentity, TcpAssociationReaderFailure, TcpAssociationReaderSupervisionDecision,
    TcpAssociationReaderSupervisor,
};
use crate::{
    RemoteAssociationAddress, RemoteAssociationRouteRegistration, RemoteError, RemoteStreamId,
    Result,
};

use super::reports::{TcpAssociationReadReport, TcpAssociationSupervisedReadReport};
use super::stream_reader::TcpAssociationStreamReader;

/// Complete inbound TCP association whose lane streams are ready to be read.
pub struct TcpAcceptedAssociation {
    pub(super) reader: TcpAssociationStreamReader,
    pub(super) remote_identity: Option<TcpAssociationIdentity>,
    pub(super) route_registration: Option<RemoteAssociationRouteRegistration>,
    pub(super) streams: Vec<TcpAcceptedStream>,
}

impl TcpAcceptedAssociation {
    /// Returns the handshaken peer identity, when handshakes were required.
    pub fn remote_identity(&self) -> Option<&TcpAssociationIdentity> {
        self.remote_identity.as_ref()
    }

    /// Returns the handshaken peer address, when available.
    pub fn remote_address(&self) -> Option<&RemoteAssociationAddress> {
        self.remote_identity
            .as_ref()
            .map(TcpAssociationIdentity::address)
    }

    /// Returns the handshaken peer incarnation identifier, when available.
    pub fn remote_uid(&self) -> Option<u64> {
        self.remote_identity
            .as_ref()
            .map(TcpAssociationIdentity::uid)
    }

    /// Returns the number of lane streams in this association.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Reads every lane sequentially to EOF and aggregates their frame counts.
    ///
    /// # Errors
    ///
    /// Returns the first stream read, framing, or frame-handler error.
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

    /// Spawns one blocking reader thread per lane stream.
    pub fn spawn_lane_readers(self) -> TcpAssociationReaderHandle {
        let route_lifecycle = self
            .route_registration
            .as_ref()
            .map(RemoteAssociationRouteRegistration::lifecycle);
        let joins = self
            .streams
            .into_iter()
            .map(|accepted| {
                let reader = self.reader.clone();
                let route_lifecycle = route_lifecycle.clone();
                let peer = accepted.peer.to_string();
                let stream = Arc::new(accepted.stream);
                let reader_stream = Arc::clone(&stream);
                TcpAssociationReaderJoin {
                    stream_id: accepted.stream_id,
                    stream,
                    join: thread::spawn(move || {
                        let result = reader.read_shared_stream(peer, reader_stream);
                        if let Some(lifecycle) = route_lifecycle {
                            lifecycle.close_owned_route(
                                "tcp accepted association lane reader completed",
                            );
                        }
                        result
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

/// Join handle for the reader threads of one TCP association.
pub struct TcpAssociationReaderHandle {
    joins: Vec<TcpAssociationReaderJoin>,
}

struct TcpAssociationReaderJoin {
    stream_id: Option<RemoteStreamId>,
    stream: Arc<TcpStream>,
    join: JoinHandle<Result<TcpAssociationReadReport>>,
}

impl TcpAssociationReaderHandle {
    pub(crate) fn spawn_streams(
        reader: TcpAssociationStreamReader,
        streams: Vec<(String, TcpStream)>,
        route_lifecycle: RemoteAssociationRouteLifecycle,
    ) -> Self {
        let joins = streams
            .into_iter()
            .map(|(peer, stream)| {
                let reader = reader.clone();
                let route_lifecycle = route_lifecycle.clone();
                let stream = Arc::new(stream);
                let reader_stream = Arc::clone(&stream);
                TcpAssociationReaderJoin {
                    stream_id: None,
                    stream,
                    join: thread::spawn(move || {
                        let result = reader.read_shared_stream(peer, reader_stream);
                        route_lifecycle
                            .close_owned_route("tcp dialed association lane reader completed");
                        result
                    }),
                }
            })
            .collect();
        Self { joins }
    }

    /// Joins every reader and returns aggregate read totals.
    ///
    /// # Errors
    ///
    /// Returns an inbound error when a reader fails or panics.
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

    pub(crate) fn is_finished(&self) -> bool {
        self.joins.iter().all(|reader| reader.join.is_finished())
    }

    pub(crate) fn stop(&self) {
        for reader in &self.joins {
            let _ = reader.stream.shutdown(Shutdown::Both);
        }
    }

    /// Shuts down every lane socket, then joins readers with the default supervisor stopped.
    ///
    /// Late reader failures are reported as ignored supervision decisions.
    pub fn join_after_stop(self) -> TcpAssociationSupervisedReadReport {
        self.stop();
        let mut supervisor = TcpAssociationReaderSupervisor::default();
        supervisor.stop();
        self.join_with_supervisor(&mut supervisor)
    }

    /// Shuts down every lane socket and joins stopped readers before `deadline`.
    ///
    /// Returns `None` on timeout.
    pub fn join_after_stop_until(
        self,
        deadline: Instant,
    ) -> Option<TcpAssociationSupervisedReadReport> {
        self.stop();
        let mut supervisor = TcpAssociationReaderSupervisor::default();
        supervisor.stop();
        self.join_with_supervisor_until(&mut supervisor, deadline)
    }

    /// Joins readers and records failures through `supervisor`.
    pub fn join_with_supervisor(
        self,
        supervisor: &mut TcpAssociationReaderSupervisor,
    ) -> TcpAssociationSupervisedReadReport {
        let mut report = TcpAssociationReadReport::default();
        let mut supervision = Vec::new();
        for reader_join in self.joins {
            collect_reader_join(reader_join, supervisor, &mut report, &mut supervision);
        }
        TcpAssociationSupervisedReadReport {
            read: report,
            supervision,
        }
    }

    /// Joins supervised readers before `deadline`, or returns `None` on timeout.
    pub fn join_with_supervisor_until(
        self,
        supervisor: &mut TcpAssociationReaderSupervisor,
        deadline: Instant,
    ) -> Option<TcpAssociationSupervisedReadReport> {
        let mut report = TcpAssociationReadReport::default();
        let mut supervision = Vec::new();
        for reader_join in self.joins {
            while !reader_join.join.is_finished() {
                let now = Instant::now();
                if now >= deadline {
                    return None;
                }
                thread::sleep((deadline - now).min(Duration::from_millis(1)));
            }
            collect_reader_join(reader_join, supervisor, &mut report, &mut supervision);
        }
        Some(TcpAssociationSupervisedReadReport {
            read: report,
            supervision,
        })
    }
}

fn collect_reader_join(
    reader_join: TcpAssociationReaderJoin,
    supervisor: &mut TcpAssociationReaderSupervisor,
    report: &mut TcpAssociationReadReport,
    supervision: &mut Vec<TcpAssociationReaderSupervisionDecision>,
) {
    match reader_join.join.join() {
        Ok(Ok(stream_report)) => {
            report.streams += stream_report.streams;
            report.frames += stream_report.frames;
        }
        Ok(Err(error)) => {
            let reason = error.to_string();
            supervision
                .push(supervisor.record_failure(reader_failure(reader_join.stream_id, reason)));
        }
        Err(_) => {
            let reason = "tcp lane reader panicked".to_string();
            supervision
                .push(supervisor.record_failure(reader_failure(reader_join.stream_id, reason)));
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
