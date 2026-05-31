use crate::RemoteStreamId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpAssociationReaderRestartSettings {
    max_restarts: Option<usize>,
}

impl TcpAssociationReaderRestartSettings {
    pub fn unlimited() -> Self {
        Self { max_restarts: None }
    }

    pub fn max_restarts(max_restarts: usize) -> Self {
        Self {
            max_restarts: Some(max_restarts),
        }
    }

    pub fn max_restarts_limit(&self) -> Option<usize> {
        self.max_restarts
    }
}

impl Default for TcpAssociationReaderRestartSettings {
    fn default() -> Self {
        Self::unlimited()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpAssociationReaderFailure {
    Lane {
        stream_id: RemoteStreamId,
        reason: String,
    },
    Association {
        reason: String,
    },
}

impl TcpAssociationReaderFailure {
    pub fn lane(stream_id: RemoteStreamId, reason: impl Into<String>) -> Self {
        Self::Lane {
            stream_id,
            reason: reason.into(),
        }
    }

    pub fn association(reason: impl Into<String>) -> Self {
        Self::Association {
            reason: reason.into(),
        }
    }

    pub fn reason(&self) -> &str {
        match self {
            Self::Lane { reason, .. } | Self::Association { reason } => reason,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpAssociationReaderSupervisionDecision {
    RestartInboundStreams {
        restart_count: usize,
        failure: TcpAssociationReaderFailure,
    },
    StopInboundStreams {
        restart_count: usize,
        failure: TcpAssociationReaderFailure,
    },
    IgnoreWhileStopped {
        failure: TcpAssociationReaderFailure,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpAssociationReaderSupervisor {
    settings: TcpAssociationReaderRestartSettings,
    restart_count: usize,
    stopped: bool,
}

impl TcpAssociationReaderSupervisor {
    pub fn new(settings: TcpAssociationReaderRestartSettings) -> Self {
        Self {
            settings,
            restart_count: 0,
            stopped: false,
        }
    }

    pub fn settings(&self) -> &TcpAssociationReaderRestartSettings {
        &self.settings
    }

    pub fn restart_count(&self) -> usize {
        self.restart_count
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    pub fn stop(&mut self) {
        self.stopped = true;
    }

    pub fn record_failure(
        &mut self,
        failure: TcpAssociationReaderFailure,
    ) -> TcpAssociationReaderSupervisionDecision {
        if self.stopped {
            return TcpAssociationReaderSupervisionDecision::IgnoreWhileStopped { failure };
        }

        match self.settings.max_restarts {
            Some(max_restarts) if self.restart_count >= max_restarts => {
                self.stopped = true;
                TcpAssociationReaderSupervisionDecision::StopInboundStreams {
                    restart_count: self.restart_count,
                    failure,
                }
            }
            _ => {
                self.restart_count += 1;
                TcpAssociationReaderSupervisionDecision::RestartInboundStreams {
                    restart_count: self.restart_count,
                    failure,
                }
            }
        }
    }
}

impl Default for TcpAssociationReaderSupervisor {
    fn default() -> Self {
        Self::new(TcpAssociationReaderRestartSettings::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reader_supervisor_restarts_inbound_streams_by_default() {
        let mut supervisor = TcpAssociationReaderSupervisor::default();

        let first = supervisor.record_failure(TcpAssociationReaderFailure::lane(
            RemoteStreamId::Control,
            "bad frame",
        ));
        let second = supervisor.record_failure(TcpAssociationReaderFailure::lane(
            RemoteStreamId::Ordinary,
            "socket closed",
        ));

        assert!(matches!(
            first,
            TcpAssociationReaderSupervisionDecision::RestartInboundStreams {
                restart_count: 1,
                failure: TcpAssociationReaderFailure::Lane {
                    stream_id: RemoteStreamId::Control,
                    ..
                }
            }
        ));
        assert!(matches!(
            second,
            TcpAssociationReaderSupervisionDecision::RestartInboundStreams {
                restart_count: 2,
                failure: TcpAssociationReaderFailure::Lane {
                    stream_id: RemoteStreamId::Ordinary,
                    ..
                }
            }
        ));
        assert_eq!(supervisor.restart_count(), 2);
        assert!(!supervisor.is_stopped());
    }

    #[test]
    fn reader_supervisor_stops_after_configured_restart_limit() {
        let mut supervisor = TcpAssociationReaderSupervisor::new(
            TcpAssociationReaderRestartSettings::max_restarts(1),
        );

        assert!(matches!(
            supervisor.record_failure(TcpAssociationReaderFailure::association("truncated")),
            TcpAssociationReaderSupervisionDecision::RestartInboundStreams {
                restart_count: 1,
                ..
            }
        ));
        assert!(matches!(
            supervisor.record_failure(TcpAssociationReaderFailure::association("bad magic")),
            TcpAssociationReaderSupervisionDecision::StopInboundStreams {
                restart_count: 1,
                failure: TcpAssociationReaderFailure::Association { .. }
            }
        ));
        assert!(supervisor.is_stopped());
    }

    #[test]
    fn reader_supervisor_ignores_failures_after_stop() {
        let mut supervisor = TcpAssociationReaderSupervisor::default();

        supervisor.stop();

        assert_eq!(
            supervisor.record_failure(TcpAssociationReaderFailure::lane(
                RemoteStreamId::Large,
                "late failure",
            )),
            TcpAssociationReaderSupervisionDecision::IgnoreWhileStopped {
                failure: TcpAssociationReaderFailure::lane(RemoteStreamId::Large, "late failure")
            }
        );
        assert_eq!(supervisor.restart_count(), 0);
    }
}
