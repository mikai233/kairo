#![deny(missing_docs)]

use crate::RemoteStreamId;

/// Restart limit applied to TCP association reader failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpAssociationReaderRestartSettings {
    max_restarts: Option<usize>,
}

impl TcpAssociationReaderRestartSettings {
    /// Allows an unlimited number of restart decisions.
    pub fn unlimited() -> Self {
        Self { max_restarts: None }
    }

    /// Stops readers after `max_restarts` restart decisions.
    pub fn max_restarts(max_restarts: usize) -> Self {
        Self {
            max_restarts: Some(max_restarts),
        }
    }

    /// Returns the configured restart limit, or `None` when unlimited.
    pub fn max_restarts_limit(&self) -> Option<usize> {
        self.max_restarts
    }
}

impl Default for TcpAssociationReaderRestartSettings {
    fn default() -> Self {
        Self::unlimited()
    }
}

/// Failure observed while joining TCP association reader threads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpAssociationReaderFailure {
    /// One identified lane reader failed.
    Lane {
        /// Lane whose reader failed.
        stream_id: RemoteStreamId,
        /// Human-readable failure reason.
        reason: String,
    },
    /// An association reader failed without lane identity.
    Association {
        /// Human-readable failure reason.
        reason: String,
    },
}

impl TcpAssociationReaderFailure {
    /// Creates a failure for one identified lane.
    pub fn lane(stream_id: RemoteStreamId, reason: impl Into<String>) -> Self {
        Self::Lane {
            stream_id,
            reason: reason.into(),
        }
    }

    /// Creates an association-wide reader failure.
    pub fn association(reason: impl Into<String>) -> Self {
        Self::Association {
            reason: reason.into(),
        }
    }

    /// Returns the human-readable failure reason.
    pub fn reason(&self) -> &str {
        match self {
            Self::Lane { reason, .. } | Self::Association { reason } => reason,
        }
    }
}

/// Policy decision produced for one TCP association reader failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpAssociationReaderSupervisionDecision {
    /// Restart association readers after a failure.
    RestartInboundStreams {
        /// Number of restart decisions issued so far.
        restart_count: usize,
        /// Failure that triggered the decision.
        failure: TcpAssociationReaderFailure,
    },
    /// Stop association readers because the restart limit was exhausted.
    StopInboundStreams {
        /// Number of restarts completed before stopping.
        restart_count: usize,
        /// Failure that exhausted the policy.
        failure: TcpAssociationReaderFailure,
    },
    /// Ignore a failure observed after the supervisor was stopped.
    IgnoreWhileStopped {
        /// Late failure that was ignored.
        failure: TcpAssociationReaderFailure,
    },
}

/// Stateful restart-policy evaluator for TCP association reader failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpAssociationReaderSupervisor {
    settings: TcpAssociationReaderRestartSettings,
    restart_count: usize,
    stopped: bool,
}

impl TcpAssociationReaderSupervisor {
    /// Creates a running supervisor with the supplied restart policy.
    pub fn new(settings: TcpAssociationReaderRestartSettings) -> Self {
        Self {
            settings,
            restart_count: 0,
            stopped: false,
        }
    }

    /// Returns the configured restart policy.
    pub fn settings(&self) -> &TcpAssociationReaderRestartSettings {
        &self.settings
    }

    /// Returns the number of restart decisions issued so far.
    pub fn restart_count(&self) -> usize {
        self.restart_count
    }

    /// Returns whether this supervisor will no longer restart readers.
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    /// Stops this supervisor so later failures are ignored.
    pub fn stop(&mut self) {
        self.stopped = true;
    }

    /// Records a failure and returns the corresponding policy decision.
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
