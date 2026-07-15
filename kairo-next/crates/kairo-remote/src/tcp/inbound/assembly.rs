use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::tcp::{TcpAssociationHandshake, TcpAssociationIdentity};
use crate::{RemoteError, Result};

use super::accepted::TcpAcceptedStream;

pub const DEFAULT_TCP_LANE_ARRIVAL_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_TCP_MAX_PENDING_ASSOCIATIONS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpAssociationAssemblySettings {
    lane_arrival_timeout: Duration,
    max_pending_associations: usize,
}

impl TcpAssociationAssemblySettings {
    pub fn new(lane_arrival_timeout: Duration, max_pending_associations: usize) -> Result<Self> {
        if lane_arrival_timeout.is_zero() {
            return Err(RemoteError::InvalidTcpAssociationAssemblySettings(
                "tcp lane arrival timeout must be greater than zero".to_string(),
            ));
        }
        if max_pending_associations == 0 {
            return Err(RemoteError::InvalidTcpAssociationAssemblySettings(
                "tcp maximum pending associations must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            lane_arrival_timeout,
            max_pending_associations,
        })
    }

    pub fn lane_arrival_timeout(self) -> Duration {
        self.lane_arrival_timeout
    }

    pub fn max_pending_associations(self) -> usize {
        self.max_pending_associations
    }
}

impl Default for TcpAssociationAssemblySettings {
    fn default() -> Self {
        Self {
            lane_arrival_timeout: DEFAULT_TCP_LANE_ARRIVAL_TIMEOUT,
            max_pending_associations: DEFAULT_TCP_MAX_PENDING_ASSOCIATIONS,
        }
    }
}

pub(super) struct TcpCompletedAssociationAssembly {
    pub(super) handshakes: Vec<TcpAssociationHandshake>,
    pub(super) streams: Vec<TcpAcceptedStream>,
}

pub(super) struct TcpPendingAssociationAssemblies {
    expected_streams: usize,
    settings: TcpAssociationAssemblySettings,
    pending: HashMap<TcpAssociationIdentity, TcpPendingAssociationAssembly>,
}

impl TcpPendingAssociationAssemblies {
    pub(super) fn new(expected_streams: usize, settings: TcpAssociationAssemblySettings) -> Self {
        Self {
            expected_streams,
            settings,
            pending: HashMap::new(),
        }
    }

    pub(super) fn insert(
        &mut self,
        handshake: TcpAssociationHandshake,
        stream: TcpAcceptedStream,
        now: Instant,
    ) -> Result<Option<TcpCompletedAssociationAssembly>> {
        let identity = handshake.from().clone();
        let stream_id = handshake.stream_id();

        if let Some((pending_identity, _)) = self.pending.iter().find(|(pending_identity, _)| {
            pending_identity.address() == identity.address()
                && pending_identity.uid() != identity.uid()
        }) {
            return Err(RemoteError::InvalidFrame(format!(
                "tcp association mixed remote identities {}#{} and {}#{}",
                pending_identity.address(),
                pending_identity.uid(),
                identity.address(),
                identity.uid()
            )));
        }

        if !self.pending.contains_key(&identity)
            && self.pending.len() >= self.settings.max_pending_associations()
        {
            return Err(RemoteError::Inbound(format!(
                "tcp pending association limit {} reached while receiving {}#{}",
                self.settings.max_pending_associations(),
                identity.address(),
                identity.uid()
            )));
        }

        let pending =
            self.pending
                .entry(identity.clone())
                .or_insert_with(|| TcpPendingAssociationAssembly {
                    started_at: now,
                    handshakes: Vec::with_capacity(self.expected_streams),
                    streams: Vec::with_capacity(self.expected_streams),
                });
        if pending
            .handshakes
            .iter()
            .any(|existing| existing.stream_id() == stream_id)
        {
            return Err(RemoteError::InvalidFrame(format!(
                "tcp association duplicated {stream_id:?} lane handshake"
            )));
        }
        pending.handshakes.push(handshake);
        pending.streams.push(stream);

        if pending.streams.len() < self.expected_streams {
            return Ok(None);
        }

        let completed = self
            .pending
            .remove(&identity)
            .expect("completed tcp association assembly must still be pending");
        Ok(Some(TcpCompletedAssociationAssembly {
            handshakes: completed.handshakes,
            streams: completed.streams,
        }))
    }

    pub(super) fn expire(&mut self, now: Instant) -> usize {
        let before = self.pending.len();
        let timeout = self.settings.lane_arrival_timeout();
        self.pending
            .retain(|_, pending| now.duration_since(pending.started_at) < timeout);
        before - self.pending.len()
    }
}

struct TcpPendingAssociationAssembly {
    started_at: Instant,
    handshakes: Vec<TcpAssociationHandshake>,
    streams: Vec<TcpAcceptedStream>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_association_assembly_settings_reject_zero_limits() {
        assert!(TcpAssociationAssemblySettings::new(Duration::ZERO, 1).is_err());
        assert!(TcpAssociationAssemblySettings::new(Duration::from_secs(1), 0).is_err());
    }
}
