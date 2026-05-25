use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use crate::ClusterAssociationPeerTarget;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterTcpPeerReconnectError {
    ZeroRetryInterval,
}

impl Display for ClusterTcpPeerReconnectError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRetryInterval => {
                write!(f, "cluster tcp peer retry interval must be non-zero")
            }
        }
    }
}

impl std::error::Error for ClusterTcpPeerReconnectError {}

pub type ClusterTcpPeerReconnectResult<T> = Result<T, ClusterTcpPeerReconnectError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTcpPeerReconnectSettings {
    retry_interval: Duration,
}

impl ClusterTcpPeerReconnectSettings {
    pub fn new(retry_interval: Duration) -> ClusterTcpPeerReconnectResult<Self> {
        if retry_interval.is_zero() {
            return Err(ClusterTcpPeerReconnectError::ZeroRetryInterval);
        }
        Ok(Self { retry_interval })
    }

    pub fn retry_interval(&self) -> Duration {
        self.retry_interval
    }
}

impl Default for ClusterTcpPeerReconnectSettings {
    fn default() -> Self {
        Self {
            retry_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTcpPeerReconnectPending {
    pub target: ClusterAssociationPeerTarget,
    pub attempts: u64,
    pub next_retry_at: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterTcpPeerReconnectReport {
    pub scheduled: Vec<ClusterTcpPeerReconnectPending>,
    pub cleared: Vec<ClusterAssociationPeerTarget>,
}

impl ClusterTcpPeerReconnectReport {
    pub fn is_empty(&self) -> bool {
        self.scheduled.is_empty() && self.cleared.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct ClusterTcpPeerReconnectState {
    settings: ClusterTcpPeerReconnectSettings,
    pending: BTreeMap<String, ClusterTcpPeerReconnectPending>,
}

impl ClusterTcpPeerReconnectState {
    pub fn new(settings: ClusterTcpPeerReconnectSettings) -> Self {
        Self {
            settings,
            pending: BTreeMap::new(),
        }
    }

    pub fn settings(&self) -> &ClusterTcpPeerReconnectSettings {
        &self.settings
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn pending_reconnects(&self) -> Vec<ClusterTcpPeerReconnectPending> {
        self.pending.values().cloned().collect()
    }

    pub fn record_failure(
        &mut self,
        target: ClusterAssociationPeerTarget,
        now: Duration,
    ) -> ClusterTcpPeerReconnectPending {
        let key = peer_key(&target);
        let attempts = self
            .pending
            .get(&key)
            .map_or(1, |pending| pending.attempts.saturating_add(1));
        let pending = ClusterTcpPeerReconnectPending {
            target,
            attempts,
            next_retry_at: now.saturating_add(self.settings.retry_interval()),
        };
        self.pending.insert(key, pending.clone());
        pending
    }

    pub fn clear_peer(
        &mut self,
        target: &ClusterAssociationPeerTarget,
    ) -> Option<ClusterAssociationPeerTarget> {
        self.pending
            .remove(&peer_key(target))
            .map(|pending| pending.target)
    }

    pub fn clear_all(&mut self) -> Vec<ClusterAssociationPeerTarget> {
        std::mem::take(&mut self.pending)
            .into_values()
            .map(|pending| pending.target)
            .collect()
    }

    pub fn due_targets(&self, now: Duration) -> Vec<ClusterAssociationPeerTarget> {
        self.pending
            .values()
            .filter(|pending| pending.next_retry_at <= now)
            .map(|pending| pending.target.clone())
            .collect()
    }
}

impl Default for ClusterTcpPeerReconnectState {
    fn default() -> Self {
        Self::new(ClusterTcpPeerReconnectSettings::default())
    }
}

fn peer_key(target: &ClusterAssociationPeerTarget) -> String {
    target.node().ordering_key()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kairo_actor::Address;

    use super::*;
    use crate::UniqueAddress;

    fn target(system: &str, port: u16, uid: u64) -> ClusterAssociationPeerTarget {
        ClusterAssociationPeerTarget::new(UniqueAddress::new(
            Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
            uid,
        ))
        .unwrap()
    }

    #[test]
    fn settings_reject_zero_retry_interval() {
        assert_eq!(
            ClusterTcpPeerReconnectSettings::new(Duration::ZERO).unwrap_err(),
            ClusterTcpPeerReconnectError::ZeroRetryInterval
        );
    }

    #[test]
    fn failures_schedule_due_retries_and_increment_attempts() {
        let settings = ClusterTcpPeerReconnectSettings::new(Duration::from_millis(50)).unwrap();
        let mut state = ClusterTcpPeerReconnectState::new(settings);
        let peer = target("peer", 2552, 2);

        let pending = state.record_failure(peer.clone(), Duration::from_millis(10));

        assert_eq!(pending.attempts, 1);
        assert_eq!(pending.next_retry_at, Duration::from_millis(60));
        assert!(state.due_targets(Duration::from_millis(59)).is_empty());
        assert_eq!(
            state.due_targets(Duration::from_millis(60)),
            vec![peer.clone()]
        );

        let pending = state.record_failure(peer.clone(), Duration::from_millis(60));

        assert_eq!(pending.attempts, 2);
        assert_eq!(pending.next_retry_at, Duration::from_millis(110));
        assert_eq!(state.pending_count(), 1);
        assert_eq!(state.pending_reconnects(), vec![pending]);
    }

    #[test]
    fn successful_or_removed_peer_clears_pending_retry() {
        let mut state = ClusterTcpPeerReconnectState::default();
        let peer = target("peer", 2552, 2);
        state.record_failure(peer.clone(), Duration::ZERO);

        assert_eq!(state.clear_peer(&peer), Some(peer.clone()));
        assert_eq!(state.clear_peer(&peer), None);
        assert!(state.pending_reconnects().is_empty());
    }
}
