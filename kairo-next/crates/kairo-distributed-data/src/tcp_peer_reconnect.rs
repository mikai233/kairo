use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use kairo_cluster::ClusterAssociationPeerTarget;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicatorTcpPeerReconnectError {
    ZeroRetryInterval,
}

impl Display for ReplicatorTcpPeerReconnectError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRetryInterval => {
                write!(
                    f,
                    "distributed-data tcp peer retry interval must be non-zero"
                )
            }
        }
    }
}

impl std::error::Error for ReplicatorTcpPeerReconnectError {}

pub type ReplicatorTcpPeerReconnectResult<T> = Result<T, ReplicatorTcpPeerReconnectError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorTcpPeerReconnectSettings {
    retry_interval: Duration,
}

impl ReplicatorTcpPeerReconnectSettings {
    pub fn new(retry_interval: Duration) -> ReplicatorTcpPeerReconnectResult<Self> {
        if retry_interval.is_zero() {
            return Err(ReplicatorTcpPeerReconnectError::ZeroRetryInterval);
        }
        Ok(Self { retry_interval })
    }

    pub fn retry_interval(&self) -> Duration {
        self.retry_interval
    }
}

impl Default for ReplicatorTcpPeerReconnectSettings {
    fn default() -> Self {
        Self {
            retry_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorTcpPeerReconnectPending {
    pub target: ClusterAssociationPeerTarget,
    pub attempts: u64,
    pub next_retry_at: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicatorTcpPeerReconnectReport {
    pub scheduled: Vec<ReplicatorTcpPeerReconnectPending>,
    pub cleared: Vec<ClusterAssociationPeerTarget>,
}

impl ReplicatorTcpPeerReconnectReport {
    pub fn is_empty(&self) -> bool {
        self.scheduled.is_empty() && self.cleared.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct ReplicatorTcpPeerReconnectState {
    settings: ReplicatorTcpPeerReconnectSettings,
    pending: BTreeMap<String, ReplicatorTcpPeerReconnectPending>,
}

impl ReplicatorTcpPeerReconnectState {
    pub fn new(settings: ReplicatorTcpPeerReconnectSettings) -> Self {
        Self {
            settings,
            pending: BTreeMap::new(),
        }
    }

    pub fn settings(&self) -> &ReplicatorTcpPeerReconnectSettings {
        &self.settings
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn pending_reconnects(&self) -> Vec<ReplicatorTcpPeerReconnectPending> {
        self.pending.values().cloned().collect()
    }

    pub fn record_failure(
        &mut self,
        target: ClusterAssociationPeerTarget,
        now: Duration,
    ) -> ReplicatorTcpPeerReconnectPending {
        let key = peer_key(&target);
        let attempts = self
            .pending
            .get(&key)
            .map_or(1, |pending| pending.attempts.saturating_add(1));
        let pending = ReplicatorTcpPeerReconnectPending {
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

impl Default for ReplicatorTcpPeerReconnectState {
    fn default() -> Self {
        Self::new(ReplicatorTcpPeerReconnectSettings::default())
    }
}

fn peer_key(target: &ClusterAssociationPeerTarget) -> String {
    target.node().ordering_key()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kairo_actor::Address;
    use kairo_cluster::UniqueAddress;

    use super::*;

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
            ReplicatorTcpPeerReconnectSettings::new(Duration::ZERO).unwrap_err(),
            ReplicatorTcpPeerReconnectError::ZeroRetryInterval
        );
    }

    #[test]
    fn failures_schedule_due_retries_and_increment_attempts() {
        let settings = ReplicatorTcpPeerReconnectSettings::new(Duration::from_millis(50)).unwrap();
        let mut state = ReplicatorTcpPeerReconnectState::new(settings);
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
        let mut state = ReplicatorTcpPeerReconnectState::default();
        let peer = target("peer", 2552, 2);
        state.record_failure(peer.clone(), Duration::ZERO);

        assert_eq!(state.clear_peer(&peer), Some(peer.clone()));
        assert_eq!(state.clear_peer(&peer), None);
        assert!(state.pending_reconnects().is_empty());
    }

    #[test]
    fn clear_all_returns_pending_targets_once() {
        let mut state = ReplicatorTcpPeerReconnectState::default();
        let first = target("first", 2552, 2);
        let second = target("second", 2553, 3);
        state.record_failure(first.clone(), Duration::ZERO);
        state.record_failure(second.clone(), Duration::ZERO);

        let cleared = state.clear_all();

        assert_eq!(cleared, vec![first, second]);
        assert!(state.clear_all().is_empty());
        assert_eq!(state.pending_count(), 0);
    }
}
