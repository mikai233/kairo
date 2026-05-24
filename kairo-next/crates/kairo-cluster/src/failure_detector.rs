use std::{collections::HashMap, hash::Hash, time::Duration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureDetectorError {
    ZeroHeartbeatInterval,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeadlineFailureDetectorSettings {
    heartbeat_interval: Duration,
    acceptable_heartbeat_pause: Duration,
}

impl DeadlineFailureDetectorSettings {
    pub fn new(
        heartbeat_interval: Duration,
        acceptable_heartbeat_pause: Duration,
    ) -> Result<Self, FailureDetectorError> {
        if heartbeat_interval.is_zero() {
            return Err(FailureDetectorError::ZeroHeartbeatInterval);
        }
        Ok(Self {
            heartbeat_interval,
            acceptable_heartbeat_pause,
        })
    }

    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    pub fn acceptable_heartbeat_pause(&self) -> Duration {
        self.acceptable_heartbeat_pause
    }

    fn deadline(&self) -> Duration {
        self.heartbeat_interval + self.acceptable_heartbeat_pause
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadlineFailureDetector {
    settings: DeadlineFailureDetectorSettings,
    latest_heartbeat_at: Option<Duration>,
}

impl DeadlineFailureDetector {
    pub fn new(settings: DeadlineFailureDetectorSettings) -> Self {
        Self {
            settings,
            latest_heartbeat_at: None,
        }
    }

    pub fn heartbeat(&mut self, now: Duration) {
        self.latest_heartbeat_at = Some(now);
    }

    pub fn is_available(&self, now: Duration) -> bool {
        self.latest_heartbeat_at.is_none_or(|latest| {
            latest
                .checked_add(self.settings.deadline())
                .is_some_and(|deadline| deadline > now)
        })
    }

    pub fn is_monitoring(&self) -> bool {
        self.latest_heartbeat_at.is_some()
    }

    pub fn latest_heartbeat_at(&self) -> Option<Duration> {
        self.latest_heartbeat_at
    }
}

#[derive(Debug, Clone)]
pub struct FailureDetectorRegistry<K> {
    settings: DeadlineFailureDetectorSettings,
    detectors: HashMap<K, DeadlineFailureDetector>,
}

impl<K> FailureDetectorRegistry<K>
where
    K: Eq + Hash + Clone,
{
    pub fn new(settings: DeadlineFailureDetectorSettings) -> Self {
        Self {
            settings,
            detectors: HashMap::new(),
        }
    }

    pub fn heartbeat(&mut self, resource: K, now: Duration) {
        self.detectors
            .entry(resource)
            .or_insert_with(|| DeadlineFailureDetector::new(self.settings))
            .heartbeat(now);
    }

    pub fn is_available(&self, resource: &K, now: Duration) -> bool {
        self.detectors
            .get(resource)
            .map(|detector| detector.is_available(now))
            .unwrap_or(true)
    }

    pub fn is_monitoring(&self, resource: &K) -> bool {
        self.detectors
            .get(resource)
            .map(DeadlineFailureDetector::is_monitoring)
            .unwrap_or(false)
    }

    pub fn remove(&mut self, resource: &K) {
        self.detectors.remove(resource);
    }

    pub fn reset(&mut self) {
        self.detectors.clear();
    }

    pub fn detector(&self, resource: &K) -> Option<&DeadlineFailureDetector> {
        self.detectors.get(resource)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kairo_actor::Address;

    use super::*;
    use crate::UniqueAddress;

    #[test]
    fn deadline_detector_treats_unmonitored_resource_as_available() {
        let detector = DeadlineFailureDetector::new(settings());

        assert!(detector.is_available(ms(10_000)));
        assert!(!detector.is_monitoring());
    }

    #[test]
    fn deadline_detector_becomes_unavailable_after_deadline() {
        let mut detector = DeadlineFailureDetector::new(settings());
        detector.heartbeat(ms(1_000));

        assert!(detector.is_monitoring());
        assert!(detector.is_available(ms(4_999)));
        assert!(!detector.is_available(ms(5_000)));
    }

    #[test]
    fn heartbeat_extends_deadline() {
        let mut detector = DeadlineFailureDetector::new(settings());
        detector.heartbeat(ms(1_000));
        detector.heartbeat(ms(4_000));

        assert!(detector.is_available(ms(7_999)));
        assert!(!detector.is_available(ms(8_000)));
    }

    #[test]
    fn registry_creates_detector_on_first_heartbeat() {
        let node = node("a", 1);
        let mut registry = FailureDetectorRegistry::new(settings());

        assert!(registry.is_available(&node, ms(100)));
        assert!(!registry.is_monitoring(&node));

        registry.heartbeat(node.clone(), ms(100));

        assert!(registry.is_monitoring(&node));
        assert!(registry.is_available(&node, ms(4_099)));
        assert!(!registry.is_available(&node, ms(4_100)));
    }

    #[test]
    fn registry_remove_forgets_detector_and_resource_becomes_available() {
        let node = node("a", 1);
        let mut registry = FailureDetectorRegistry::new(settings());
        registry.heartbeat(node.clone(), ms(100));

        registry.remove(&node);

        assert!(!registry.is_monitoring(&node));
        assert!(registry.is_available(&node, ms(100_000)));
    }

    #[test]
    fn settings_reject_zero_heartbeat_interval() {
        assert_eq!(
            DeadlineFailureDetectorSettings::new(Duration::ZERO, ms(1)),
            Err(FailureDetectorError::ZeroHeartbeatInterval)
        );
    }

    fn settings() -> DeadlineFailureDetectorSettings {
        DeadlineFailureDetectorSettings::new(ms(1_000), ms(3_000)).unwrap()
    }

    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }
}
