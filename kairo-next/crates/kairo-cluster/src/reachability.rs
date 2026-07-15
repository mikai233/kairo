#![deny(missing_docs)]

use std::collections::{HashMap, HashSet};

use crate::UniqueAddress;

/// One observer's or the cluster's aggregate view of a subject's reachability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReachabilityStatus {
    /// No negative observation is recorded.
    Reachable,
    /// At least one observer currently considers the subject unreachable.
    Unreachable,
    /// At least one observer considers the subject permanently terminated.
    Terminated,
}

/// Versioned reachability observation owned by one cluster member.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReachabilityRecord {
    /// Member whose local failure detector produced the observation.
    pub observer: UniqueAddress,
    /// Member being observed.
    pub subject: UniqueAddress,
    /// Observed state.
    pub status: ReachabilityStatus,
    /// Observer-row version at which this record was written.
    pub version: u64,
}

/// Immutable, observer-versioned cluster reachability table.
///
/// Each observer exclusively advances its row. Merge selects the complete row
/// at the newest observer version, which avoids combining observations from
/// different points in that observer's history. Reachable is implicit, so an
/// all-reachable table contains no records.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Reachability {
    records: Vec<ReachabilityRecord>,
    versions: HashMap<UniqueAddress, u64>,
}

impl Reachability {
    /// Creates an all-reachable table with no observer versions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reconstructs a table from explicit records and observer-row versions.
    ///
    /// This is intended for validated wire and persistence boundaries; callers
    /// are responsible for providing at most one record per observer/subject.
    pub fn from_parts(
        records: impl IntoIterator<Item = ReachabilityRecord>,
        versions: impl IntoIterator<Item = (UniqueAddress, u64)>,
    ) -> Self {
        Self {
            records: records.into_iter().collect(),
            versions: versions.into_iter().collect(),
        }
    }

    /// Returns every explicit reachability record.
    pub fn records(&self) -> &[ReachabilityRecord] {
        &self.records
    }

    /// Returns the complete observer-row version table.
    pub fn versions(&self) -> &HashMap<UniqueAddress, u64> {
        &self.versions
    }

    /// Returns an observer's row version, or zero when it has no history.
    pub fn version(&self, observer: &UniqueAddress) -> u64 {
        self.versions.get(observer).copied().unwrap_or(0)
    }

    /// Returns whether no negative observation is recorded for any subject.
    pub fn is_all_reachable(&self) -> bool {
        self.records.is_empty()
    }

    /// Returns a table where `observer` reports `subject` unreachable.
    pub fn unreachable(&self, observer: UniqueAddress, subject: UniqueAddress) -> Self {
        self.change(observer, subject, ReachabilityStatus::Unreachable)
    }

    /// Returns a table where `observer` reports `subject` reachable.
    ///
    /// When this clears the observer's final negative observation, its explicit
    /// record row is pruned while the advanced row version is retained.
    pub fn reachable(&self, observer: UniqueAddress, subject: UniqueAddress) -> Self {
        self.change(observer, subject, ReachabilityStatus::Reachable)
    }

    /// Returns a table where `observer` reports `subject` terminated.
    ///
    /// Terminated records are terminal for that observer/subject pair.
    pub fn terminated(&self, observer: UniqueAddress, subject: UniqueAddress) -> Self {
        self.change(observer, subject, ReachabilityStatus::Terminated)
    }

    /// Returns one observer's status for one subject, defaulting to reachable.
    pub fn status(&self, observer: &UniqueAddress, subject: &UniqueAddress) -> ReachabilityStatus {
        self.records
            .iter()
            .find(|record| &record.observer == observer && &record.subject == subject)
            .map(|record| record.status)
            .unwrap_or(ReachabilityStatus::Reachable)
    }

    /// Returns the aggregate status reported for `subject`.
    ///
    /// Terminated dominates unreachable, which dominates implicit reachable.
    pub fn status_of(&self, subject: &UniqueAddress) -> ReachabilityStatus {
        if self.records.iter().any(|record| {
            &record.subject == subject && record.status == ReachabilityStatus::Terminated
        }) {
            ReachabilityStatus::Terminated
        } else if self.records.iter().any(|record| {
            &record.subject == subject && record.status == ReachabilityStatus::Unreachable
        }) {
            ReachabilityStatus::Unreachable
        } else {
            ReachabilityStatus::Reachable
        }
    }

    /// Returns subjects reported unreachable but not terminated by any observer.
    pub fn all_unreachable(&self) -> HashSet<UniqueAddress> {
        let terminated: HashSet<_> = self
            .records
            .iter()
            .filter(|record| record.status == ReachabilityStatus::Terminated)
            .map(|record| record.subject.clone())
            .collect();
        self.records
            .iter()
            .filter(|record| record.status == ReachabilityStatus::Unreachable)
            .map(|record| record.subject.clone())
            .filter(|subject| !terminated.contains(subject))
            .collect()
    }

    /// Returns every subject with an unreachable or terminated observation.
    pub fn all_unreachable_or_terminated(&self) -> HashSet<UniqueAddress> {
        self.records
            .iter()
            .filter(|record| {
                matches!(
                    record.status,
                    ReachabilityStatus::Unreachable | ReachabilityStatus::Terminated
                )
            })
            .map(|record| record.subject.clone())
            .collect()
    }

    /// Returns every member that owns at least one negative observation row.
    pub fn all_observers(&self) -> HashSet<UniqueAddress> {
        self.records
            .iter()
            .filter(|record| {
                matches!(
                    record.status,
                    ReachabilityStatus::Unreachable | ReachabilityStatus::Terminated
                )
            })
            .map(|record| record.observer.clone())
            .collect()
    }

    /// Merges tables by choosing the newest complete row for each allowed observer.
    ///
    /// Records whose observer or subject is absent from `allowed` are discarded.
    pub fn merge(&self, allowed: &HashSet<UniqueAddress>, other: &Self) -> Self {
        let mut records = Vec::new();
        let mut versions = self.versions.clone();

        for observer in allowed {
            let version_left = self.version(observer);
            let version_right = other.version(observer);
            let rows = if version_left >= version_right {
                self.observer_rows(observer)
            } else {
                other.observer_rows(observer)
            };

            records.extend(
                rows.into_iter()
                    .filter(|record| allowed.contains(&record.subject)),
            );

            if version_right > version_left {
                versions.insert(observer.clone(), version_right);
            }
        }

        versions.retain(|observer, _| allowed.contains(observer));
        Self { records, versions }
    }

    /// Removes records and observer versions involving any member in `removed`.
    pub fn remove(&self, removed: &HashSet<UniqueAddress>) -> Self {
        let records = self
            .records
            .iter()
            .filter(|record| {
                !removed.contains(&record.observer) && !removed.contains(&record.subject)
            })
            .cloned()
            .collect();
        let mut versions = self.versions.clone();
        versions.retain(|observer, _| !removed.contains(observer));
        Self { records, versions }
    }

    fn change(
        &self,
        observer: UniqueAddress,
        subject: UniqueAddress,
        status: ReachabilityStatus,
    ) -> Self {
        let Some(index) = self
            .records
            .iter()
            .position(|record| record.observer == observer && record.subject == subject)
        else {
            if status == ReachabilityStatus::Reachable {
                return self.clone();
            }
            let mut changed = self.clone();
            let version = changed.next_version(&observer);
            changed.versions.insert(observer.clone(), version);
            changed.records.push(ReachabilityRecord {
                observer,
                subject,
                status,
                version,
            });
            return changed;
        };

        let old = &self.records[index];
        if old.status == ReachabilityStatus::Terminated || old.status == status {
            return self.clone();
        }

        let mut changed = self.clone();
        let version = changed.next_version(&observer);
        changed.versions.insert(observer.clone(), version);

        if status == ReachabilityStatus::Reachable
            && changed
                .records
                .iter()
                .filter(|record| record.observer == observer)
                .all(|record| {
                    record.subject == subject || record.status == ReachabilityStatus::Reachable
                })
        {
            changed.records.retain(|record| record.observer != observer);
        } else {
            changed.records[index] = ReachabilityRecord {
                observer,
                subject,
                status,
                version,
            };
        }
        changed
    }

    fn next_version(&self, observer: &UniqueAddress) -> u64 {
        self.version(observer) + 1
    }

    fn observer_rows(&self, observer: &UniqueAddress) -> Vec<ReachabilityRecord> {
        self.records
            .iter()
            .filter(|record| &record.observer == observer)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use kairo_actor::Address;

    use super::*;

    #[test]
    fn reachable_is_default_and_unreachable_adds_versioned_record() {
        let observer = node("a", 1);
        let subject = node("b", 2);
        let reachability = Reachability::new();

        assert_eq!(
            reachability.status(&observer, &subject),
            ReachabilityStatus::Reachable
        );
        let changed = reachability.unreachable(observer.clone(), subject.clone());

        assert_eq!(
            changed.status(&observer, &subject),
            ReachabilityStatus::Unreachable
        );
        assert_eq!(changed.version(&observer), 1);
        assert_eq!(changed.records().len(), 1);
    }

    #[test]
    fn reachable_prunes_observer_row_when_no_negative_records_remain() {
        let observer = node("a", 1);
        let subject = node("b", 2);
        let reachability = Reachability::new()
            .unreachable(observer.clone(), subject.clone())
            .reachable(observer.clone(), subject.clone());

        assert!(reachability.is_all_reachable());
        assert_eq!(reachability.version(&observer), 2);
    }

    #[test]
    fn terminated_dominates_aggregated_subject_status() {
        let observer_a = node("a", 1);
        let observer_b = node("b", 2);
        let subject = node("c", 3);
        let reachability = Reachability::new()
            .unreachable(observer_a, subject.clone())
            .terminated(observer_b, subject.clone());

        assert_eq!(
            reachability.status_of(&subject),
            ReachabilityStatus::Terminated
        );
        assert!(reachability.all_unreachable().is_empty());
        assert!(
            reachability
                .all_unreachable_or_terminated()
                .contains(&subject)
        );
    }

    #[test]
    fn all_observers_reports_negative_reachability_observers() {
        let observer_a = node("a", 1);
        let observer_b = node("b", 2);
        let subject = node("c", 3);
        let reachability = Reachability::new()
            .unreachable(observer_a.clone(), subject.clone())
            .terminated(observer_b.clone(), subject);

        assert_eq!(
            reachability.all_observers(),
            HashSet::from([observer_a, observer_b])
        );
    }

    #[test]
    fn merge_keeps_newest_rows_per_observer_and_filters_disallowed_nodes() {
        let observer = node("a", 1);
        let subject = node("b", 2);
        let removed = node("c", 3);
        let left = Reachability::new()
            .unreachable(observer.clone(), subject.clone())
            .unreachable(observer.clone(), removed.clone());
        let right = left.reachable(observer.clone(), subject.clone());
        let allowed = HashSet::from([observer.clone(), subject.clone()]);

        let merged = left.merge(&allowed, &right);

        assert_eq!(merged.records().len(), 1);
        assert_eq!(merged.version(&observer), 3);
        assert_eq!(
            merged.status(&observer, &subject),
            ReachabilityStatus::Reachable
        );
        assert_eq!(
            merged.status(&observer, &removed),
            ReachabilityStatus::Reachable
        );
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress {
            address: Address::local(system),
            uid,
        }
    }
}
