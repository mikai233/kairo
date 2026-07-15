#![deny(missing_docs)]

use std::collections::BTreeMap;

use kairo_serialization::ActorRefWireData;

use crate::RegionId;

/// Coordinator-side mapping from remote region ids to stable wire refs.
///
/// Region ids are the actor-ref path strings used by coordinator allocation
/// state. Entries must be removed when a region stops so reply and handoff
/// routing cannot retain stale side-table state.
#[derive(Debug, Clone, Default)]
pub struct CoordinatorRemoteRegions {
    regions: BTreeMap<RegionId, ActorRefWireData>,
}

impl CoordinatorRemoteRegions {
    /// Creates an empty remote-region mapping.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces a wire ref and returns its stable region id.
    pub fn register(&mut self, region: ActorRefWireData) -> RegionId {
        let region_id = remote_region_id(&region);
        self.regions.insert(region_id.clone(), region);
        region_id
    }

    /// Returns the wire ref retained for `region`.
    pub fn wire_ref(&self, region: &RegionId) -> Option<&ActorRefWireData> {
        self.regions.get(region)
    }

    /// Removes and returns the wire ref retained for `region`.
    pub fn remove(&mut self, region: &RegionId) -> Option<ActorRefWireData> {
        self.regions.remove(region)
    }

    /// Returns the number of retained remote regions.
    pub fn len(&self) -> usize {
        self.regions.len()
    }

    /// Returns whether no remote region wire refs are retained.
    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }
}

/// Derives a coordinator region id from a stable actor-ref wire path.
pub fn remote_region_id(region: &ActorRefWireData) -> RegionId {
    region.path().to_string()
}

#[cfg(test)]
mod tests {
    use kairo_serialization::ActorRefWireData;

    use super::*;

    #[test]
    fn remote_region_ids_use_stable_wire_paths() {
        let region =
            ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/region").unwrap();
        let mut regions = CoordinatorRemoteRegions::new();

        let id = regions.register(region.clone());

        assert_eq!(id, "kairo://remote@127.0.0.1:2552/system/sharding/region");
        assert_eq!(regions.wire_ref(&id), Some(&region));
    }

    #[test]
    fn stopped_remote_region_wire_ref_is_forgotten() {
        let region =
            ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/region").unwrap();
        let mut regions = CoordinatorRemoteRegions::new();
        let id = regions.register(region.clone());

        assert_eq!(regions.remove(&id), Some(region));
        assert_eq!(regions.wire_ref(&id), None);
        assert!(regions.is_empty());
        assert_eq!(regions.remove(&id), None);
    }
}
