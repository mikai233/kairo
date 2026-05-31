use std::collections::BTreeMap;

use kairo_serialization::ActorRefWireData;

use crate::RegionId;

#[derive(Debug, Clone, Default)]
pub struct CoordinatorRemoteRegions {
    regions: BTreeMap<RegionId, ActorRefWireData>,
}

impl CoordinatorRemoteRegions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, region: ActorRefWireData) -> RegionId {
        let region_id = remote_region_id(&region);
        self.regions.insert(region_id.clone(), region);
        region_id
    }

    pub fn wire_ref(&self, region: &RegionId) -> Option<&ActorRefWireData> {
        self.regions.get(region)
    }

    pub fn len(&self) -> usize {
        self.regions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }
}

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
}
