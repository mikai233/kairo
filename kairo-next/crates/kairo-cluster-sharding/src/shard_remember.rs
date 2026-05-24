use std::collections::BTreeSet;

use crate::{EntityId, RememberShardUpdate};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardRememberState {
    enabled: bool,
    update_in_progress: bool,
    pending_starts: BTreeSet<EntityId>,
    pending_stops: BTreeSet<EntityId>,
}

impl ShardRememberState {
    pub(crate) fn disabled() -> Self {
        Self {
            enabled: false,
            update_in_progress: false,
            pending_starts: BTreeSet::new(),
            pending_stops: BTreeSet::new(),
        }
    }

    pub(crate) fn enabled() -> Self {
        Self {
            enabled: true,
            update_in_progress: false,
            pending_starts: BTreeSet::new(),
            pending_stops: BTreeSet::new(),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn update_in_progress(&self) -> bool {
        self.update_in_progress
    }

    pub(crate) fn record_start(&mut self, entity_id: EntityId) -> Option<RememberShardUpdate> {
        if !self.enabled {
            return None;
        }
        self.pending_stops.remove(&entity_id);
        if self.update_in_progress {
            self.pending_starts.insert(entity_id);
            None
        } else {
            self.update_in_progress = true;
            Some(RememberShardUpdate::new(
                [entity_id],
                std::iter::empty::<EntityId>(),
            ))
        }
    }

    pub(crate) fn record_stop(&mut self, entity_id: EntityId) -> Option<RememberShardUpdate> {
        if !self.enabled {
            return None;
        }
        self.pending_starts.remove(&entity_id);
        if self.update_in_progress {
            self.pending_stops.insert(entity_id);
            None
        } else {
            self.update_in_progress = true;
            Some(RememberShardUpdate::new(
                std::iter::empty::<EntityId>(),
                [entity_id],
            ))
        }
    }

    pub(crate) fn complete_update(
        &mut self,
        update: &RememberShardUpdate,
    ) -> Option<RememberShardUpdate> {
        if !self.enabled {
            return None;
        }

        self.update_in_progress = false;
        for entity_id in update.started() {
            self.pending_starts.remove(entity_id);
        }
        for entity_id in update.stopped() {
            self.pending_stops.remove(entity_id);
        }

        if self.pending_starts.is_empty() && self.pending_stops.is_empty() {
            return None;
        }

        let started = std::mem::take(&mut self.pending_starts);
        let stopped = std::mem::take(&mut self.pending_stops);
        self.update_in_progress = true;
        Some(RememberShardUpdate::new(started, stopped))
    }
}
