use std::sync::Arc;

use kairo_actor::{ActorError, ActorSystem, Address};

use crate::{Cluster, ClusterDaemonHandle, ClusterError, UniqueAddress};

/// ActorSystem-owned public entry point for the composed cluster runtime.
#[derive(Clone)]
pub struct ClusterExtension {
    handle: ClusterDaemonHandle,
}

impl ClusterExtension {
    pub(crate) fn new(handle: ClusterDaemonHandle) -> Self {
        Self { handle }
    }

    pub fn get(system: &ActorSystem) -> Result<Arc<Self>, ActorError> {
        system.extension::<Self>()
    }

    pub fn cluster(&self) -> &Cluster {
        self.handle.cluster()
    }

    pub fn self_node(&self) -> &UniqueAddress {
        self.handle.self_node()
    }

    pub fn join(&self, address: Address) -> Result<(), ClusterError> {
        self.cluster().join(address)
    }

    pub fn leave_self(&self) -> Result<(), ClusterError> {
        self.cluster().leave_self()
    }

    pub fn leave(&self, address: Address) -> Result<(), ClusterError> {
        self.cluster().leave(address)
    }

    pub fn down(&self, address: Address) -> Result<(), ClusterError> {
        self.cluster().down(address)
    }
}
