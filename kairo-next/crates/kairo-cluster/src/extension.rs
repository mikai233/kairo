#![deny(missing_docs)]

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

    /// Retrieves the ActorSystem-owned cluster extension.
    pub fn get(system: &ActorSystem) -> Result<Arc<Self>, ActorError> {
        system.extension::<Self>()
    }

    /// Returns the public cluster facade.
    pub fn cluster(&self) -> &Cluster {
        self.handle.cluster()
    }

    /// Returns the local cluster node incarnation.
    pub fn self_node(&self) -> &UniqueAddress {
        self.handle.self_node()
    }

    /// Starts the extension-owned one-shot join flow through `address`.
    pub fn join(&self, address: Address) -> Result<(), ClusterError> {
        self.cluster().join(address)
    }

    /// Begins graceful leave for the local member.
    pub fn leave_self(&self) -> Result<(), ClusterError> {
        self.cluster().leave_self()
    }

    /// Requests graceful leave for the member at `address`.
    pub fn leave(&self, address: Address) -> Result<(), ClusterError> {
        self.cluster().leave(address)
    }

    /// Requests that the member at `address` be marked down.
    pub fn down(&self, address: Address) -> Result<(), ClusterError> {
        self.cluster().down(address)
    }
}
