#![deny(missing_docs)]

//! Pure full-state gossip planning and application for one typed CRDT family.
//!
//! Status exchange compares deterministic envelope digests within stable key
//! chunks. Different or remotely missing keys produce bounded full-state
//! gossip, while locally missing keys produce a status containing the
//! not-found sentinel. Applying gossip merges envelopes through
//! [`crate::ReplicatorState`] and can plan a send-back response without owning
//! transport or cluster membership.

mod apply;
mod error;
mod hash;
mod report;
mod status;

#[cfg(test)]
mod tests;

pub(crate) use apply::apply_gossip_with_seen;
pub use apply::{apply_gossip, create_gossip};
pub use error::ReplicatorGossipError;
pub use hash::{REPLICATOR_GOSSIP_NOT_FOUND_DIGEST, digest_envelope};
pub use report::{ReplicatorGossipApplyReport, ReplicatorGossipStatusPlan};
pub use status::{build_gossip_status, respond_to_gossip_status};
