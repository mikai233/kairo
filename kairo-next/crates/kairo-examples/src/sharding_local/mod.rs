use std::sync::atomic::{AtomicU64, Ordering};

mod graceful_shutdown;
mod local;

pub use graceful_shutdown::{
    GracefulRegionShutdownObservation, run_local_graceful_region_shutdown,
};
pub use local::{EntityObservation, LocalShardingExample};

static REPLY_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) fn next_reply_id() -> u64 {
    REPLY_ID.fetch_add(1, Ordering::Relaxed)
}
