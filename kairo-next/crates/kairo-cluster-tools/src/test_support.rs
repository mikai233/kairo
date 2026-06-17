use std::sync::{Mutex, MutexGuard};

pub(crate) type SocketTestGuard = MutexGuard<'static, ()>;

pub(crate) fn cluster_tools_socket_test_lock() -> SocketTestGuard {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}
