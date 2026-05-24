#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatcherSettings {
    throughput: usize,
}

impl DispatcherSettings {
    pub const DEFAULT_THROUGHPUT: usize = 5;

    pub fn new(throughput: usize) -> Self {
        Self { throughput }
    }

    pub fn throughput(&self) -> usize {
        self.throughput
    }
}

impl Default for DispatcherSettings {
    fn default() -> Self {
        Self {
            throughput: Self::DEFAULT_THROUGHPUT,
        }
    }
}
