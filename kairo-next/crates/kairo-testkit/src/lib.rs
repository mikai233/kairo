//! Test probes and actor system test harnesses.

use std::marker::PhantomData;

#[derive(Debug)]
pub struct TestProbe<M> {
    _message: PhantomData<fn(M)>,
}

impl<M> Default for TestProbe<M> {
    fn default() -> Self {
        Self {
            _message: PhantomData,
        }
    }
}
