use std::collections::VecDeque;

use crate::shard_actor::ShardMsg;

pub(crate) struct ShardRememberLoadState<M> {
    loading: bool,
    stashed: VecDeque<ShardMsg<M>>,
}

impl<M> ShardRememberLoadState<M> {
    pub(crate) fn ready() -> Self {
        Self {
            loading: false,
            stashed: VecDeque::new(),
        }
    }

    pub(crate) fn loading() -> Self {
        Self {
            loading: true,
            stashed: VecDeque::new(),
        }
    }

    pub(crate) fn is_loading(&self) -> bool {
        self.loading
    }

    pub(crate) fn stash(&mut self, message: ShardMsg<M>) {
        self.stashed.push_back(message);
    }

    pub(crate) fn mark_ready(&mut self) -> VecDeque<ShardMsg<M>> {
        self.loading = false;
        std::mem::take(&mut self.stashed)
    }
}
