use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};
use kairo_cluster::UniqueAddress;

use crate::{
    SingletonManagerEffect, SingletonManagerRuntime, SingletonManagerState, SingletonOldestChange,
    SingletonOldestObservation,
};

pub struct SingletonManagerActor {
    runtime: SingletonManagerRuntime,
}

impl SingletonManagerActor {
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            runtime: SingletonManagerRuntime::new(self_node),
        }
    }

    pub fn props(self_node: UniqueAddress) -> Props<Self> {
        Props::new(move || Self::new(self_node))
    }

    pub fn runtime(&self) -> &SingletonManagerRuntime {
        &self.runtime
    }
}

pub enum SingletonManagerMsg {
    ApplyInitialObservation {
        observation: SingletonOldestObservation,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    ApplyOldestChange {
        change: SingletonOldestChange,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    MarkRemoved {
        node: UniqueAddress,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    HandOverToMe {
        from: UniqueAddress,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    HandOverInProgress {
        from: UniqueAddress,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    HandOverDone {
        from: UniqueAddress,
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    SingletonTerminated {
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    StopManager {
        reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    },
    GetState {
        reply_to: ActorRef<SingletonManagerSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonManagerSnapshot {
    pub self_node: UniqueAddress,
    pub state: SingletonManagerState,
    pub removed_members: Vec<UniqueAddress>,
}

impl Actor for SingletonManagerActor {
    type Msg = SingletonManagerMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            SingletonManagerMsg::ApplyInitialObservation {
                observation,
                reply_to,
            } => {
                let effects = self.runtime.apply_initial_observation(observation);
                reply_effects(reply_to, effects);
            }
            SingletonManagerMsg::ApplyOldestChange { change, reply_to } => {
                let effects = self.runtime.apply_oldest_change(change);
                reply_effects(reply_to, effects);
            }
            SingletonManagerMsg::MarkRemoved { node, reply_to } => {
                let effects = self.runtime.mark_removed(node);
                reply_effects(reply_to, effects);
            }
            SingletonManagerMsg::HandOverToMe { from, reply_to } => {
                let effects = self.runtime.hand_over_to_me(from);
                reply_effects(reply_to, effects);
            }
            SingletonManagerMsg::HandOverInProgress { from, reply_to } => {
                let effects = self.runtime.hand_over_in_progress(&from);
                reply_effects(reply_to, effects);
            }
            SingletonManagerMsg::HandOverDone { from, reply_to } => {
                let effects = self.runtime.hand_over_done(&from);
                reply_effects(reply_to, effects);
            }
            SingletonManagerMsg::SingletonTerminated { reply_to } => {
                let effects = self.runtime.singleton_terminated();
                reply_effects(reply_to, effects);
            }
            SingletonManagerMsg::StopManager { reply_to } => {
                let effects = self.runtime.stop_manager();
                reply_effects(reply_to, effects);
            }
            SingletonManagerMsg::GetState { reply_to } => {
                let _ = reply_to.tell(SingletonManagerSnapshot::from(&self.runtime));
            }
        }
        Ok(())
    }
}

impl From<&SingletonManagerRuntime> for SingletonManagerSnapshot {
    fn from(runtime: &SingletonManagerRuntime) -> Self {
        let mut removed_members: Vec<_> = runtime.removed_members().iter().cloned().collect();
        removed_members.sort_by_key(UniqueAddress::ordering_key);
        Self {
            self_node: runtime.self_node().clone(),
            state: runtime.state().clone(),
            removed_members,
        }
    }
}

fn reply_effects(
    reply_to: Option<ActorRef<Vec<SingletonManagerEffect>>>,
    effects: Vec<SingletonManagerEffect>,
) {
    if let Some(reply_to) = reply_to {
        let _ = reply_to.tell(effects);
    }
}
