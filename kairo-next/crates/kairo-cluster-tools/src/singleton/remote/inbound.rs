use std::sync::Arc;

use kairo_actor::ActorRef;
use kairo_cluster::UniqueAddress;
use kairo_serialization::{Registry, RemoteEnvelope, RemoteMessage, SerializedMessage};

use crate::{
    SingletonHandOverDone, SingletonHandOverInProgress, SingletonHandOverToMe, SingletonManagerMsg,
    SingletonTakeOverFromMe,
};

use super::{
    DEFAULT_SINGLETON_MANAGER_REMOTE_PATH, SingletonManagerRemoteError, validate_recipient,
};

/// Validating inbound adapter for the generic singleton-manager actor.
///
/// Full envelopes must target this node's exact canonical manager path. The
/// adapter accepts only the four stable handover manifests, deserializes their
/// explicit sender incarnation, and re-enters the synchronous manager mailbox.
#[derive(Clone)]
pub struct SingletonManagerRemoteInbound {
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    recipient_path: String,
    manager: ActorRef<SingletonManagerMsg>,
}

impl SingletonManagerRemoteInbound {
    /// Creates an inbound adapter for `manager` at the default system path.
    pub fn new(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        manager: ActorRef<SingletonManagerMsg>,
    ) -> Self {
        Self {
            self_node,
            registry,
            recipient_path: DEFAULT_SINGLETON_MANAGER_REMOTE_PATH.to_string(),
            manager,
        }
    }

    /// Overrides the canonical recipient path expected on full envelopes.
    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    /// Validates and delivers one complete remote envelope.
    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), SingletonManagerRemoteError> {
        validate_recipient(&self.self_node, &self.recipient_path, &envelope.recipient)?;
        self.receive_message(envelope.message)
    }

    /// Delivers an already-demultiplexed serialized handover message.
    ///
    /// This entry point does not validate an envelope recipient; callers must
    /// perform canonical routing before discarding the outer envelope.
    pub fn receive_message(
        &self,
        message: SerializedMessage,
    ) -> Result<(), SingletonManagerRemoteError> {
        match message.manifest.as_str() {
            SingletonHandOverToMe::MANIFEST => {
                let msg = self
                    .registry
                    .deserialize::<SingletonHandOverToMe>(message)?;
                self.tell_manager(SingletonManagerMsg::HandOverToMe {
                    from: msg.from,
                    reply_to: None,
                })
            }
            SingletonHandOverInProgress::MANIFEST => {
                let msg = self
                    .registry
                    .deserialize::<SingletonHandOverInProgress>(message)?;
                self.tell_manager(SingletonManagerMsg::HandOverInProgress {
                    from: msg.from,
                    reply_to: None,
                })
            }
            SingletonHandOverDone::MANIFEST => {
                let msg = self
                    .registry
                    .deserialize::<SingletonHandOverDone>(message)?;
                self.tell_manager(SingletonManagerMsg::HandOverDone {
                    from: msg.from,
                    reply_to: None,
                })
            }
            SingletonTakeOverFromMe::MANIFEST => {
                let msg = self
                    .registry
                    .deserialize::<SingletonTakeOverFromMe>(message)?;
                self.tell_manager(SingletonManagerMsg::TakeOverFromMe {
                    from: msg.from,
                    reply_to: None,
                })
            }
            manifest => Err(SingletonManagerRemoteError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }

    fn tell_manager(
        &self,
        message: SingletonManagerMsg,
    ) -> Result<(), SingletonManagerRemoteError> {
        self.manager
            .tell(message)
            .map_err(|error| SingletonManagerRemoteError::Send {
                target: self.manager.path().to_string(),
                reason: error.reason().to_string(),
            })
    }
}
