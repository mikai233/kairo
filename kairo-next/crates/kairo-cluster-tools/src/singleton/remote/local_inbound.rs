use std::marker::PhantomData;
use std::sync::Arc;

use kairo_actor::ActorRef;
use kairo_cluster::UniqueAddress;
use kairo_serialization::{Registry, RemoteEnvelope, RemoteMessage, SerializedMessage};

use crate::{
    LocalSingletonManagerMsg, SingletonHandOverDone, SingletonHandOverInProgress,
    SingletonHandOverToMe, SingletonTakeOverFromMe,
};

use super::{
    DEFAULT_SINGLETON_MANAGER_REMOTE_PATH, SingletonManagerRemoteError, validate_recipient,
};

/// Validating inbound adapter for a typed local singleton-manager actor.
///
/// This is the child-owning counterpart to [`super::SingletonManagerRemoteInbound`].
/// It accepts only stable handover manifests and never imposes serialization
/// requirements on the singleton's local business-message type `M`.
#[derive(Clone)]
pub struct LocalSingletonManagerRemoteInbound<M>
where
    M: Send + 'static,
{
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    recipient_path: String,
    manager: ActorRef<LocalSingletonManagerMsg<M>>,
    _message: PhantomData<fn(M)>,
}

impl<M> LocalSingletonManagerRemoteInbound<M>
where
    M: Send + 'static,
{
    /// Creates an inbound adapter for `manager` at the default system path.
    pub fn new(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        manager: ActorRef<LocalSingletonManagerMsg<M>>,
    ) -> Self {
        Self {
            self_node,
            registry,
            recipient_path: DEFAULT_SINGLETON_MANAGER_REMOTE_PATH.to_string(),
            manager,
            _message: PhantomData,
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
                let message = self
                    .registry
                    .deserialize::<SingletonHandOverToMe>(message)?;
                self.tell(LocalSingletonManagerMsg::HandOverToMe {
                    from: message.from,
                    reply_to: None,
                })
            }
            SingletonHandOverInProgress::MANIFEST => {
                let message = self
                    .registry
                    .deserialize::<SingletonHandOverInProgress>(message)?;
                self.tell(LocalSingletonManagerMsg::HandOverInProgress {
                    from: message.from,
                    reply_to: None,
                })
            }
            SingletonHandOverDone::MANIFEST => {
                let message = self
                    .registry
                    .deserialize::<SingletonHandOverDone>(message)?;
                self.tell(LocalSingletonManagerMsg::HandOverDone {
                    from: message.from,
                    reply_to: None,
                })
            }
            SingletonTakeOverFromMe::MANIFEST => {
                let message = self
                    .registry
                    .deserialize::<SingletonTakeOverFromMe>(message)?;
                self.tell(LocalSingletonManagerMsg::TakeOverFromMe {
                    from: message.from,
                    reply_to: None,
                })
            }
            manifest => Err(SingletonManagerRemoteError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }

    fn tell(
        &self,
        message: LocalSingletonManagerMsg<M>,
    ) -> Result<(), SingletonManagerRemoteError> {
        self.manager
            .tell(message)
            .map_err(|error| SingletonManagerRemoteError::Send {
                target: self.manager.path().to_string(),
                reason: error.reason().to_string(),
            })
    }
}
