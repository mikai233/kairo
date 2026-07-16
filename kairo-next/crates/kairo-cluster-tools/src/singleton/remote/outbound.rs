use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_cluster::UniqueAddress;
use kairo_remote::RemoteOutbound;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{
    SingletonHandOverDone, SingletonHandOverInProgress, SingletonHandOverToMe,
    SingletonManagerEffect, SingletonTakeOverFromMe,
};

use super::{
    DEFAULT_SINGLETON_MANAGER_REMOTE_PATH, SingletonManagerRemoteError, recipient_for_node,
};

/// Converts singleton-manager protocol effects into stable remote envelopes.
///
/// The adapter stamps every message with the exact local member incarnation,
/// serializes it through the shared registry, and sends it to the target's
/// canonical manager path. Local lifecycle effects are rejected rather than
/// silently crossing the transport boundary.
#[derive(Clone)]
pub struct SingletonManagerRemoteOutbound {
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    recipient_path: String,
    outbound: Arc<dyn RemoteOutbound>,
}

impl SingletonManagerRemoteOutbound {
    /// Creates an adapter that owns `outbound` behind a shared trait object.
    pub fn new(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        outbound: impl RemoteOutbound + 'static,
    ) -> Self {
        Self::from_arc(self_node, registry, Arc::new(outbound))
    }

    /// Creates an adapter from an existing shared outbound transport.
    pub fn from_arc(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        Self {
            self_node,
            registry,
            recipient_path: DEFAULT_SINGLETON_MANAGER_REMOTE_PATH.to_string(),
            outbound,
        }
    }

    /// Overrides the canonical recipient path used for subsequent sends.
    ///
    /// Path validation is deferred until a recipient is constructed.
    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    /// Constructs the canonical wire recipient for one target incarnation.
    ///
    /// The target must have a remote host and the configured path must be
    /// absolute.
    pub fn recipient_for_node(
        &self,
        node: &UniqueAddress,
    ) -> Result<ActorRefWireData, SingletonManagerRemoteError> {
        recipient_for_node(node, &self.recipient_path)
    }

    /// Sends one remote protocol effect.
    ///
    /// Local child-start, child-stop, and manager-stop effects return
    /// [`SingletonManagerRemoteError::UnsupportedEffect`].
    pub fn send_effect(
        &self,
        effect: &SingletonManagerEffect,
    ) -> Result<(), SingletonManagerRemoteError> {
        match effect {
            SingletonManagerEffect::SendHandOverToMe { to } => self.send_remote_message(
                to,
                &SingletonHandOverToMe {
                    from: self.self_node.clone(),
                },
            ),
            SingletonManagerEffect::SendHandOverInProgress { to } => self.send_remote_message(
                to,
                &SingletonHandOverInProgress {
                    from: self.self_node.clone(),
                },
            ),
            SingletonManagerEffect::SendHandOverDone { to } => self.send_remote_message(
                to,
                &SingletonHandOverDone {
                    from: self.self_node.clone(),
                },
            ),
            SingletonManagerEffect::SendTakeOverFromMe { to } => self.send_remote_message(
                to,
                &SingletonTakeOverFromMe {
                    from: self.self_node.clone(),
                },
            ),
            SingletonManagerEffect::StartSingleton => Err(
                SingletonManagerRemoteError::UnsupportedEffect("start-singleton"),
            ),
            SingletonManagerEffect::StopSingleton => Err(
                SingletonManagerRemoteError::UnsupportedEffect("stop-singleton"),
            ),
            SingletonManagerEffect::StopManager => Err(
                SingletonManagerRemoteError::UnsupportedEffect("stop-manager"),
            ),
        }
    }

    /// Sends an effect batch in slice order, stopping at the first failure.
    ///
    /// This operation is not transactional: effects preceding a failure may
    /// already have been delivered.
    pub fn send_effects(
        &self,
        effects: &[SingletonManagerEffect],
    ) -> Result<(), SingletonManagerRemoteError> {
        for effect in effects {
            self.send_effect(effect)?;
        }
        Ok(())
    }

    fn send_remote_message<M>(
        &self,
        to: &UniqueAddress,
        message: &M,
    ) -> Result<(), SingletonManagerRemoteError>
    where
        M: RemoteMessage,
    {
        let recipient = self.recipient_for_node(to)?;
        let target = to.ordering_key();
        let envelope = RemoteEnvelope::new(recipient, None, self.registry.serialize(message)?);
        self.outbound
            .send(envelope)
            .map_err(|error| SingletonManagerRemoteError::Send {
                target,
                reason: error.to_string(),
            })
    }
}

impl Recipient<Vec<SingletonManagerEffect>> for SingletonManagerRemoteOutbound {
    fn tell(
        &self,
        message: Vec<SingletonManagerEffect>,
    ) -> Result<(), SendError<Vec<SingletonManagerEffect>>> {
        let rejected = message.clone();
        self.send_effects(&message)
            .map_err(|error| SendError::new(rejected, error.to_string()))
    }
}
