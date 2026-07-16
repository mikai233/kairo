use kairo_actor::{ActorRef, Recipient, SendError};

use super::DistributedPubSubMediatorMsg;
use crate::LocalPubSubMsg;

/// In-process bridge that re-enters a mediator through `LocalDelivery`.
#[derive(Clone)]
pub(super) struct MediatorLocalRecipient<M>
where
    M: Send + 'static,
{
    mediator: ActorRef<DistributedPubSubMediatorMsg<M>>,
}

impl<M> MediatorLocalRecipient<M>
where
    M: Send + 'static,
{
    /// Creates a local-delivery bridge for one typed mediator.
    pub(super) fn new(mediator: ActorRef<DistributedPubSubMediatorMsg<M>>) -> Self {
        Self { mediator }
    }
}

impl<M> Recipient<LocalPubSubMsg<M>> for MediatorLocalRecipient<M>
where
    M: Send + 'static,
{
    fn tell(&self, message: LocalPubSubMsg<M>) -> Result<(), SendError<LocalPubSubMsg<M>>> {
        self.mediator
            .tell(DistributedPubSubMediatorMsg::LocalDelivery(message))
            .map_err(|error| {
                let reason = error.reason().to_string();
                match error.into_message() {
                    DistributedPubSubMediatorMsg::LocalDelivery(message) => {
                        SendError::new(message, reason)
                    }
                    _ => unreachable!("mediator local recipient only sends LocalDelivery"),
                }
            })
    }
}
