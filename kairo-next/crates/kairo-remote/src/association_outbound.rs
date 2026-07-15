#![deny(missing_docs)]

use std::sync::{Arc, Mutex};

use kairo_serialization::RemoteEnvelope;

use crate::{RemoteAssociation, RemoteOutbound, Result};

#[derive(Clone)]
/// Guards an outbound transport with shared association lifecycle state.
///
/// Terminal association states reject a send before the inner outbound is
/// invoked.
pub struct AssociationRemoteOutbound {
    association: Arc<Mutex<RemoteAssociation>>,
    outbound: Arc<dyn RemoteOutbound>,
}

impl AssociationRemoteOutbound {
    /// Creates a guarded outbound with newly shared ownership of `association`.
    pub fn new(association: RemoteAssociation, outbound: Arc<dyn RemoteOutbound>) -> Self {
        Self {
            association: Arc::new(Mutex::new(association)),
            outbound,
        }
    }

    /// Creates a guarded outbound using existing shared association state.
    pub fn shared(
        association: Arc<Mutex<RemoteAssociation>>,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        Self {
            association,
            outbound,
        }
    }

    /// Returns the shared association lifecycle state.
    pub fn association(&self) -> &Arc<Mutex<RemoteAssociation>> {
        &self.association
    }

    /// Returns the guarded inner outbound.
    pub fn outbound(&self) -> &Arc<dyn RemoteOutbound> {
        &self.outbound
    }
}

impl RemoteOutbound for AssociationRemoteOutbound {
    fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
        {
            let association = self
                .association
                .lock()
                .expect("remote association mutex poisoned");
            association.ensure_send_allowed()?;
        }

        self.outbound.send(envelope)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bytes::Bytes;
    use kairo_serialization::{ActorRefWireData, Manifest, SerializedMessage};

    use super::*;
    use crate::RemoteError;

    #[derive(Default)]
    struct CollectingOutbound {
        sent: Mutex<Vec<RemoteEnvelope>>,
    }

    impl CollectingOutbound {
        fn sent(&self) -> Vec<RemoteEnvelope> {
            self.sent
                .lock()
                .expect("collecting outbound mutex poisoned")
                .clone()
        }
    }

    impl RemoteOutbound for CollectingOutbound {
        fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
            self.sent
                .lock()
                .expect("collecting outbound mutex poisoned")
                .push(envelope);
            Ok(())
        }
    }

    struct FailingOutbound;

    impl RemoteOutbound for FailingOutbound {
        fn send(&self, _envelope: RemoteEnvelope) -> Result<()> {
            Err(RemoteError::Outbound("send queue full".to_string()))
        }
    }

    fn envelope(value: u8) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/target").unwrap(),
            None,
            SerializedMessage::new(
                501,
                Manifest::new("kairo.remote.test.AssociationOutbound"),
                1,
                Bytes::from(vec![value]),
            ),
        )
    }

    #[test]
    fn sends_through_idle_handshaking_and_active_associations() {
        let outbound = Arc::new(CollectingOutbound::default());
        let association = Arc::new(Mutex::new(RemoteAssociation::new(
            "kairo://remote@127.0.0.1:25520",
        )));
        let guarded = AssociationRemoteOutbound::shared(
            association.clone(),
            outbound.clone() as Arc<dyn RemoteOutbound>,
        );

        guarded.send(envelope(1)).unwrap();
        association
            .lock()
            .expect("association mutex poisoned")
            .start_handshake();
        guarded.send(envelope(2)).unwrap();
        association
            .lock()
            .expect("association mutex poisoned")
            .activate(Some(9));
        guarded.send(envelope(3)).unwrap();

        let sent = outbound.sent();
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[0].message.payload, Bytes::from_static(&[1]));
        assert_eq!(sent[1].message.payload, Bytes::from_static(&[2]));
        assert_eq!(sent[2].message.payload, Bytes::from_static(&[3]));
    }

    #[test]
    fn rejects_quarantined_association_before_forwarding() {
        let outbound = Arc::new(CollectingOutbound::default());
        let mut association = RemoteAssociation::new("kairo://remote@127.0.0.1:25520");
        association.quarantine(Some(9), "uid mismatch");
        let guarded = AssociationRemoteOutbound::new(
            association,
            outbound.clone() as Arc<dyn RemoteOutbound>,
        );

        let error = guarded
            .send(envelope(4))
            .expect_err("quarantined association should reject sends");

        assert!(matches!(error, RemoteError::AssociationQuarantined { .. }));
        assert!(outbound.sent().is_empty());
    }

    #[test]
    fn rejects_closed_association_before_forwarding() {
        let outbound = Arc::new(CollectingOutbound::default());
        let mut association = RemoteAssociation::new("kairo://remote@127.0.0.1:25520");
        association.close("transport stopped");
        let guarded = AssociationRemoteOutbound::new(
            association,
            outbound.clone() as Arc<dyn RemoteOutbound>,
        );

        let error = guarded
            .send(envelope(5))
            .expect_err("closed association should reject sends");

        assert!(matches!(error, RemoteError::AssociationClosed { .. }));
        assert!(outbound.sent().is_empty());
    }

    #[test]
    fn propagates_inner_outbound_failure_after_association_check() {
        let guarded = AssociationRemoteOutbound::new(
            RemoteAssociation::new("kairo://remote@127.0.0.1:25520"),
            Arc::new(FailingOutbound),
        );

        let error = guarded
            .send(envelope(6))
            .expect_err("inner outbound failure should propagate");

        assert!(matches!(error, RemoteError::Outbound(_)));
        assert!(error.to_string().contains("send queue full"));
    }
}
