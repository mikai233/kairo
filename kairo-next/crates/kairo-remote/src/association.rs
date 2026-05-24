use crate::{RemoteError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteAssociation {
    remote_address: String,
    state: AssociationState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssociationState {
    Idle,
    Handshaking,
    Active {
        remote_uid: Option<u64>,
    },
    Quarantined {
        remote_uid: Option<u64>,
        reason: String,
    },
    Closed {
        reason: String,
    },
}

impl RemoteAssociation {
    pub fn new(remote_address: impl Into<String>) -> Self {
        Self {
            remote_address: remote_address.into(),
            state: AssociationState::Idle,
        }
    }

    pub fn remote_address(&self) -> &str {
        &self.remote_address
    }

    pub fn state(&self) -> &AssociationState {
        &self.state
    }

    pub fn start_handshake(&mut self) {
        if matches!(self.state, AssociationState::Idle) {
            self.state = AssociationState::Handshaking;
        }
    }

    pub fn activate(&mut self, remote_uid: Option<u64>) {
        self.state = AssociationState::Active { remote_uid };
    }

    pub fn quarantine(&mut self, remote_uid: Option<u64>, reason: impl Into<String>) {
        self.state = AssociationState::Quarantined {
            remote_uid,
            reason: reason.into(),
        };
    }

    pub fn close(&mut self, reason: impl Into<String>) {
        self.state = AssociationState::Closed {
            reason: reason.into(),
        };
    }

    pub fn ensure_send_allowed(&self) -> Result<()> {
        match &self.state {
            AssociationState::Idle
            | AssociationState::Handshaking
            | AssociationState::Active { .. } => Ok(()),
            AssociationState::Quarantined { reason, .. } => {
                Err(RemoteError::AssociationQuarantined {
                    remote: self.remote_address.clone(),
                    reason: reason.clone(),
                })
            }
            AssociationState::Closed { reason } => Err(RemoteError::AssociationClosed {
                remote: self.remote_address.clone(),
                reason: reason.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn association_blocks_send_after_close_or_quarantine() {
        let mut association = RemoteAssociation::new("kairo://sys@127.0.0.1:25520");
        association.start_handshake();
        association.activate(Some(7));
        assert!(association.ensure_send_allowed().is_ok());

        association.quarantine(Some(7), "uid mismatch");
        assert!(matches!(
            association.ensure_send_allowed(),
            Err(RemoteError::AssociationQuarantined { .. })
        ));

        association.close("transport stopped");
        assert!(matches!(
            association.ensure_send_allowed(),
            Err(RemoteError::AssociationClosed { .. })
        ));
    }
}
