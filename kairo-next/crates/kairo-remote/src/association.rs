use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use crate::{RemoteError, Result};

#[derive(Clone)]
pub struct RemoteAssociation {
    remote_address: String,
    state: AssociationState,
    diagnostics: Option<Arc<dyn RemoteAssociationDiagnostics>>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteAssociationDiagnostic {
    Quarantined {
        remote: String,
        remote_uid: Option<u64>,
        reason: String,
    },
    Closed {
        remote: String,
        reason: String,
    },
}

pub trait RemoteAssociationDiagnostics: Send + Sync + 'static {
    fn record(&self, diagnostic: RemoteAssociationDiagnostic);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteAssociationDiagnosticFilter {
    quarantine_events: bool,
    close_events: bool,
}

impl RemoteAssociationDiagnosticFilter {
    pub fn new(quarantine_events: bool) -> Self {
        Self::with_categories(quarantine_events, quarantine_events)
    }

    pub fn with_categories(quarantine_events: bool, close_events: bool) -> Self {
        Self {
            quarantine_events,
            close_events,
        }
    }

    pub fn all() -> Self {
        Self::with_categories(true, true)
    }

    pub fn disabled() -> Self {
        Self::with_categories(false, false)
    }

    pub fn quarantine_events(&self) -> bool {
        self.quarantine_events
    }

    pub fn close_events(&self) -> bool {
        self.close_events
    }

    pub fn observes(&self, diagnostic: &RemoteAssociationDiagnostic) -> bool {
        match diagnostic {
            RemoteAssociationDiagnostic::Quarantined { .. } => self.quarantine_events,
            RemoteAssociationDiagnostic::Closed { .. } => self.close_events,
        }
    }

    pub fn wrap(
        self,
        diagnostics: Arc<dyn RemoteAssociationDiagnostics>,
    ) -> Option<Arc<dyn RemoteAssociationDiagnostics>> {
        if self == Self::disabled() {
            None
        } else {
            Some(Arc::new(FilteredRemoteAssociationDiagnostics {
                filter: self,
                diagnostics,
            }))
        }
    }
}

impl Default for RemoteAssociationDiagnosticFilter {
    fn default() -> Self {
        Self::all()
    }
}

struct FilteredRemoteAssociationDiagnostics {
    filter: RemoteAssociationDiagnosticFilter,
    diagnostics: Arc<dyn RemoteAssociationDiagnostics>,
}

impl RemoteAssociationDiagnostics for FilteredRemoteAssociationDiagnostics {
    fn record(&self, diagnostic: RemoteAssociationDiagnostic) {
        if self.filter.observes(&diagnostic) {
            self.diagnostics.record(diagnostic);
        }
    }
}

impl<F> RemoteAssociationDiagnostics for F
where
    F: Fn(RemoteAssociationDiagnostic) + Send + Sync + 'static,
{
    fn record(&self, diagnostic: RemoteAssociationDiagnostic) {
        self(diagnostic);
    }
}

impl RemoteAssociation {
    pub fn new(remote_address: impl Into<String>) -> Self {
        Self {
            remote_address: remote_address.into(),
            state: AssociationState::Idle,
            diagnostics: None,
        }
    }

    pub fn with_diagnostics(mut self, diagnostics: Arc<dyn RemoteAssociationDiagnostics>) -> Self {
        self.diagnostics = Some(diagnostics);
        self
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
        match &self.state {
            AssociationState::Idle
            | AssociationState::Handshaking
            | AssociationState::Active { .. } => {
                self.state = AssociationState::Active { remote_uid };
            }
            AssociationState::Quarantined { .. } | AssociationState::Closed { .. } => {}
        }
    }

    pub fn quarantine(&mut self, remote_uid: Option<u64>, reason: impl Into<String>) {
        let reason = reason.into();
        self.state = AssociationState::Quarantined {
            remote_uid,
            reason: reason.clone(),
        };
        self.record_diagnostic(RemoteAssociationDiagnostic::Quarantined {
            remote: self.remote_address.clone(),
            remote_uid,
            reason,
        });
    }

    pub fn close(&mut self, reason: impl Into<String>) {
        if matches!(
            self.state,
            AssociationState::Quarantined { .. } | AssociationState::Closed { .. }
        ) {
            return;
        }
        let reason = reason.into();
        self.state = AssociationState::Closed {
            reason: reason.clone(),
        };
        self.record_diagnostic(RemoteAssociationDiagnostic::Closed {
            remote: self.remote_address.clone(),
            reason,
        });
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

    fn record_diagnostic(&self, diagnostic: RemoteAssociationDiagnostic) {
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.record(diagnostic);
        }
    }
}

impl Debug for RemoteAssociation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteAssociation")
            .field("remote_address", &self.remote_address)
            .field("state", &self.state)
            .field("has_diagnostics", &self.diagnostics.is_some())
            .finish()
    }
}

impl PartialEq for RemoteAssociation {
    fn eq(&self, other: &Self) -> bool {
        self.remote_address == other.remote_address && self.state == other.state
    }
}

impl Eq for RemoteAssociation {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Default)]
    struct CollectingDiagnostics {
        records: Mutex<Vec<RemoteAssociationDiagnostic>>,
    }

    impl CollectingDiagnostics {
        fn records(&self) -> Vec<RemoteAssociationDiagnostic> {
            self.records
                .lock()
                .expect("association diagnostics poisoned")
                .clone()
        }
    }

    impl RemoteAssociationDiagnostics for CollectingDiagnostics {
        fn record(&self, diagnostic: RemoteAssociationDiagnostic) {
            self.records
                .lock()
                .expect("association diagnostics poisoned")
                .push(diagnostic);
        }
    }

    #[test]
    fn association_quarantine_remains_stronger_than_transport_close() {
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
            Err(RemoteError::AssociationQuarantined { .. })
        ));
        assert_eq!(
            association.state(),
            &AssociationState::Quarantined {
                remote_uid: Some(7),
                reason: "uid mismatch".to_string(),
            }
        );
    }

    #[test]
    fn association_terminal_states_are_not_reopened_by_late_handshake_or_activation() {
        let mut closed = RemoteAssociation::new("kairo://closed@127.0.0.1:25520");
        closed.close("transport stopped");

        closed.start_handshake();
        closed.activate(Some(9));

        assert_eq!(
            closed.state(),
            &AssociationState::Closed {
                reason: "transport stopped".to_string(),
            }
        );
        assert!(matches!(
            closed.ensure_send_allowed(),
            Err(RemoteError::AssociationClosed { .. })
        ));

        let mut quarantined = RemoteAssociation::new("kairo://quarantined@127.0.0.1:25521");
        quarantined.activate(Some(7));
        quarantined.quarantine(Some(7), "uid mismatch");

        quarantined.start_handshake();
        quarantined.activate(Some(9));

        assert_eq!(
            quarantined.state(),
            &AssociationState::Quarantined {
                remote_uid: Some(7),
                reason: "uid mismatch".to_string(),
            }
        );
        assert!(matches!(
            quarantined.ensure_send_allowed(),
            Err(RemoteError::AssociationQuarantined { .. })
        ));
    }

    #[test]
    fn repeated_close_preserves_first_terminal_reason() {
        let mut association = RemoteAssociation::new("kairo://remote@127.0.0.1:25520");

        association.close("runtime shutdown");
        association.close("late reader completion");

        assert_eq!(
            association.state(),
            &AssociationState::Closed {
                reason: "runtime shutdown".to_string()
            }
        );
    }

    #[test]
    fn association_reports_quarantine_diagnostics() {
        let diagnostics = Arc::new(CollectingDiagnostics::default());
        let mut association = RemoteAssociation::new("kairo://sys@127.0.0.1:25520")
            .with_diagnostics(diagnostics.clone() as Arc<dyn RemoteAssociationDiagnostics>);

        association.quarantine(Some(7), "uid mismatch");

        assert_eq!(
            diagnostics.records(),
            vec![RemoteAssociationDiagnostic::Quarantined {
                remote: "kairo://sys@127.0.0.1:25520".to_string(),
                remote_uid: Some(7),
                reason: "uid mismatch".to_string(),
            }]
        );
    }

    #[test]
    fn association_reports_close_diagnostics() {
        let diagnostics = Arc::new(CollectingDiagnostics::default());
        let mut association = RemoteAssociation::new("kairo://sys@127.0.0.1:25520")
            .with_diagnostics(diagnostics.clone() as Arc<dyn RemoteAssociationDiagnostics>);

        association.close("transport stopped");

        assert_eq!(
            diagnostics.records(),
            vec![RemoteAssociationDiagnostic::Closed {
                remote: "kairo://sys@127.0.0.1:25520".to_string(),
                reason: "transport stopped".to_string(),
            }]
        );
    }

    #[test]
    fn association_diagnostic_filter_controls_quarantine_and_close_events() {
        let diagnostics = Arc::new(CollectingDiagnostics::default());
        assert!(
            RemoteAssociationDiagnosticFilter::disabled()
                .wrap(diagnostics.clone() as Arc<dyn RemoteAssociationDiagnostics>)
                .is_none()
        );

        let observer = RemoteAssociationDiagnosticFilter::with_categories(true, false)
            .wrap(diagnostics.clone() as Arc<dyn RemoteAssociationDiagnostics>)
            .expect("association diagnostics should install observer");
        observer.record(RemoteAssociationDiagnostic::Quarantined {
            remote: "kairo://sys@127.0.0.1:25520".to_string(),
            remote_uid: Some(7),
            reason: "uid mismatch".to_string(),
        });
        observer.record(RemoteAssociationDiagnostic::Closed {
            remote: "kairo://sys@127.0.0.1:25520".to_string(),
            reason: "transport stopped".to_string(),
        });

        assert_eq!(diagnostics.records().len(), 1);
        assert!(matches!(
            diagnostics.records()[0],
            RemoteAssociationDiagnostic::Quarantined { .. }
        ));
    }
}
