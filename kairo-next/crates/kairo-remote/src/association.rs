#![deny(missing_docs)]

use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use crate::{RemoteError, Result};

/// Lifecycle state and diagnostics for one remote actor-system association.
///
/// This value is transport-independent. Callers commonly share it behind a
/// [`std::sync::Mutex`] so all outbound lanes observe the same terminal state.
#[derive(Clone)]
pub struct RemoteAssociation {
    remote_address: String,
    state: AssociationState,
    diagnostics: Option<Arc<dyn RemoteAssociationDiagnostics>>,
}

/// Lifecycle state of a remote association.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssociationState {
    /// No handshake attempt has started.
    Idle,
    /// A transport handshake is in progress.
    Handshaking,
    /// The association may carry outbound messages.
    Active {
        /// Remote actor-system incarnation learned from the handshake, if any.
        remote_uid: Option<u64>,
    },
    /// The remote incarnation is rejected until a different incarnation is
    /// established.
    Quarantined {
        /// Rejected remote actor-system incarnation, if known.
        remote_uid: Option<u64>,
        /// Diagnostic reason for quarantine.
        reason: String,
    },
    /// The association is permanently closed.
    Closed {
        /// First diagnostic reason that closed the association.
        reason: String,
    },
}

/// Operator-facing association lifecycle diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteAssociationDiagnostic {
    /// An association entered quarantine.
    Quarantined {
        /// Canonical remote actor-system address.
        remote: String,
        /// Quarantined remote incarnation, if known.
        remote_uid: Option<u64>,
        /// Quarantine reason.
        reason: String,
    },
    /// An association closed.
    Closed {
        /// Canonical remote actor-system address.
        remote: String,
        /// Close reason.
        reason: String,
    },
}

/// Observer for association quarantine and close diagnostics.
pub trait RemoteAssociationDiagnostics: Send + Sync + 'static {
    /// Records one association diagnostic.
    fn record(&self, diagnostic: RemoteAssociationDiagnostic);
}

/// Selects which association lifecycle events reach a diagnostics observer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteAssociationDiagnosticFilter {
    quarantine_events: bool,
    close_events: bool,
}

impl RemoteAssociationDiagnosticFilter {
    /// Creates a filter that enables or disables both lifecycle categories.
    pub fn new(quarantine_events: bool) -> Self {
        Self::with_categories(quarantine_events, quarantine_events)
    }

    /// Creates a filter with independent quarantine and close controls.
    pub fn with_categories(quarantine_events: bool, close_events: bool) -> Self {
        Self {
            quarantine_events,
            close_events,
        }
    }

    /// Creates a filter that observes all association lifecycle events.
    pub fn all() -> Self {
        Self::with_categories(true, true)
    }

    /// Creates a filter that disables association diagnostics.
    pub fn disabled() -> Self {
        Self::with_categories(false, false)
    }

    /// Returns whether quarantine events are observed.
    pub fn quarantine_events(&self) -> bool {
        self.quarantine_events
    }

    /// Returns whether close events are observed.
    pub fn close_events(&self) -> bool {
        self.close_events
    }

    /// Returns whether this filter observes `diagnostic`.
    pub fn observes(&self, diagnostic: &RemoteAssociationDiagnostic) -> bool {
        match diagnostic {
            RemoteAssociationDiagnostic::Quarantined { .. } => self.quarantine_events,
            RemoteAssociationDiagnostic::Closed { .. } => self.close_events,
        }
    }

    /// Applies this filter to a diagnostics observer.
    ///
    /// Returns `None` when all categories are disabled.
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
    /// Creates an idle association for a canonical remote address.
    pub fn new(remote_address: impl Into<String>) -> Self {
        Self {
            remote_address: remote_address.into(),
            state: AssociationState::Idle,
            diagnostics: None,
        }
    }

    /// Attaches an observer for quarantine and close events.
    pub fn with_diagnostics(mut self, diagnostics: Arc<dyn RemoteAssociationDiagnostics>) -> Self {
        self.diagnostics = Some(diagnostics);
        self
    }

    /// Returns the canonical remote actor-system address.
    pub fn remote_address(&self) -> &str {
        &self.remote_address
    }

    /// Returns the current lifecycle state.
    pub fn state(&self) -> &AssociationState {
        &self.state
    }

    /// Moves an idle association into handshaking.
    ///
    /// Calls in every other state are ignored.
    pub fn start_handshake(&mut self) {
        if matches!(self.state, AssociationState::Idle) {
            self.state = AssociationState::Handshaking;
        }
    }

    /// Activates an idle, handshaking, or already active association.
    ///
    /// Quarantined and closed terminal states are never reopened by a late
    /// activation.
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

    /// Quarantines the association and records the rejected incarnation.
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

    /// Closes a non-terminal association and preserves the first terminal
    /// reason.
    ///
    /// Quarantine is stronger than transport close and is therefore preserved.
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

    /// Verifies that the association can currently accept an outbound send.
    ///
    /// Idle and handshaking associations allow sends so transport queues may
    /// accept work while activation completes. Terminal states return their
    /// specific association error.
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
