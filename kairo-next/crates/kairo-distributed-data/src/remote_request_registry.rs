use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};

use kairo_serialization::RemoteEnvelope;

use crate::{ReplicaId, ReplicatorRemoteRequestError, ReplicatorRemoteRequestReceiver};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicatorRemoteRequestRegistrationError {
    BlankManifest,
    DuplicateManifest {
        manifest: String,
    },
    PathCollision {
        path: String,
        registered_manifest: String,
        requested_manifest: String,
    },
}

impl Display for ReplicatorRemoteRequestRegistrationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlankManifest => write!(f, "replicator CRDT data manifest must not be blank"),
            Self::DuplicateManifest { manifest } => {
                write!(
                    f,
                    "replicator CRDT data manifest `{manifest}` is already registered"
                )
            }
            Self::PathCollision {
                path,
                registered_manifest,
                requested_manifest,
            } => write!(
                f,
                "replicator remote path `{path}` for manifest `{requested_manifest}` is already owned by `{registered_manifest}`"
            ),
        }
    }
}

impl std::error::Error for ReplicatorRemoteRequestRegistrationError {}

struct RegisteredRequestReceiver {
    manifest: String,
    receiver: Arc<dyn ReplicatorRemoteRequestReceiver>,
}

/// Shared ordinary-lane router for independently typed CRDT family receivers.
#[derive(Clone, Default)]
pub struct ReplicatorRemoteRequestRegistry {
    by_path: Arc<Mutex<BTreeMap<String, RegisteredRequestReceiver>>>,
}

impl ReplicatorRemoteRequestRegistry {
    pub fn register(
        &self,
        manifest: impl Into<String>,
        recipient_path: impl Into<String>,
        receiver: Arc<dyn ReplicatorRemoteRequestReceiver>,
    ) -> Result<(), ReplicatorRemoteRequestRegistrationError> {
        let manifest = manifest.into();
        if manifest.trim().is_empty() {
            return Err(ReplicatorRemoteRequestRegistrationError::BlankManifest);
        }
        let recipient_path = recipient_path.into();
        let mut receivers = self
            .by_path
            .lock()
            .expect("replicator remote request registry lock poisoned");
        if receivers.values().any(|entry| entry.manifest == manifest) {
            return Err(ReplicatorRemoteRequestRegistrationError::DuplicateManifest { manifest });
        }
        if let Some(registered) = receivers.get(&recipient_path) {
            return Err(ReplicatorRemoteRequestRegistrationError::PathCollision {
                path: recipient_path,
                registered_manifest: registered.manifest.clone(),
                requested_manifest: manifest,
            });
        }
        receivers.insert(
            recipient_path,
            RegisteredRequestReceiver { manifest, receiver },
        );
        Ok(())
    }

    pub fn registered_manifests(&self) -> Vec<String> {
        self.by_path
            .lock()
            .expect("replicator remote request registry lock poisoned")
            .values()
            .map(|entry| entry.manifest.clone())
            .collect()
    }
}

impl ReplicatorRemoteRequestReceiver for ReplicatorRemoteRequestRegistry {
    fn receive_request_from(
        &self,
        from: ReplicaId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteRequestError> {
        let recipient_path = envelope.recipient.path().to_string();
        let receiver = self
            .by_path
            .lock()
            .expect("replicator remote request registry lock poisoned")
            .get(&recipient_path)
            .map(|entry| Arc::clone(&entry.receiver))
            .ok_or(ReplicatorRemoteRequestError::UnknownRecipient(
                recipient_path,
            ))?;
        receiver.receive_request_from(from, envelope)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kairo_serialization::{ActorRefWireData, Manifest, SerializedMessage};

    use super::*;

    #[derive(Default)]
    struct RecordingReceiver {
        replicas: Mutex<Vec<ReplicaId>>,
    }

    impl ReplicatorRemoteRequestReceiver for RecordingReceiver {
        fn receive_request_from(
            &self,
            from: ReplicaId,
            _envelope: RemoteEnvelope,
        ) -> Result<(), ReplicatorRemoteRequestError> {
            self.replicas.lock().unwrap().push(from);
            Ok(())
        }
    }

    fn envelope(path: &str) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new(format!("kairo://node@127.0.0.1:25520{path}")).unwrap(),
            None,
            SerializedMessage::new(1, Manifest::new("request"), 1, Bytes::new()),
        )
    }

    #[test]
    fn registry_routes_requests_by_canonical_recipient_path() {
        let registry = ReplicatorRemoteRequestRegistry::default();
        let counters = Arc::new(RecordingReceiver::default());
        let sets = Arc::new(RecordingReceiver::default());
        registry
            .register(
                "counter",
                "kairo://node@127.0.0.1:25520/system/ddata-counter",
                counters.clone(),
            )
            .unwrap();
        registry
            .register(
                "set",
                "kairo://node@127.0.0.1:25520/system/ddata-set",
                sets.clone(),
            )
            .unwrap();

        registry
            .receive_request_from(ReplicaId::new("node-a"), envelope("/system/ddata-set"))
            .unwrap();

        assert!(counters.replicas.lock().unwrap().is_empty());
        assert_eq!(
            *sets.replicas.lock().unwrap(),
            vec![ReplicaId::new("node-a")]
        );
    }

    #[test]
    fn registry_rejects_duplicate_manifests_and_path_collisions() {
        let registry = ReplicatorRemoteRequestRegistry::default();
        registry
            .register(
                "counter",
                "kairo://node@127.0.0.1:25520/system/ddata-family",
                Arc::new(RecordingReceiver::default()),
            )
            .unwrap();

        assert!(matches!(
            registry.register(
                "counter",
                "kairo://node@127.0.0.1:25520/system/ddata-other",
                Arc::new(RecordingReceiver::default())
            ),
            Err(ReplicatorRemoteRequestRegistrationError::DuplicateManifest { .. })
        ));
        assert!(matches!(
            registry.register(
                "set",
                "kairo://node@127.0.0.1:25520/system/ddata-family",
                Arc::new(RecordingReceiver::default())
            ),
            Err(ReplicatorRemoteRequestRegistrationError::PathCollision { .. })
        ));
    }

    #[test]
    fn registry_rejects_unknown_recipient_without_fallback() {
        let error = ReplicatorRemoteRequestRegistry::default()
            .receive_request_from(ReplicaId::new("node-a"), envelope("/system/ddata-missing"))
            .unwrap_err();

        assert!(matches!(
            error,
            ReplicatorRemoteRequestError::UnknownRecipient(path)
                if path == "kairo://node@127.0.0.1:25520/system/ddata-missing"
        ));
    }
}
