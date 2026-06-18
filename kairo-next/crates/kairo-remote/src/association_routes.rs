use std::sync::Arc;

use crate::{
    AssociationOutboundPipeline, RemoteAssociation, RemoteAssociationAddress,
    RemoteAssociationCache, RemoteAssociationDiagnostics, RemoteByteSink, RemoteLaneClassifier,
    RemoteOutbound,
};

#[derive(Clone)]
pub struct RemoteAssociationRouteInstaller {
    cache: RemoteAssociationCache,
    classifier: RemoteLaneClassifier,
    diagnostics: Option<Arc<dyn RemoteAssociationDiagnostics>>,
}

impl RemoteAssociationRouteInstaller {
    pub fn new(cache: RemoteAssociationCache) -> Self {
        Self {
            cache,
            classifier: RemoteLaneClassifier::default(),
            diagnostics: None,
        }
    }

    pub fn with_classifier(mut self, classifier: RemoteLaneClassifier) -> Self {
        self.classifier = classifier;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Arc<dyn RemoteAssociationDiagnostics>) -> Self {
        self.diagnostics = Some(diagnostics);
        self
    }

    pub fn cache(&self) -> &RemoteAssociationCache {
        &self.cache
    }

    pub fn insert_stream_pipeline(
        &self,
        address: RemoteAssociationAddress,
        control: Arc<dyn RemoteByteSink>,
        ordinary: Arc<dyn RemoteByteSink>,
        large: Arc<dyn RemoteByteSink>,
    ) -> RemoteAssociationRouteRegistration {
        let mut association = RemoteAssociation::new(address.to_string());
        if let Some(diagnostics) = &self.diagnostics {
            association = association.with_diagnostics(diagnostics.clone());
        }
        let pipeline = AssociationOutboundPipeline::from_association(
            association,
            self.classifier.clone(),
            control,
            ordinary,
            large,
        );
        let outbound = Arc::new(pipeline.clone()) as Arc<dyn RemoteOutbound>;
        let replaced = self
            .cache
            .insert_route(address.clone(), outbound.clone())
            .is_some();
        RemoteAssociationRouteRegistration {
            cache: self.cache.clone(),
            address,
            pipeline,
            outbound,
            replaced,
        }
    }

    pub fn remove_route(
        &self,
        address: &RemoteAssociationAddress,
    ) -> Option<Arc<dyn RemoteOutbound>> {
        let route = self.cache.remove_route(address)?;
        let _ = route.close("remote association route removed");
        Some(route)
    }
}

#[derive(Clone)]
pub struct RemoteAssociationRouteRegistration {
    cache: RemoteAssociationCache,
    address: RemoteAssociationAddress,
    pipeline: AssociationOutboundPipeline,
    outbound: Arc<dyn RemoteOutbound>,
    replaced: bool,
}

impl RemoteAssociationRouteRegistration {
    pub fn address(&self) -> &RemoteAssociationAddress {
        &self.address
    }

    pub fn pipeline(&self) -> &AssociationOutboundPipeline {
        &self.pipeline
    }

    pub fn replaced_existing_route(&self) -> bool {
        self.replaced
    }

    pub fn remove_route(&self, reason: &str) -> bool {
        let Some(route) = self
            .cache
            .remove_route_if_same(&self.address, &self.outbound)
        else {
            return false;
        };
        let _ = route.close(reason);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use kairo_serialization::{ActorRefWireData, Manifest, RemoteEnvelope, SerializedMessage};

    use super::*;
    use crate::{
        RemoteAssociationDiagnostic, RemoteAssociationDiagnostics, RemoteError,
        RemoteStreamDecoder, RemoteStreamFrame, RemoteStreamId, decode_remote_envelope_frame,
    };

    #[derive(Default)]
    struct CollectingByteSink {
        writes: Mutex<Vec<Bytes>>,
    }

    impl CollectingByteSink {
        fn writes(&self) -> Vec<Bytes> {
            self.writes.lock().expect("byte sink poisoned").clone()
        }
    }

    impl RemoteByteSink for CollectingByteSink {
        fn send_bytes(&self, bytes: Bytes) -> crate::Result<()> {
            self.writes.lock().expect("byte sink poisoned").push(bytes);
            Ok(())
        }
    }

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

    fn address() -> RemoteAssociationAddress {
        RemoteAssociationAddress::new("kairo", "remote", "127.0.0.1", Some(25520)).unwrap()
    }

    fn envelope(value: u8) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/target").unwrap(),
            None,
            SerializedMessage::new(
                777,
                Manifest::new("kairo.remote.test.AssociationRoute"),
                1,
                Bytes::from(vec![value]),
            ),
        )
    }

    fn decode_stream(writes: Vec<Bytes>) -> Vec<RemoteStreamFrame> {
        let mut decoder = RemoteStreamDecoder::new();
        let mut frames = Vec::new();
        for write in writes {
            frames.extend(decoder.push(write).unwrap());
        }
        decoder.finish().unwrap();
        frames
    }

    #[test]
    fn installer_populates_cache_with_stream_pipeline_route() {
        let cache = RemoteAssociationCache::new();
        let installer = RemoteAssociationRouteInstaller::new(cache.clone());
        let control = Arc::new(CollectingByteSink::default());
        let ordinary = Arc::new(CollectingByteSink::default());
        let large = Arc::new(CollectingByteSink::default());

        let registration = installer.insert_stream_pipeline(
            address(),
            control.clone() as Arc<dyn RemoteByteSink>,
            ordinary.clone() as Arc<dyn RemoteByteSink>,
            large.clone() as Arc<dyn RemoteByteSink>,
        );

        assert_eq!(registration.address(), &address());
        assert!(!registration.replaced_existing_route());
        assert_eq!(cache.route_count(), 1);

        cache.send(envelope(3)).unwrap();

        assert!(control.writes().is_empty());
        assert!(large.writes().is_empty());
        let frames = decode_stream(ordinary.writes());
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].stream_id(), RemoteStreamId::Ordinary);
        let decoded = decode_remote_envelope_frame(frames[0].payload().clone()).unwrap();
        assert_eq!(decoded.recipient.path(), envelope(3).recipient.path());
        assert_eq!(decoded.message.payload, Bytes::from_static(&[3]));
    }

    #[test]
    fn cache_route_uses_shared_association_state() {
        let cache = RemoteAssociationCache::new();
        let installer = RemoteAssociationRouteInstaller::new(cache.clone());
        let registration = installer.insert_stream_pipeline(
            address(),
            Arc::new(CollectingByteSink::default()),
            Arc::new(CollectingByteSink::default()),
            Arc::new(CollectingByteSink::default()),
        );

        registration
            .pipeline()
            .association()
            .lock()
            .expect("association mutex poisoned")
            .close("socket stopped");

        let error = cache
            .send(envelope(4))
            .expect_err("closed association should reject cached route send");

        assert!(matches!(error, RemoteError::AssociationClosed { .. }));
    }

    #[test]
    fn installer_reports_replaced_routes_and_can_remove_them() {
        let cache = RemoteAssociationCache::new();
        let installer = RemoteAssociationRouteInstaller::new(cache.clone());
        let sinks = || {
            (
                Arc::new(CollectingByteSink::default()) as Arc<dyn RemoteByteSink>,
                Arc::new(CollectingByteSink::default()) as Arc<dyn RemoteByteSink>,
                Arc::new(CollectingByteSink::default()) as Arc<dyn RemoteByteSink>,
            )
        };
        let (control, ordinary, large) = sinks();
        let first = installer.insert_stream_pipeline(address(), control, ordinary, large);
        let (control, ordinary, large) = sinks();
        let second = installer.insert_stream_pipeline(address(), control, ordinary, large);

        assert!(!first.replaced_existing_route());
        assert!(second.replaced_existing_route());
        assert_eq!(cache.route_count(), 1);

        assert!(installer.remove_route(&address()).is_some());
        assert_eq!(cache.route_count(), 0);
    }

    #[test]
    fn stale_registration_removal_does_not_remove_replacement_route() {
        let cache = RemoteAssociationCache::new();
        let installer = RemoteAssociationRouteInstaller::new(cache.clone());
        let sinks = || {
            (
                Arc::new(CollectingByteSink::default()) as Arc<dyn RemoteByteSink>,
                Arc::new(CollectingByteSink::default()) as Arc<dyn RemoteByteSink>,
                Arc::new(CollectingByteSink::default()) as Arc<dyn RemoteByteSink>,
            )
        };
        let (control, ordinary, large) = sinks();
        let stale = installer.insert_stream_pipeline(address(), control, ordinary, large);
        let (control, ordinary, large) = sinks();
        let replacement = installer.insert_stream_pipeline(address(), control, ordinary, large);

        assert!(!stale.remove_route("old reader finished"));
        assert_eq!(cache.route_count(), 1);
        assert!(replacement.remove_route("replacement reader finished"));
        assert_eq!(cache.route_count(), 0);
    }

    #[test]
    fn installer_applies_diagnostics_to_stream_pipeline_association() {
        let cache = RemoteAssociationCache::new();
        let diagnostics = Arc::new(CollectingDiagnostics::default());
        let installer = RemoteAssociationRouteInstaller::new(cache)
            .with_diagnostics(diagnostics.clone() as Arc<dyn RemoteAssociationDiagnostics>);
        let registration = installer.insert_stream_pipeline(
            address(),
            Arc::new(CollectingByteSink::default()),
            Arc::new(CollectingByteSink::default()),
            Arc::new(CollectingByteSink::default()),
        );

        registration.pipeline().close("peer route removed").unwrap();

        assert_eq!(
            diagnostics.records(),
            vec![RemoteAssociationDiagnostic::Closed {
                remote: "kairo://remote@127.0.0.1:25520".to_string(),
                reason: "peer route removed".to_string(),
            }]
        );
    }

    #[test]
    fn installer_remove_route_closes_removed_stream_pipeline_association() {
        let cache = RemoteAssociationCache::new();
        let diagnostics = Arc::new(CollectingDiagnostics::default());
        let installer = RemoteAssociationRouteInstaller::new(cache.clone())
            .with_diagnostics(diagnostics.clone() as Arc<dyn RemoteAssociationDiagnostics>);
        installer.insert_stream_pipeline(
            address(),
            Arc::new(CollectingByteSink::default()),
            Arc::new(CollectingByteSink::default()),
            Arc::new(CollectingByteSink::default()),
        );

        assert!(installer.remove_route(&address()).is_some());

        assert_eq!(cache.route_count(), 0);
        assert_eq!(
            diagnostics.records(),
            vec![RemoteAssociationDiagnostic::Closed {
                remote: "kairo://remote@127.0.0.1:25520".to_string(),
                reason: "remote association route removed".to_string(),
            }]
        );
    }
}
