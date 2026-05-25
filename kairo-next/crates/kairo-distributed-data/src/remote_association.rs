use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};

use kairo_actor::{Recipient, SendError};
use kairo_remote::{RemoteAssociationCache, RemoteOutbound};

use crate::{ReplicaId, ReplicatorRemoteEnvelope};

#[derive(Debug)]
pub enum ReplicatorRemoteAssociationError {
    MissingRoute { target: ReplicaId },
    Send { target: ReplicaId, reason: String },
}

impl Display for ReplicatorRemoteAssociationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRoute { target } => {
                write!(
                    f,
                    "no remote association route registered for replicator target {}",
                    target.as_str()
                )
            }
            Self::Send { target, reason } => {
                write!(
                    f,
                    "remote association send to replicator target {} failed: {reason}",
                    target.as_str()
                )
            }
        }
    }
}

impl std::error::Error for ReplicatorRemoteAssociationError {}

#[derive(Clone, Default)]
pub struct ReplicatorRemoteAssociationRoutes {
    routes: Arc<Mutex<BTreeMap<ReplicaId, Arc<dyn RemoteOutbound>>>>,
}

impl ReplicatorRemoteAssociationRoutes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_route(
        &self,
        target: ReplicaId,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Option<Arc<dyn RemoteOutbound>> {
        self.routes
            .lock()
            .expect("replicator remote association routes poisoned")
            .insert(target, outbound)
    }

    pub fn remove_route(&self, target: &ReplicaId) -> Option<Arc<dyn RemoteOutbound>> {
        self.routes
            .lock()
            .expect("replicator remote association routes poisoned")
            .remove(target)
    }

    pub fn clear(&self) {
        self.routes
            .lock()
            .expect("replicator remote association routes poisoned")
            .clear();
    }

    pub fn target_count(&self) -> usize {
        self.routes
            .lock()
            .expect("replicator remote association routes poisoned")
            .len()
    }

    pub fn targets(&self) -> Vec<ReplicaId> {
        self.routes
            .lock()
            .expect("replicator remote association routes poisoned")
            .keys()
            .cloned()
            .collect()
    }

    pub fn contains_target(&self, target: &ReplicaId) -> bool {
        self.routes
            .lock()
            .expect("replicator remote association routes poisoned")
            .contains_key(target)
    }

    fn route_for(&self, target: &ReplicaId) -> Option<Arc<dyn RemoteOutbound>> {
        self.routes
            .lock()
            .expect("replicator remote association routes poisoned")
            .get(target)
            .cloned()
    }
}

#[derive(Clone)]
pub struct ReplicatorRemoteAssociationOutbound {
    routes: ReplicatorRemoteAssociationRoutes,
}

impl ReplicatorRemoteAssociationOutbound {
    pub fn new(routes: ReplicatorRemoteAssociationRoutes) -> Self {
        Self { routes }
    }

    pub fn routes(&self) -> &ReplicatorRemoteAssociationRoutes {
        &self.routes
    }

    pub fn send(
        &self,
        envelope: ReplicatorRemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteAssociationError> {
        let target = envelope.target.clone();
        let outbound = self.routes.route_for(&target).ok_or_else(|| {
            ReplicatorRemoteAssociationError::MissingRoute {
                target: target.clone(),
            }
        })?;
        outbound
            .send(envelope.envelope)
            .map_err(|error| ReplicatorRemoteAssociationError::Send {
                target,
                reason: error.to_string(),
            })
    }
}

impl From<ReplicatorRemoteAssociationRoutes> for ReplicatorRemoteAssociationOutbound {
    fn from(routes: ReplicatorRemoteAssociationRoutes) -> Self {
        Self::new(routes)
    }
}

impl Recipient<ReplicatorRemoteEnvelope> for ReplicatorRemoteAssociationOutbound {
    fn tell(
        &self,
        message: ReplicatorRemoteEnvelope,
    ) -> Result<(), SendError<ReplicatorRemoteEnvelope>> {
        let rejected = message.clone();
        self.send(message)
            .map_err(|error| SendError::new(rejected, error.to_string()))
    }
}

#[derive(Clone)]
pub struct ReplicatorRemoteAssociationCacheOutbound {
    cache: RemoteAssociationCache,
}

impl ReplicatorRemoteAssociationCacheOutbound {
    pub fn new(cache: RemoteAssociationCache) -> Self {
        Self { cache }
    }

    pub fn cache(&self) -> &RemoteAssociationCache {
        &self.cache
    }

    pub fn send(
        &self,
        envelope: ReplicatorRemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteAssociationError> {
        let target = envelope.target.clone();
        self.cache
            .send(envelope.envelope)
            .map_err(|error| ReplicatorRemoteAssociationError::Send {
                target,
                reason: error.to_string(),
            })
    }
}

impl From<RemoteAssociationCache> for ReplicatorRemoteAssociationCacheOutbound {
    fn from(cache: RemoteAssociationCache) -> Self {
        Self::new(cache)
    }
}

impl Recipient<ReplicatorRemoteEnvelope> for ReplicatorRemoteAssociationCacheOutbound {
    fn tell(
        &self,
        message: ReplicatorRemoteEnvelope,
    ) -> Result<(), SendError<ReplicatorRemoteEnvelope>> {
        let rejected = message.clone();
        self.send(message)
            .map_err(|error| SendError::new(rejected, error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bytes::Bytes;
    use kairo_remote::{
        AssociationOutboundPipeline, RemoteAssociationAddress, RemoteByteSink, RemoteError,
        RemoteLaneClassifier, RemoteStreamDecoder, RemoteStreamId, Result,
        decode_remote_envelope_frame,
    };
    use kairo_serialization::{ActorRefWireData, Manifest, RemoteEnvelope, SerializedMessage};

    use super::*;

    #[derive(Default)]
    struct CollectingOutbound {
        sent: Mutex<Vec<RemoteEnvelope>>,
    }

    impl CollectingOutbound {
        fn sent(&self) -> Vec<RemoteEnvelope> {
            self.sent
                .lock()
                .expect("collecting outbound poisoned")
                .clone()
        }
    }

    impl RemoteOutbound for CollectingOutbound {
        fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
            self.sent
                .lock()
                .expect("collecting outbound poisoned")
                .push(envelope);
            Ok(())
        }
    }

    struct FailingOutbound;

    impl RemoteOutbound for FailingOutbound {
        fn send(&self, _envelope: RemoteEnvelope) -> Result<()> {
            Err(RemoteError::Outbound("association closed".to_string()))
        }
    }

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
        fn send_bytes(&self, bytes: Bytes) -> Result<()> {
            self.writes.lock().expect("byte sink poisoned").push(bytes);
            Ok(())
        }
    }

    fn envelope(target: &str, value: u8) -> ReplicatorRemoteEnvelope {
        ReplicatorRemoteEnvelope::new(
            ReplicaId::new(target),
            RemoteEnvelope::new(
                ActorRefWireData::new("kairo://ddata@peer.example.test:2552/system/ddata").unwrap(),
                Some(
                    ActorRefWireData::new("kairo://ddata@self.example.test:2551/system/ddata")
                        .unwrap(),
                ),
                SerializedMessage::new(
                    5001,
                    Manifest::new("kairo.ddata.test.AssociationEnvelope"),
                    1,
                    Bytes::from(vec![value]),
                ),
            ),
        )
    }

    fn decode_stream(writes: Vec<Bytes>) -> Vec<kairo_remote::RemoteStreamFrame> {
        let mut decoder = RemoteStreamDecoder::new();
        let mut frames = Vec::new();
        for write in writes {
            frames.extend(decoder.push(write).unwrap());
        }
        decoder.finish().unwrap();
        frames
    }

    #[test]
    fn association_outbound_routes_envelope_to_registered_remote_outbound() {
        let routes = ReplicatorRemoteAssociationRoutes::new();
        let peer = ReplicaId::new("peer");
        let collecting = Arc::new(CollectingOutbound::default());
        routes.set_route(peer.clone(), collecting.clone() as Arc<dyn RemoteOutbound>);
        let outbound = ReplicatorRemoteAssociationOutbound::new(routes);

        outbound.tell(envelope("peer", 7)).unwrap();

        let sent = collecting.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].recipient.path(),
            "kairo://ddata@peer.example.test:2552/system/ddata"
        );
        assert_eq!(sent[0].message.payload, Bytes::from_static(&[7]));
        assert_eq!(outbound.routes().targets(), vec![peer]);
    }

    #[test]
    fn association_outbound_can_drive_remote_association_pipeline() {
        let routes = ReplicatorRemoteAssociationRoutes::new();
        let control = Arc::new(CollectingByteSink::default());
        let ordinary = Arc::new(CollectingByteSink::default());
        let large = Arc::new(CollectingByteSink::default());
        let pipeline = AssociationOutboundPipeline::new(
            "kairo://ddata@peer.example.test:2552",
            RemoteLaneClassifier::default(),
            control.clone() as Arc<dyn RemoteByteSink>,
            ordinary.clone() as Arc<dyn RemoteByteSink>,
            large.clone() as Arc<dyn RemoteByteSink>,
        );
        routes.set_route(ReplicaId::new("peer"), Arc::new(pipeline));
        let outbound = ReplicatorRemoteAssociationOutbound::new(routes);

        outbound.tell(envelope("peer", 17)).unwrap();

        assert!(control.writes().is_empty());
        assert!(large.writes().is_empty());
        let stream_frames = decode_stream(ordinary.writes());
        assert_eq!(stream_frames.len(), 1);
        assert_eq!(stream_frames[0].stream_id(), RemoteStreamId::Ordinary);
        let decoded = decode_remote_envelope_frame(stream_frames[0].payload().clone()).unwrap();
        assert_eq!(
            decoded.recipient.path(),
            "kairo://ddata@peer.example.test:2552/system/ddata"
        );
        assert_eq!(decoded.message.payload, Bytes::from_static(&[17]));
    }

    #[test]
    fn association_outbound_rejects_missing_target_route() {
        let outbound =
            ReplicatorRemoteAssociationOutbound::new(ReplicatorRemoteAssociationRoutes::new());
        let message = envelope("peer", 9);

        let error = outbound
            .tell(message)
            .expect_err("missing target should reject the envelope");

        assert_eq!(error.into_message().target, ReplicaId::new("peer"));
    }

    #[test]
    fn association_outbound_reports_missing_target_reason() {
        let outbound =
            ReplicatorRemoteAssociationOutbound::new(ReplicatorRemoteAssociationRoutes::new());
        let message = envelope("peer", 9);

        let error = outbound
            .tell(message)
            .expect_err("missing target should reject the envelope");

        assert!(
            error
                .reason()
                .contains("no remote association route registered")
        );
    }

    #[test]
    fn cloned_association_routes_share_later_updates() {
        let routes = ReplicatorRemoteAssociationRoutes::new();
        let outbound = ReplicatorRemoteAssociationOutbound::new(routes.clone());
        let cloned = outbound.clone();
        let collecting = Arc::new(CollectingOutbound::default());

        routes.set_route(
            ReplicaId::new("late-peer"),
            collecting.clone() as Arc<dyn RemoteOutbound>,
        );
        cloned.tell(envelope("late-peer", 11)).unwrap();

        assert_eq!(collecting.sent().len(), 1);
    }

    #[test]
    fn association_outbound_propagates_remote_send_failure() {
        let routes = ReplicatorRemoteAssociationRoutes::new();
        routes.set_route(ReplicaId::new("peer"), Arc::new(FailingOutbound));
        let outbound = ReplicatorRemoteAssociationOutbound::new(routes);

        let error = outbound
            .tell(envelope("peer", 3))
            .expect_err("remote send failure should reject the envelope");

        assert!(error.reason().contains("association closed"));
        assert_eq!(error.into_message().target, ReplicaId::new("peer"));
    }

    #[test]
    fn association_cache_outbound_routes_by_remote_envelope_recipient() {
        let cache = RemoteAssociationCache::new();
        let collecting = Arc::new(CollectingOutbound::default());
        cache.insert_route(
            RemoteAssociationAddress::new("kairo", "ddata", "peer.example.test", Some(2552))
                .unwrap(),
            collecting.clone() as Arc<dyn RemoteOutbound>,
        );
        let outbound = ReplicatorRemoteAssociationCacheOutbound::new(cache);

        outbound.tell(envelope("peer", 23)).unwrap();

        let sent = collecting.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].recipient.path(),
            "kairo://ddata@peer.example.test:2552/system/ddata"
        );
        assert_eq!(sent[0].message.payload, Bytes::from_static(&[23]));
    }

    #[test]
    fn association_cache_outbound_reports_missing_cache_route() {
        let outbound = ReplicatorRemoteAssociationCacheOutbound::new(RemoteAssociationCache::new());
        let message = envelope("peer", 31);

        let error = outbound
            .tell(message)
            .expect_err("missing association cache route should reject the envelope");

        assert!(
            error
                .reason()
                .contains("no remote association route for `kairo://ddata@peer.example.test:2552`")
        );
        assert_eq!(error.into_message().target, ReplicaId::new("peer"));
    }
}
