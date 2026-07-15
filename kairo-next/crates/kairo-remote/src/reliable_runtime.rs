use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use kairo_actor::{Actor, ActorResult, Context, Props};
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{
    AssociationState, ReliableSystemAck, ReliableSystemEnvelope, ReliableSystemNack,
    ReliableSystemReceiveOutcome, ReliableSystemReceiver, ReliableSystemSender,
    RemoteAssociationAddress, RemoteAssociationCache, RemoteAssociationRegistry, RemoteError,
    RemoteFrameHandler, RemoteOutbound, RemoteStreamId, Result, decode_remote_envelope_frame,
};

const RELIABLE_DELIVERY_ACTOR_NAME: &str = "remote-reliable-delivery";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReliableSystemDeliverySettings {
    pub buffer_capacity: usize,
    pub retry_interval: Duration,
    pub give_up_after: Duration,
}

impl ReliableSystemDeliverySettings {
    pub fn new(
        buffer_capacity: usize,
        retry_interval: Duration,
        give_up_after: Duration,
    ) -> Result<Self> {
        if buffer_capacity == 0 {
            return Err(RemoteError::InvalidReliableSystemDelivery(
                "runtime buffer capacity must be greater than zero".to_string(),
            ));
        }
        if retry_interval.is_zero() {
            return Err(RemoteError::InvalidReliableSystemDelivery(
                "runtime retry interval must be greater than zero".to_string(),
            ));
        }
        if give_up_after < retry_interval {
            return Err(RemoteError::InvalidReliableSystemDelivery(
                "runtime give-up duration must not be shorter than the retry interval".to_string(),
            ));
        }
        Ok(Self {
            buffer_capacity,
            retry_interval,
            give_up_after,
        })
    }
}

impl Default for ReliableSystemDeliverySettings {
    fn default() -> Self {
        Self {
            buffer_capacity: 256,
            retry_interval: Duration::from_millis(500),
            give_up_after: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReliableSystemDeliveryFailure {
    pub remote: RemoteAssociationAddress,
    pub remote_uid: u64,
    pub envelope: RemoteEnvelope,
    pub reason: String,
}

pub trait ReliableSystemDeliveryObserver: Send + Sync + 'static {
    fn delivery_failed(&self, failure: ReliableSystemDeliveryFailure);
}

impl<F> ReliableSystemDeliveryObserver for F
where
    F: Fn(ReliableSystemDeliveryFailure) + Send + Sync + 'static,
{
    fn delivery_failed(&self, failure: ReliableSystemDeliveryFailure) {
        self(failure);
    }
}

pub(crate) struct IgnoreReliableSystemDeliveryFailures;

impl ReliableSystemDeliveryObserver for IgnoreReliableSystemDeliveryFailures {
    fn delivery_failed(&self, _failure: ReliableSystemDeliveryFailure) {}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReliableSystemDeliveryStats {
    pub associations: usize,
    pub unacknowledged: usize,
}

struct ReliableSenderState {
    sender: ReliableSystemSender,
    last_ack_at: Instant,
}

#[derive(Default)]
struct ReliableRuntimeState {
    senders: BTreeMap<RemoteAssociationAddress, ReliableSenderState>,
    receivers: BTreeMap<RemoteAssociationAddress, ReliableSystemReceiver>,
}

pub(crate) struct ReliableSystemDeliveryRuntime {
    local_address: RemoteAssociationAddress,
    local_uid: u64,
    registry: Arc<Registry>,
    raw_outbound: RemoteAssociationCache,
    associations: RemoteAssociationRegistry,
    reliable_manifests: HashSet<String>,
    settings: ReliableSystemDeliverySettings,
    observer: Arc<dyn ReliableSystemDeliveryObserver>,
    state: Mutex<ReliableRuntimeState>,
}

impl ReliableSystemDeliveryRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        local_address: RemoteAssociationAddress,
        local_uid: u64,
        registry: Arc<Registry>,
        raw_outbound: RemoteAssociationCache,
        associations: RemoteAssociationRegistry,
        reliable_manifests: HashSet<String>,
        settings: ReliableSystemDeliverySettings,
        observer: Arc<dyn ReliableSystemDeliveryObserver>,
    ) -> Self {
        Self {
            local_address,
            local_uid,
            registry,
            raw_outbound,
            associations,
            reliable_manifests,
            settings,
            observer,
            state: Mutex::new(ReliableRuntimeState::default()),
        }
    }

    pub(crate) fn stats(&self) -> ReliableSystemDeliveryStats {
        let state = self
            .state
            .lock()
            .expect("reliable system runtime lock poisoned");
        ReliableSystemDeliveryStats {
            associations: state.senders.len(),
            unacknowledged: state
                .senders
                .values()
                .map(|state| state.sender.pending_len())
                .sum(),
        }
    }

    fn send_selected(&self, envelope: RemoteEnvelope) -> Result<()> {
        if !self
            .reliable_manifests
            .contains(envelope.message.manifest.as_str())
        {
            return self.raw_outbound.send(envelope);
        }
        let address = self
            .raw_outbound
            .address_for_recipient(&envelope.recipient)?;
        let remote_uid = self.associations.uid_for_address(&address).ok_or_else(|| {
            RemoteError::InvalidReliableSystemDelivery(format!(
                "reliable manifest `{}` has no completed association UID for {address}",
                envelope.message.manifest.as_str()
            ))
        })?;

        let mut incarnation_failures = Vec::new();
        let send_result = {
            let mut state = self
                .state
                .lock()
                .expect("reliable system runtime lock poisoned");
            let sender =
                state
                    .senders
                    .entry(address.clone())
                    .or_insert_with(|| ReliableSenderState {
                        sender: ReliableSystemSender::new(
                            self.local_uid,
                            remote_uid,
                            self.settings.buffer_capacity,
                        )
                        .expect("validated reliable-system buffer capacity"),
                        last_ack_at: Instant::now(),
                    });
            if sender.sender.remote_uid() != remote_uid {
                let previous_uid = sender.sender.remote_uid();
                incarnation_failures = sender
                    .sender
                    .reset_remote_uid(remote_uid)
                    .into_iter()
                    .map(|envelope| ReliableSystemDeliveryFailure {
                        remote: address.clone(),
                        remote_uid: previous_uid,
                        envelope,
                        reason: "remote association incarnation changed".to_string(),
                    })
                    .collect();
                sender.last_ack_at = Instant::now();
            }
            if sender.sender.pending_len() == 0 {
                sender.last_ack_at = Instant::now();
            }
            sender.sender.retain(envelope).and_then(|reliable| {
                self.wrap_reliable(reliable)
                    .and_then(|envelope| self.raw_outbound.send(envelope))
            })
        };

        for failure in incarnation_failures {
            self.observer.delivery_failed(failure);
        }

        let overflow = matches!(
            send_result,
            Err(RemoteError::ReliableSystemBufferFull { .. })
        ) || matches!(
            send_result,
            Err(RemoteError::OutboundLaneQueueFull { ref lane, .. }) if lane == "control"
        );
        if overflow {
            self.fail_association(&address, remote_uid, "reliable system delivery overflow");
        }
        send_result
    }

    fn receive_frame(
        &self,
        remote: &RemoteAssociationAddress,
        inner: &dyn RemoteFrameHandler,
        stream_id: RemoteStreamId,
        frame: Bytes,
    ) -> Result<()> {
        let envelope = decode_remote_envelope_frame(frame)?;
        let manifest = envelope.message.manifest.as_str();
        if manifest == ReliableSystemEnvelope::MANIFEST {
            self.require_control(stream_id, manifest)?;
            return self.receive_reliable(remote, inner, envelope);
        }
        if manifest == ReliableSystemAck::MANIFEST {
            self.require_control(stream_id, manifest)?;
            let ack = self
                .registry
                .deserialize::<ReliableSystemAck>(envelope.message)?;
            return self.receive_ack(remote, ack);
        }
        if manifest == ReliableSystemNack::MANIFEST {
            self.require_control(stream_id, manifest)?;
            let nack = self
                .registry
                .deserialize::<ReliableSystemNack>(envelope.message)?;
            return self.receive_nack(remote, nack);
        }
        inner.handle_frame(stream_id, crate::encode_remote_envelope_frame(&envelope)?)
    }

    fn receive_reliable(
        &self,
        remote: &RemoteAssociationAddress,
        inner: &dyn RemoteFrameHandler,
        envelope: RemoteEnvelope,
    ) -> Result<()> {
        let reliable = self
            .registry
            .deserialize::<ReliableSystemEnvelope>(envelope.message)?;
        let active_uid = self.validate_remote_uid(remote, reliable.from_uid)?;
        if reliable.to_uid != self.local_uid {
            return self.invalid_transition(
                remote,
                active_uid,
                format!(
                    "reliable envelope target UID {} does not match local UID {}",
                    reliable.to_uid, self.local_uid
                ),
            );
        }
        let outcome = {
            let mut state = self
                .state
                .lock()
                .expect("reliable system runtime lock poisoned");
            let receiver = state
                .receivers
                .entry(remote.clone())
                .or_insert_with(|| ReliableSystemReceiver::new(self.local_uid, active_uid));
            if receiver.remote_uid() != active_uid {
                *receiver = ReliableSystemReceiver::new(self.local_uid, active_uid);
            }
            receiver.receive(reliable)
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                return self.invalid_transition(remote, active_uid, error.to_string());
            }
        };
        match outcome {
            ReliableSystemReceiveOutcome::Deliver { envelope, ack } => {
                inner.handle_frame(
                    RemoteStreamId::Control,
                    crate::encode_remote_envelope_frame(&envelope)?,
                )?;
                self.send_reply(remote, &ack)
            }
            ReliableSystemReceiveOutcome::Duplicate { ack } => self.send_reply(remote, &ack),
            ReliableSystemReceiveOutcome::Gap { nack } => self.send_reply(remote, &nack),
        }
    }

    fn receive_ack(&self, remote: &RemoteAssociationAddress, ack: ReliableSystemAck) -> Result<()> {
        let active_uid = self.validate_remote_uid(remote, ack.from_uid)?;
        let result = {
            let mut state = self
                .state
                .lock()
                .expect("reliable system runtime lock poisoned");
            let sender = state.senders.get_mut(remote).ok_or_else(|| {
                RemoteError::InvalidReliableSystemDelivery(format!(
                    "ack for {remote} has no reliable sender state"
                ))
            })?;
            let result = sender.sender.acknowledge(&ack);
            if result.is_ok() {
                sender.last_ack_at = Instant::now();
            }
            result
        };
        result
            .map(|_| ())
            .or_else(|error| self.invalid_transition(remote, active_uid, error.to_string()))
    }

    fn receive_nack(
        &self,
        remote: &RemoteAssociationAddress,
        nack: ReliableSystemNack,
    ) -> Result<()> {
        let active_uid = self.validate_remote_uid(remote, nack.from_uid)?;
        let result = {
            let mut state = self
                .state
                .lock()
                .expect("reliable system runtime lock poisoned");
            let sender = state.senders.get_mut(remote).ok_or_else(|| {
                RemoteError::InvalidReliableSystemDelivery(format!(
                    "nack for {remote} has no reliable sender state"
                ))
            })?;
            let result = sender.sender.negative_acknowledge(&nack);
            if result.is_ok() {
                sender.last_ack_at = Instant::now();
            }
            result
        };
        result
            .map(|_| ())
            .or_else(|error| self.invalid_transition(remote, active_uid, error.to_string()))
    }

    pub(crate) fn retry_tick(&self) {
        let now = Instant::now();
        let mut give_up = Vec::new();
        let mut send_failures = Vec::new();
        {
            let state = self
                .state
                .lock()
                .expect("reliable system runtime lock poisoned");
            for (address, sender) in &state.senders {
                if sender.sender.pending_len() == 0 {
                    continue;
                }
                if now.duration_since(sender.last_ack_at) >= self.settings.give_up_after {
                    give_up.push((address.clone(), sender.sender.remote_uid()));
                    continue;
                }
                for reliable in sender.sender.retry_batch() {
                    let result = self
                        .wrap_reliable(reliable)
                        .and_then(|envelope| self.raw_outbound.send(envelope));
                    if result.is_err() {
                        send_failures.push((address.clone(), sender.sender.remote_uid()));
                        break;
                    }
                }
            }
        }
        for (address, remote_uid) in give_up {
            self.fail_association(
                &address,
                remote_uid,
                "reliable system acknowledgement give-up deadline elapsed",
            );
        }
        for (address, remote_uid) in send_failures {
            if self.association_is_quarantined(&address, remote_uid) {
                self.fail_association(
                    &address,
                    remote_uid,
                    "reliable system retry failed on quarantined association",
                );
            }
        }
    }

    fn wrap_reliable(&self, reliable: ReliableSystemEnvelope) -> Result<RemoteEnvelope> {
        let recipient = reliable.envelope.recipient.clone();
        let sender = reliable.envelope.sender.clone();
        Ok(RemoteEnvelope::new(
            recipient,
            sender,
            self.registry.serialize(&reliable)?,
        ))
    }

    fn send_reply<M: RemoteMessage>(
        &self,
        remote: &RemoteAssociationAddress,
        message: &M,
    ) -> Result<()> {
        self.raw_outbound.send(RemoteEnvelope::new(
            system_delivery_ref(remote)?,
            Some(system_delivery_ref(&self.local_address)?),
            self.registry.serialize(message)?,
        ))
    }

    fn validate_remote_uid(
        &self,
        remote: &RemoteAssociationAddress,
        claimed_uid: u64,
    ) -> Result<u64> {
        let active_uid = self.associations.uid_for_address(remote).ok_or_else(|| {
            RemoteError::InvalidReliableSystemDelivery(format!(
                "reliable frame from {remote} has no completed association UID"
            ))
        })?;
        if claimed_uid != active_uid {
            return self.invalid_transition(
                remote,
                active_uid,
                format!("reliable frame UID {claimed_uid} does not match active UID {active_uid}"),
            );
        }
        Ok(active_uid)
    }

    fn require_control(&self, stream_id: RemoteStreamId, manifest: &str) -> Result<()> {
        if stream_id == RemoteStreamId::Control {
            Ok(())
        } else {
            Err(RemoteError::Inbound(format!(
                "reliable protocol manifest `{manifest}` arrived on {stream_id:?} lane"
            )))
        }
    }

    fn invalid_transition<T>(
        &self,
        remote: &RemoteAssociationAddress,
        remote_uid: u64,
        reason: String,
    ) -> Result<T> {
        self.fail_association(remote, remote_uid, &reason);
        Err(RemoteError::InvalidReliableSystemDelivery(reason))
    }

    fn association_is_quarantined(
        &self,
        remote: &RemoteAssociationAddress,
        remote_uid: u64,
    ) -> bool {
        self.associations
            .association_for_address(remote)
            .is_some_and(|association| {
                matches!(
                    association
                        .lock()
                        .expect("remote association lock poisoned")
                        .state(),
                    AssociationState::Quarantined {
                        remote_uid: Some(uid),
                        ..
                    } if *uid == remote_uid
                )
            })
    }

    fn fail_association(&self, remote: &RemoteAssociationAddress, remote_uid: u64, reason: &str) {
        let quarantined = self
            .associations
            .quarantine_if_uid(remote, remote_uid, reason);
        let failed = self
            .state
            .lock()
            .expect("reliable system runtime lock poisoned")
            .senders
            .remove(remote)
            .map(|mut state| state.sender.reset_remote_uid(remote_uid))
            .unwrap_or_default();
        for envelope in failed {
            self.observer
                .delivery_failed(ReliableSystemDeliveryFailure {
                    remote: remote.clone(),
                    remote_uid,
                    envelope,
                    reason: reason.to_string(),
                });
        }
        if quarantined {
            self.raw_outbound
                .remove_route_and_close(remote, reason)
                .map(|result| result.ok());
        }
    }
}

impl RemoteOutbound for ReliableSystemDeliveryRuntime {
    fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
        self.send_selected(envelope)
    }
}

pub(crate) struct ReliableSystemInboundHandler {
    runtime: Arc<ReliableSystemDeliveryRuntime>,
    inner: Arc<dyn RemoteFrameHandler>,
    remote: RemoteAssociationAddress,
}

impl ReliableSystemInboundHandler {
    pub(crate) fn new(
        runtime: Arc<ReliableSystemDeliveryRuntime>,
        inner: Arc<dyn RemoteFrameHandler>,
        remote: RemoteAssociationAddress,
    ) -> Self {
        Self {
            runtime,
            inner,
            remote,
        }
    }
}

impl RemoteFrameHandler for ReliableSystemInboundHandler {
    fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> Result<()> {
        self.runtime
            .receive_frame(&self.remote, self.inner.as_ref(), stream_id, frame)
    }
}

pub(crate) enum ReliableSystemRuntimeCommand {
    Tick,
}

pub(crate) struct ReliableSystemRuntimeActor {
    runtime: Arc<ReliableSystemDeliveryRuntime>,
    retry_interval: Duration,
}

impl ReliableSystemRuntimeActor {
    pub(crate) fn props(runtime: Arc<ReliableSystemDeliveryRuntime>) -> Props<Self> {
        let retry_interval = runtime.settings.retry_interval;
        Props::new(move || Self {
            runtime,
            retry_interval,
        })
    }

    fn schedule_tick(&self, ctx: &Context<ReliableSystemRuntimeCommand>) {
        ctx.schedule_once_self(self.retry_interval, ReliableSystemRuntimeCommand::Tick);
    }
}

impl Actor for ReliableSystemRuntimeActor {
    type Msg = ReliableSystemRuntimeCommand;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.schedule_tick(ctx);
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ReliableSystemRuntimeCommand::Tick => {
                self.runtime.retry_tick();
                self.schedule_tick(ctx);
                Ok(())
            }
        }
    }
}

pub(crate) fn reliable_delivery_actor_name() -> &'static str {
    RELIABLE_DELIVERY_ACTOR_NAME
}

pub(crate) fn is_reliable_protocol_manifest(manifest: &str) -> bool {
    matches!(
        manifest,
        ReliableSystemEnvelope::MANIFEST
            | ReliableSystemAck::MANIFEST
            | ReliableSystemNack::MANIFEST
    )
}

fn system_delivery_ref(address: &RemoteAssociationAddress) -> Result<ActorRefWireData> {
    ActorRefWireData::new(format!("{address}/system/{RELIABLE_DELIVERY_ACTOR_NAME}"))
        .map_err(RemoteError::from)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    use super::*;
    use crate::{WatchRemote, register_remote_protocol_codecs};

    #[derive(Default)]
    struct CollectingOutbound {
        sent: Mutex<Vec<RemoteEnvelope>>,
        closed: AtomicUsize,
    }

    impl RemoteOutbound for CollectingOutbound {
        fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
            self.sent
                .lock()
                .expect("collecting reliable outbound poisoned")
                .push(envelope);
            Ok(())
        }

        fn close(&self, _reason: &str) -> Result<()> {
            self.closed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Default)]
    struct CollectingFailures(Mutex<Vec<ReliableSystemDeliveryFailure>>);

    impl ReliableSystemDeliveryObserver for CollectingFailures {
        fn delivery_failed(&self, failure: ReliableSystemDeliveryFailure) {
            self.0
                .lock()
                .expect("collecting reliable failures poisoned")
                .push(failure);
        }
    }

    fn address(system: &str, port: u16) -> RemoteAssociationAddress {
        RemoteAssociationAddress::new("kairo", system, "127.0.0.1", Some(port)).unwrap()
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_remote_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn watch_envelope(registry: &Registry, remote: &RemoteAssociationAddress) -> RemoteEnvelope {
        let watchee = ActorRefWireData::new(format!("{remote}/user/target#1")).unwrap();
        let watcher =
            ActorRefWireData::new("kairo://local@127.0.0.1:25520/user/watcher#2").unwrap();
        RemoteEnvelope::new(
            system_delivery_ref(remote).unwrap(),
            Some(watcher.clone()),
            registry
                .serialize(&WatchRemote { watchee, watcher })
                .unwrap(),
        )
    }

    fn runtime(
        settings: ReliableSystemDeliverySettings,
    ) -> (
        Arc<ReliableSystemDeliveryRuntime>,
        Arc<Registry>,
        RemoteAssociationRegistry,
        RemoteAssociationAddress,
        Arc<CollectingOutbound>,
        Arc<CollectingFailures>,
    ) {
        let registry = registry();
        let associations = RemoteAssociationRegistry::new();
        let remote = address("remote", 25521);
        associations.complete_handshake(remote.clone(), 22).unwrap();
        let raw = RemoteAssociationCache::new();
        let collecting = Arc::new(CollectingOutbound::default());
        raw.insert_route(remote.clone(), collecting.clone());
        let failures = Arc::new(CollectingFailures::default());
        let runtime = Arc::new(ReliableSystemDeliveryRuntime::new(
            address("local", 25520),
            11,
            registry.clone(),
            raw,
            associations.clone(),
            [WatchRemote::MANIFEST.to_string()].into_iter().collect(),
            settings,
            failures.clone(),
        ));
        (
            runtime,
            registry,
            associations,
            remote,
            collecting,
            failures,
        )
    }

    #[test]
    fn runtime_retries_retained_system_envelope_and_clears_on_ack() {
        let (runtime, registry, _associations, remote, collecting, _failures) = runtime(
            ReliableSystemDeliverySettings::new(
                4,
                Duration::from_millis(1),
                Duration::from_secs(1),
            )
            .unwrap(),
        );

        runtime.send(watch_envelope(&registry, &remote)).unwrap();
        runtime.retry_tick();

        let sent = collecting
            .sent
            .lock()
            .expect("collecting reliable outbound poisoned")
            .clone();
        assert_eq!(sent.len(), 2);
        let first = registry
            .deserialize::<ReliableSystemEnvelope>(sent[0].message.clone())
            .unwrap();
        let retry = registry
            .deserialize::<ReliableSystemEnvelope>(sent[1].message.clone())
            .unwrap();
        assert_eq!(first, retry);
        assert_eq!(first.sequence_nr, 1);
        assert_eq!(runtime.stats().unacknowledged, 1);

        runtime
            .receive_ack(
                &remote,
                ReliableSystemAck {
                    from_uid: 22,
                    to_uid: 11,
                    sequence_nr: 1,
                },
            )
            .unwrap();

        assert_eq!(runtime.stats().unacknowledged, 0);
    }

    #[test]
    fn runtime_give_up_closes_exact_incarnation_and_reports_retained_failure() {
        let (runtime, registry, associations, remote, collecting, failures) = runtime(
            ReliableSystemDeliverySettings::new(
                4,
                Duration::from_millis(1),
                Duration::from_millis(1),
            )
            .unwrap(),
        );
        runtime.send(watch_envelope(&registry, &remote)).unwrap();
        thread::sleep(Duration::from_millis(3));

        runtime.retry_tick();

        assert_eq!(runtime.stats().unacknowledged, 0);
        assert_eq!(collecting.closed.load(Ordering::SeqCst), 1);
        assert_eq!(
            failures
                .0
                .lock()
                .expect("collecting reliable failures poisoned")
                .len(),
            1
        );
        assert!(associations.complete_handshake(remote.clone(), 22).is_err());
        assert!(associations.complete_handshake(remote, 23).is_ok());
    }

    #[test]
    fn runtime_buffer_overflow_quarantines_without_blocking_or_dropping_retained_state() {
        let (runtime, registry, associations, remote, collecting, failures) = runtime(
            ReliableSystemDeliverySettings::new(
                1,
                Duration::from_millis(10),
                Duration::from_secs(1),
            )
            .unwrap(),
        );
        runtime.send(watch_envelope(&registry, &remote)).unwrap();

        let error = runtime
            .send(watch_envelope(&registry, &remote))
            .expect_err("second retained system message should overflow capacity one");

        assert!(matches!(
            error,
            RemoteError::ReliableSystemBufferFull { capacity: 1 }
        ));
        assert_eq!(runtime.stats().unacknowledged, 0);
        assert_eq!(collecting.closed.load(Ordering::SeqCst), 1);
        assert_eq!(
            failures
                .0
                .lock()
                .expect("collecting reliable failures poisoned")
                .len(),
            1
        );
        assert!(associations.complete_handshake(remote, 22).is_err());
    }

    #[test]
    fn stale_give_up_state_does_not_close_route_for_new_remote_uid() {
        let (runtime, registry, associations, remote, collecting, failures) = runtime(
            ReliableSystemDeliverySettings::new(
                4,
                Duration::from_millis(1),
                Duration::from_millis(1),
            )
            .unwrap(),
        );
        runtime.send(watch_envelope(&registry, &remote)).unwrap();
        associations.complete_handshake(remote.clone(), 23).unwrap();
        thread::sleep(Duration::from_millis(3));

        runtime.retry_tick();

        assert_eq!(collecting.closed.load(Ordering::SeqCst), 0);
        assert_eq!(associations.uid_for_address(&remote), Some(23));
        assert_eq!(
            failures
                .0
                .lock()
                .expect("collecting reliable failures poisoned")[0]
                .remote_uid,
            22
        );
    }
}
