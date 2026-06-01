use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};
use kairo_cluster::{
    Cluster, ClusterAssociationPeerTarget, ClusterSubscriptionEvent,
    ClusterSubscriptionInitialState, UniqueAddress,
};
use kairo_serialization::RemoteMessage;

use crate::{
    ClusterToolsTcpPeerReconnectPending, ClusterToolsTcpPeerRouteReport,
    ClusterToolsTcpPeerRuntime, ClusterToolsTcpPeerRuntimeResult,
};

const TCP_PEER_RETRY_TIMER_KEY: &str = "cluster-tools-tcp-peer-retry";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterToolsTcpPeerConnectorSettingsError {
    ZeroRetryInterval,
}

impl std::fmt::Display for ClusterToolsTcpPeerConnectorSettingsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroRetryInterval => {
                write!(
                    f,
                    "cluster-tools tcp peer connector retry interval must be non-zero"
                )
            }
        }
    }
}

impl std::error::Error for ClusterToolsTcpPeerConnectorSettingsError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterToolsTcpPeerConnectorSettings {
    retry_interval: Duration,
    initial_retry_delay: Duration,
    automatic_retry_ticks: bool,
}

impl ClusterToolsTcpPeerConnectorSettings {
    pub fn new(
        retry_interval: Duration,
    ) -> Result<Self, ClusterToolsTcpPeerConnectorSettingsError> {
        if retry_interval.is_zero() {
            return Err(ClusterToolsTcpPeerConnectorSettingsError::ZeroRetryInterval);
        }
        Ok(Self {
            retry_interval,
            initial_retry_delay: retry_interval,
            automatic_retry_ticks: true,
        })
    }

    pub fn with_initial_retry_delay(mut self, delay: Duration) -> Self {
        self.initial_retry_delay = delay;
        self
    }

    pub fn with_automatic_retry_ticks(mut self, automatic: bool) -> Self {
        self.automatic_retry_ticks = automatic;
        self
    }

    pub fn retry_interval(&self) -> Duration {
        self.retry_interval
    }
}

impl Default for ClusterToolsTcpPeerConnectorSettings {
    fn default() -> Self {
        Self {
            retry_interval: Duration::from_secs(1),
            initial_retry_delay: Duration::from_secs(1),
            automatic_retry_ticks: true,
        }
    }
}

pub struct ClusterToolsTcpPeerConnector<M>
where
    M: RemoteMessage + Send + 'static,
{
    cluster: Cluster,
    runtime: Option<ClusterToolsTcpPeerRuntime<M>>,
    settings: ClusterToolsTcpPeerConnectorSettings,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    last_report: Option<ClusterToolsTcpPeerRouteReport>,
    last_error: Option<String>,
    retry_clock: Duration,
}

impl<M> ClusterToolsTcpPeerConnector<M>
where
    M: RemoteMessage + Send + 'static,
{
    pub fn new(cluster: Cluster, runtime: ClusterToolsTcpPeerRuntime<M>) -> Self {
        Self::with_settings(
            cluster,
            runtime,
            ClusterToolsTcpPeerConnectorSettings::default(),
        )
    }

    pub fn with_settings(
        cluster: Cluster,
        runtime: ClusterToolsTcpPeerRuntime<M>,
        settings: ClusterToolsTcpPeerConnectorSettings,
    ) -> Self {
        Self {
            cluster,
            runtime: Some(runtime),
            settings,
            subscription: None,
            last_report: None,
            last_error: None,
            retry_clock: Duration::ZERO,
        }
    }

    fn snapshot(&self) -> ClusterToolsTcpPeerConnectorSnapshot {
        let runtime = self.runtime.as_ref();
        ClusterToolsTcpPeerConnectorSnapshot {
            self_node: runtime.map(|runtime| runtime.self_node().clone()),
            active_targets: runtime
                .map(ClusterToolsTcpPeerRuntime::active_peer_targets)
                .unwrap_or_default(),
            route_count: runtime.map_or(0, ClusterToolsTcpPeerRuntime::peer_route_count),
            pending_reconnects: runtime
                .map(ClusterToolsTcpPeerRuntime::pending_peer_reconnects)
                .unwrap_or_default(),
            last_report: self.last_report.clone(),
            last_error: self.last_error.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClusterToolsTcpPeerConnectorMsg {
    Cluster(ClusterSubscriptionEvent),
    RetryDuePeerRoutes {
        now: Duration,
    },
    RetryTimerTick,
    ClearRoutes,
    Snapshot {
        reply_to: ActorRef<ClusterToolsTcpPeerConnectorSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterToolsTcpPeerConnectorSnapshot {
    pub self_node: Option<UniqueAddress>,
    pub active_targets: Vec<ClusterAssociationPeerTarget>,
    pub route_count: usize,
    pub pending_reconnects: Vec<ClusterToolsTcpPeerReconnectPending>,
    pub last_report: Option<ClusterToolsTcpPeerRouteReport>,
    pub last_error: Option<String>,
}

impl<M> Actor for ClusterToolsTcpPeerConnector<M>
where
    M: RemoteMessage + Send + 'static,
{
    type Msg = ClusterToolsTcpPeerConnectorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription = ctx.message_adapter(ClusterToolsTcpPeerConnectorMsg::Cluster)?;
        self.cluster
            .subscribe_with_initial_state(
                subscription.clone(),
                ClusterSubscriptionInitialState::Snapshot,
            )
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.subscription = Some(subscription);
        if self.settings.automatic_retry_ticks {
            ctx.start_timer_with_fixed_delay(
                TCP_PEER_RETRY_TIMER_KEY,
                self.settings.initial_retry_delay,
                self.settings.retry_interval,
                ClusterToolsTcpPeerConnectorMsg::RetryTimerTick,
            );
        }
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        if let Some(subscription) = self.subscription.take() {
            let _ = self.cluster.unsubscribe(subscription);
        }
        if let Some(runtime) = self.runtime.take() {
            let _ = runtime.shutdown();
        }
        Ok(())
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterToolsTcpPeerConnectorMsg::Cluster(event) => self.apply_cluster_event(event),
            ClusterToolsTcpPeerConnectorMsg::RetryDuePeerRoutes { now } => self.retry_due(now),
            ClusterToolsTcpPeerConnectorMsg::RetryTimerTick => {
                self.retry_clock = self
                    .retry_clock
                    .saturating_add(self.settings.retry_interval);
                self.retry_due(self.retry_clock)
            }
            ClusterToolsTcpPeerConnectorMsg::ClearRoutes => self.clear_routes(),
            ClusterToolsTcpPeerConnectorMsg::Snapshot { reply_to } => reply_to
                .tell(self.snapshot())
                .map_err(|error| ActorError::Message(error.reason().to_string())),
        }
    }
}

impl<M> ClusterToolsTcpPeerConnector<M>
where
    M: RemoteMessage + Send + 'static,
{
    fn apply_cluster_event(&mut self, event: ClusterSubscriptionEvent) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "cluster-tools tcp peer connector runtime is stopped".to_string(),
            ));
        };
        let result = match event {
            ClusterSubscriptionEvent::CurrentState(state) => runtime.apply_snapshot(state),
            ClusterSubscriptionEvent::Event(event) => runtime.apply_event(event),
        };
        self.record_route_result(result);
        Ok(())
    }

    fn retry_due(&mut self, now: Duration) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "cluster-tools tcp peer connector runtime is stopped".to_string(),
            ));
        };
        let result = runtime.retry_due_peer_routes(now);
        self.record_route_result(result);
        Ok(())
    }

    fn clear_routes(&mut self) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "cluster-tools tcp peer connector runtime is stopped".to_string(),
            ));
        };
        self.last_report = Some(runtime.clear_peer_routes());
        self.last_error = None;
        Ok(())
    }

    fn record_route_result(
        &mut self,
        result: ClusterToolsTcpPeerRuntimeResult<ClusterToolsTcpPeerRouteReport>,
    ) {
        match result {
            Ok(report) => {
                self.last_report = Some(report);
                self.last_error = None;
            }
            Err(error) => {
                self.last_error = Some(error.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests;
