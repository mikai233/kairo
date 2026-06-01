use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};

use crate::{
    Cluster, ClusterSubscriptionEvent, ClusterSubscriptionInitialState,
    ClusterTcpPeerReconnectPending, ClusterTcpPeerRouteReport, ClusterTcpPeerRuntime,
    UniqueAddress,
};

const TCP_PEER_RETRY_TIMER_KEY: &str = "cluster-tcp-peer-retry";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterTcpPeerConnectorSettingsError {
    ZeroRetryInterval,
}

impl std::fmt::Display for ClusterTcpPeerConnectorSettingsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroRetryInterval => {
                write!(
                    f,
                    "cluster tcp peer connector retry interval must be non-zero"
                )
            }
        }
    }
}

impl std::error::Error for ClusterTcpPeerConnectorSettingsError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTcpPeerConnectorSettings {
    retry_interval: Duration,
    initial_retry_delay: Duration,
    automatic_retry_ticks: bool,
}

impl ClusterTcpPeerConnectorSettings {
    pub fn new(retry_interval: Duration) -> Result<Self, ClusterTcpPeerConnectorSettingsError> {
        if retry_interval.is_zero() {
            return Err(ClusterTcpPeerConnectorSettingsError::ZeroRetryInterval);
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

impl Default for ClusterTcpPeerConnectorSettings {
    fn default() -> Self {
        Self {
            retry_interval: Duration::from_secs(1),
            initial_retry_delay: Duration::from_secs(1),
            automatic_retry_ticks: true,
        }
    }
}

pub struct ClusterTcpPeerConnector {
    cluster: Cluster,
    runtime: Option<ClusterTcpPeerRuntime>,
    settings: ClusterTcpPeerConnectorSettings,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    last_report: Option<ClusterTcpPeerRouteReport>,
    last_error: Option<String>,
    retry_clock: Duration,
}

impl ClusterTcpPeerConnector {
    pub fn new(cluster: Cluster, runtime: ClusterTcpPeerRuntime) -> Self {
        Self::with_settings(cluster, runtime, ClusterTcpPeerConnectorSettings::default())
    }

    pub fn with_settings(
        cluster: Cluster,
        runtime: ClusterTcpPeerRuntime,
        settings: ClusterTcpPeerConnectorSettings,
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

    fn snapshot(&self) -> ClusterTcpPeerConnectorSnapshot {
        let runtime = self.runtime.as_ref();
        ClusterTcpPeerConnectorSnapshot {
            self_node: runtime.map(|runtime| runtime.self_node().clone()),
            active_targets: runtime
                .map(ClusterTcpPeerRuntime::active_peer_targets)
                .unwrap_or_default(),
            route_count: runtime.map_or(0, ClusterTcpPeerRuntime::peer_route_count),
            pending_reconnects: runtime
                .map(ClusterTcpPeerRuntime::pending_peer_reconnects)
                .unwrap_or_default(),
            last_report: self.last_report.clone(),
            last_error: self.last_error.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClusterTcpPeerConnectorMsg {
    Cluster(ClusterSubscriptionEvent),
    RetryDuePeerRoutes {
        now: Duration,
    },
    RetryTimerTick,
    ClearRoutes,
    Snapshot {
        reply_to: ActorRef<ClusterTcpPeerConnectorSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTcpPeerConnectorSnapshot {
    pub self_node: Option<UniqueAddress>,
    pub active_targets: Vec<crate::ClusterAssociationPeerTarget>,
    pub route_count: usize,
    pub pending_reconnects: Vec<ClusterTcpPeerReconnectPending>,
    pub last_report: Option<ClusterTcpPeerRouteReport>,
    pub last_error: Option<String>,
}

impl Actor for ClusterTcpPeerConnector {
    type Msg = ClusterTcpPeerConnectorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription = ctx.message_adapter(ClusterTcpPeerConnectorMsg::Cluster)?;
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
                ClusterTcpPeerConnectorMsg::RetryTimerTick,
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
            ClusterTcpPeerConnectorMsg::Cluster(event) => self.apply_cluster_event(event),
            ClusterTcpPeerConnectorMsg::RetryDuePeerRoutes { now } => self.retry_due(now),
            ClusterTcpPeerConnectorMsg::RetryTimerTick => {
                self.retry_clock = self
                    .retry_clock
                    .saturating_add(self.settings.retry_interval);
                self.retry_due(self.retry_clock)
            }
            ClusterTcpPeerConnectorMsg::ClearRoutes => self.clear_routes(),
            ClusterTcpPeerConnectorMsg::Snapshot { reply_to } => reply_to
                .tell(self.snapshot())
                .map_err(|error| ActorError::Message(error.reason().to_string())),
        }
    }
}

impl ClusterTcpPeerConnector {
    fn apply_cluster_event(&mut self, event: ClusterSubscriptionEvent) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "cluster tcp peer connector runtime is stopped".to_string(),
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
                "cluster tcp peer connector runtime is stopped".to_string(),
            ));
        };
        let result = runtime.retry_due_peer_routes(now);
        self.record_route_result(result);
        Ok(())
    }

    fn clear_routes(&mut self) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "cluster tcp peer connector runtime is stopped".to_string(),
            ));
        };
        self.last_report = Some(runtime.clear_peer_routes());
        self.last_error = None;
        Ok(())
    }

    fn record_route_result(
        &mut self,
        result: crate::ClusterTcpPeerRuntimeResult<ClusterTcpPeerRouteReport>,
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
