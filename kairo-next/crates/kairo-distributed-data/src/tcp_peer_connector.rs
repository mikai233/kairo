use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};
use kairo_cluster::{
    Cluster, ClusterAssociationPeerTarget, ClusterSubscriptionEvent,
    ClusterSubscriptionInitialState, UniqueAddress,
};

use crate::{
    ReplicatorTcpPeerReconnectPending, ReplicatorTcpPeerRouteReport, ReplicatorTcpPeerRuntime,
    ReplicatorTcpPeerRuntimeResult,
};

const TCP_PEER_RETRY_TIMER_KEY: &str = "ddata-tcp-peer-retry";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicatorTcpPeerConnectorSettingsError {
    ZeroRetryInterval,
}

impl std::fmt::Display for ReplicatorTcpPeerConnectorSettingsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroRetryInterval => {
                write!(
                    f,
                    "distributed-data tcp peer connector retry interval must be non-zero"
                )
            }
        }
    }
}

impl std::error::Error for ReplicatorTcpPeerConnectorSettingsError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorTcpPeerConnectorSettings {
    retry_interval: Duration,
    initial_retry_delay: Duration,
    automatic_retry_ticks: bool,
}

impl ReplicatorTcpPeerConnectorSettings {
    pub fn new(retry_interval: Duration) -> Result<Self, ReplicatorTcpPeerConnectorSettingsError> {
        if retry_interval.is_zero() {
            return Err(ReplicatorTcpPeerConnectorSettingsError::ZeroRetryInterval);
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

impl Default for ReplicatorTcpPeerConnectorSettings {
    fn default() -> Self {
        Self {
            retry_interval: Duration::from_secs(1),
            initial_retry_delay: Duration::from_secs(1),
            automatic_retry_ticks: true,
        }
    }
}

pub struct ReplicatorTcpPeerConnector {
    cluster: Cluster,
    runtime: Option<ReplicatorTcpPeerRuntime>,
    settings: ReplicatorTcpPeerConnectorSettings,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    last_report: Option<ReplicatorTcpPeerRouteReport>,
    last_error: Option<String>,
    retry_clock: Duration,
}

impl ReplicatorTcpPeerConnector {
    pub fn new(cluster: Cluster, runtime: ReplicatorTcpPeerRuntime) -> Self {
        Self::with_settings(
            cluster,
            runtime,
            ReplicatorTcpPeerConnectorSettings::default(),
        )
    }

    pub fn with_settings(
        cluster: Cluster,
        runtime: ReplicatorTcpPeerRuntime,
        settings: ReplicatorTcpPeerConnectorSettings,
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

    fn snapshot(&self) -> ReplicatorTcpPeerConnectorSnapshot {
        let runtime = self.runtime.as_ref();
        ReplicatorTcpPeerConnectorSnapshot {
            self_node: runtime.map(|runtime| runtime.self_node().clone()),
            active_targets: runtime
                .map(ReplicatorTcpPeerRuntime::active_peer_targets)
                .unwrap_or_default(),
            route_count: runtime.map_or(0, ReplicatorTcpPeerRuntime::peer_route_count),
            pending_reconnects: runtime
                .map(ReplicatorTcpPeerRuntime::pending_peer_reconnects)
                .unwrap_or_default(),
            last_report: self.last_report.clone(),
            last_error: self.last_error.clone(),
        }
    }

    fn apply_cluster_event(&mut self, event: ClusterSubscriptionEvent) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "distributed-data tcp peer connector runtime is stopped".to_string(),
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
                "distributed-data tcp peer connector runtime is stopped".to_string(),
            ));
        };
        let result = runtime.retry_due_peer_routes(now);
        self.record_route_result(result);
        Ok(())
    }

    fn clear_routes(&mut self) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "distributed-data tcp peer connector runtime is stopped".to_string(),
            ));
        };
        self.last_report = Some(runtime.clear_peer_routes());
        self.last_error = None;
        Ok(())
    }

    fn record_route_result(
        &mut self,
        result: ReplicatorTcpPeerRuntimeResult<ReplicatorTcpPeerRouteReport>,
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

#[derive(Debug, Clone)]
pub enum ReplicatorTcpPeerConnectorMsg {
    Cluster(ClusterSubscriptionEvent),
    RetryDuePeerRoutes {
        now: Duration,
    },
    RetryTimerTick,
    ClearRoutes,
    Snapshot {
        reply_to: ActorRef<ReplicatorTcpPeerConnectorSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorTcpPeerConnectorSnapshot {
    pub self_node: Option<UniqueAddress>,
    pub active_targets: Vec<ClusterAssociationPeerTarget>,
    pub route_count: usize,
    pub pending_reconnects: Vec<ReplicatorTcpPeerReconnectPending>,
    pub last_report: Option<ReplicatorTcpPeerRouteReport>,
    pub last_error: Option<String>,
}

impl Actor for ReplicatorTcpPeerConnector {
    type Msg = ReplicatorTcpPeerConnectorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription = ctx.message_adapter(ReplicatorTcpPeerConnectorMsg::Cluster)?;
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
                ReplicatorTcpPeerConnectorMsg::RetryTimerTick,
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
            ReplicatorTcpPeerConnectorMsg::Cluster(event) => self.apply_cluster_event(event),
            ReplicatorTcpPeerConnectorMsg::RetryDuePeerRoutes { now } => self.retry_due(now),
            ReplicatorTcpPeerConnectorMsg::RetryTimerTick => {
                self.retry_clock = self
                    .retry_clock
                    .saturating_add(self.settings.retry_interval);
                self.retry_due(self.retry_clock)
            }
            ReplicatorTcpPeerConnectorMsg::ClearRoutes => self.clear_routes(),
            ReplicatorTcpPeerConnectorMsg::Snapshot { reply_to } => reply_to
                .tell(self.snapshot())
                .map_err(|error| ActorError::Message(error.reason().to_string())),
        }
    }
}

#[cfg(test)]
mod tests;
