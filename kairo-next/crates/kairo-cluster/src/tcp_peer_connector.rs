#![deny(missing_docs)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};

use crate::{
    Cluster, ClusterSubscriptionEvent, ClusterSubscriptionInitialState,
    ClusterTcpPeerReconnectPending, ClusterTcpPeerRouteReport, ClusterTcpPeerRuntime,
    UniqueAddress,
};

const TCP_PEER_RETRY_TIMER_KEY: &str = "cluster-tcp-peer-retry";

#[derive(Debug, Clone, PartialEq, Eq)]
/// Invalid TCP peer connector scheduling configuration.
pub enum ClusterTcpPeerConnectorSettingsError {
    /// A zero interval would create an immediate retry loop.
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
/// Actor timer policy for retrying failed membership-derived peer dials.
pub struct ClusterTcpPeerConnectorSettings {
    retry_interval: Duration,
    initial_retry_delay: Duration,
    automatic_retry_ticks: bool,
}

impl ClusterTcpPeerConnectorSettings {
    /// Creates settings with automatic retry ticks and an initial delay equal to `retry_interval`.
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

    /// Sets the delay before the first automatic retry tick.
    pub fn with_initial_retry_delay(mut self, delay: Duration) -> Self {
        self.initial_retry_delay = delay;
        self
    }

    /// Enables or disables actor-owned periodic retry ticks.
    ///
    /// Disabling ticks allows tests or an embedding runtime to drive retries explicitly with
    /// `RetryDuePeerRoutes` messages.
    pub fn with_automatic_retry_ticks(mut self, automatic: bool) -> Self {
        self.automatic_retry_ticks = automatic;
        self
    }

    /// Returns the non-zero interval between automatic retry ticks.
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

/// Actor that serializes cluster events and reconnect ticks onto an owned TCP peer runtime.
///
/// Runtime operations may block on transport I/O, so they execute outside synchronous actor turns
/// one at a time. The actor subscribes with a current-state snapshot, retains diagnostic state,
/// and shuts down the entire owned runtime when stopped.
pub struct ClusterTcpPeerConnector {
    cluster: Cluster,
    runtime: Arc<Mutex<Option<ClusterTcpPeerRuntime>>>,
    runtime_state: ClusterTcpPeerConnectorRuntimeState,
    pending_commands: VecDeque<ClusterTcpPeerConnectorRuntimeCommand>,
    command_in_flight: bool,
    settings: ClusterTcpPeerConnectorSettings,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    last_report: Option<ClusterTcpPeerRouteReport>,
    last_error: Option<String>,
    retry_clock: Duration,
}

impl ClusterTcpPeerConnector {
    /// Creates a connector with the default one-second automatic retry policy.
    pub fn new(cluster: Cluster, runtime: ClusterTcpPeerRuntime) -> Self {
        Self::with_settings(cluster, runtime, ClusterTcpPeerConnectorSettings::default())
    }

    /// Creates a connector with explicit actor timer settings.
    pub fn with_settings(
        cluster: Cluster,
        runtime: ClusterTcpPeerRuntime,
        settings: ClusterTcpPeerConnectorSettings,
    ) -> Self {
        let runtime_state = ClusterTcpPeerConnectorRuntimeState::from_runtime(&runtime);
        Self {
            cluster,
            runtime: Arc::new(Mutex::new(Some(runtime))),
            runtime_state,
            pending_commands: VecDeque::new(),
            command_in_flight: false,
            settings,
            subscription: None,
            last_report: None,
            last_error: None,
            retry_clock: Duration::ZERO,
        }
    }

    fn snapshot(&self) -> ClusterTcpPeerConnectorSnapshot {
        ClusterTcpPeerConnectorSnapshot {
            self_node: self.runtime_state.self_node.clone(),
            active_targets: self.runtime_state.active_targets.clone(),
            route_count: self.runtime_state.route_count,
            pending_reconnects: self.runtime_state.pending_reconnects.clone(),
            last_report: self.last_report.clone(),
            last_error: self.last_error.clone(),
        }
    }
}

#[derive(Debug, Clone)]
/// Commands accepted by the TCP peer connector actor.
pub enum ClusterTcpPeerConnectorMsg {
    /// Applies a current cluster snapshot or subsequent domain event.
    Cluster(ClusterSubscriptionEvent),
    /// Retries all peer dials due at the caller-provided clock value.
    RetryDuePeerRoutes {
        /// Monotonic logical time used by reconnect deadlines.
        now: Duration,
    },
    /// Advances the actor-owned retry clock and retries due peer routes.
    RetryTimerTick,
    /// Clears managed peer routes while leaving pending reconnect deadlines intact.
    ClearRoutes,
    /// Completes the serialized runtime command currently in flight.
    RuntimeCommandComplete(ClusterTcpPeerConnectorRuntimeCommandResult),
    /// Requests the connector's last observed runtime and route diagnostics.
    Snapshot {
        /// Recipient for the diagnostic snapshot.
        reply_to: ActorRef<ClusterTcpPeerConnectorSnapshot>,
    },
}

#[derive(Debug, Clone)]
/// Opaque completion value produced by connector-owned background runtime work.
pub struct ClusterTcpPeerConnectorRuntimeCommandResult {
    outcome: Result<ClusterTcpPeerRouteReport, String>,
    state: Option<ClusterTcpPeerConnectorRuntimeState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ClusterTcpPeerConnectorRuntimeState {
    self_node: Option<UniqueAddress>,
    active_targets: Vec<crate::ClusterAssociationPeerTarget>,
    route_count: usize,
    pending_reconnects: Vec<ClusterTcpPeerReconnectPending>,
}

impl ClusterTcpPeerConnectorRuntimeState {
    fn from_runtime(runtime: &ClusterTcpPeerRuntime) -> Self {
        Self {
            self_node: Some(runtime.self_node().clone()),
            active_targets: runtime.active_peer_targets(),
            route_count: runtime.peer_route_count(),
            pending_reconnects: runtime.pending_peer_reconnects(),
        }
    }
}

#[derive(Debug)]
enum ClusterTcpPeerConnectorRuntimeCommand {
    ApplyClusterEvent(Box<ClusterSubscriptionEvent>),
    RetryDuePeerRoutes { now: Duration },
    ClearRoutes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Diagnostic snapshot of connector-owned TCP peer state.
pub struct ClusterTcpPeerConnectorSnapshot {
    /// Local cluster identity last mirrored from the owned runtime, when available.
    pub self_node: Option<UniqueAddress>,
    /// Exact member incarnations with managed route entries.
    pub active_targets: Vec<crate::ClusterAssociationPeerTarget>,
    /// Number of managed peer route entries.
    pub route_count: usize,
    /// Failed peer dials waiting for a reconnect deadline.
    pub pending_reconnects: Vec<ClusterTcpPeerReconnectPending>,
    /// Outcome of the most recently successful runtime command.
    pub last_report: Option<ClusterTcpPeerRouteReport>,
    /// Most recent runtime-command failure, cleared by the next success.
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
        self.pending_commands.clear();
        if let Some(runtime) = self
            .runtime
            .lock()
            .expect("cluster tcp peer connector runtime lock poisoned")
            .take()
        {
            let _ = runtime.shutdown();
        }
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterTcpPeerConnectorMsg::Cluster(event) => self.enqueue_runtime_command(
                ctx,
                ClusterTcpPeerConnectorRuntimeCommand::ApplyClusterEvent(Box::new(event)),
            ),
            ClusterTcpPeerConnectorMsg::RetryDuePeerRoutes { now } => self.enqueue_runtime_command(
                ctx,
                ClusterTcpPeerConnectorRuntimeCommand::RetryDuePeerRoutes { now },
            ),
            ClusterTcpPeerConnectorMsg::RetryTimerTick => {
                self.retry_clock = self
                    .retry_clock
                    .saturating_add(self.settings.retry_interval);
                self.enqueue_runtime_command(
                    ctx,
                    ClusterTcpPeerConnectorRuntimeCommand::RetryDuePeerRoutes {
                        now: self.retry_clock,
                    },
                )
            }
            ClusterTcpPeerConnectorMsg::ClearRoutes => self
                .enqueue_runtime_command(ctx, ClusterTcpPeerConnectorRuntimeCommand::ClearRoutes),
            ClusterTcpPeerConnectorMsg::RuntimeCommandComplete(result) => {
                self.finish_runtime_command(ctx, result)
            }
            ClusterTcpPeerConnectorMsg::Snapshot { reply_to } => reply_to
                .tell(self.snapshot())
                .map_err(|error| ActorError::Message(error.reason().to_string())),
        }
    }
}

impl ClusterTcpPeerConnector {
    fn enqueue_runtime_command(
        &mut self,
        ctx: &Context<ClusterTcpPeerConnectorMsg>,
        command: ClusterTcpPeerConnectorRuntimeCommand,
    ) -> ActorResult {
        self.pending_commands.push_back(command);
        self.start_next_runtime_command(ctx)
    }

    fn start_next_runtime_command(
        &mut self,
        ctx: &Context<ClusterTcpPeerConnectorMsg>,
    ) -> ActorResult {
        if self.command_in_flight {
            return Ok(());
        }
        let Some(command) = self.pending_commands.pop_front() else {
            return Ok(());
        };
        self.command_in_flight = true;
        let runtime = Arc::clone(&self.runtime);
        ctx.spawn_task(move |myself| {
            let result = run_runtime_command(runtime, command);
            let _ = myself.tell(ClusterTcpPeerConnectorMsg::RuntimeCommandComplete(result));
        })?;
        Ok(())
    }

    fn finish_runtime_command(
        &mut self,
        ctx: &Context<ClusterTcpPeerConnectorMsg>,
        result: ClusterTcpPeerConnectorRuntimeCommandResult,
    ) -> ActorResult {
        self.command_in_flight = false;
        if let Some(state) = result.state {
            self.runtime_state = state;
        }
        self.record_route_outcome(result.outcome);
        self.start_next_runtime_command(ctx)
    }

    fn record_route_outcome(&mut self, result: Result<ClusterTcpPeerRouteReport, String>) {
        match result {
            Ok(report) => {
                self.last_report = Some(report);
                self.last_error = None;
            }
            Err(error) => {
                self.last_error = Some(error);
            }
        }
    }
}

fn run_runtime_command(
    runtime: Arc<Mutex<Option<ClusterTcpPeerRuntime>>>,
    command: ClusterTcpPeerConnectorRuntimeCommand,
) -> ClusterTcpPeerConnectorRuntimeCommandResult {
    let mut guard = runtime
        .lock()
        .expect("cluster tcp peer connector runtime lock poisoned");
    let Some(runtime) = guard.as_mut() else {
        return ClusterTcpPeerConnectorRuntimeCommandResult {
            outcome: Err("cluster tcp peer connector runtime is stopped".to_string()),
            state: None,
        };
    };

    let outcome = match command {
        ClusterTcpPeerConnectorRuntimeCommand::ApplyClusterEvent(event) => match *event {
            ClusterSubscriptionEvent::CurrentState(state) => runtime.apply_snapshot(state),
            ClusterSubscriptionEvent::Event(event) => runtime.apply_event(event),
        },
        ClusterTcpPeerConnectorRuntimeCommand::RetryDuePeerRoutes { now } => {
            runtime.retry_due_peer_routes(now)
        }
        ClusterTcpPeerConnectorRuntimeCommand::ClearRoutes => Ok(runtime.clear_peer_routes()),
    }
    .map_err(|error| error.to_string());

    ClusterTcpPeerConnectorRuntimeCommandResult {
        outcome,
        state: Some(ClusterTcpPeerConnectorRuntimeState::from_runtime(runtime)),
    }
}

#[cfg(test)]
mod tests;
