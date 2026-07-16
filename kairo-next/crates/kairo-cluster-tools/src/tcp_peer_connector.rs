#![deny(missing_docs)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};
use kairo_cluster::{
    Cluster, ClusterAssociationPeerTarget, ClusterSubscriptionEvent,
    ClusterSubscriptionInitialState, UniqueAddress,
};
use kairo_serialization::RemoteMessage;

use crate::{
    ClusterToolsTcpPeerReconnectPending, ClusterToolsTcpPeerRouteReport, ClusterToolsTcpPeerRuntime,
};

const TCP_PEER_RETRY_TIMER_KEY: &str = "cluster-tools-tcp-peer-retry";

#[derive(Debug, Clone, PartialEq, Eq)]
/// Invalid TCP peer connector scheduling configuration.
pub enum ClusterToolsTcpPeerConnectorSettingsError {
    /// A zero interval would create an immediate retry loop.
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
/// Actor timer policy for retrying failed membership-derived peer dials.
pub struct ClusterToolsTcpPeerConnectorSettings {
    retry_interval: Duration,
    initial_retry_delay: Duration,
    automatic_retry_ticks: bool,
}

impl ClusterToolsTcpPeerConnectorSettings {
    /// Creates settings with automatic retry ticks and an initial delay equal to `retry_interval`.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterToolsTcpPeerConnectorSettingsError::ZeroRetryInterval`]
    /// when `retry_interval` is zero.
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

    /// Sets the delay before the first automatic retry tick.
    pub fn with_initial_retry_delay(mut self, delay: Duration) -> Self {
        self.initial_retry_delay = delay;
        self
    }

    /// Enables or disables actor-owned periodic retry ticks.
    ///
    /// Disabling ticks allows tests or an embedding runtime to drive retries
    /// explicitly with [`ClusterToolsTcpPeerConnectorMsg::RetryDuePeerRoutes`].
    pub fn with_automatic_retry_ticks(mut self, automatic: bool) -> Self {
        self.automatic_retry_ticks = automatic;
        self
    }

    /// Returns the non-zero interval between automatic retry ticks.
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

/// Actor that serializes cluster events and reconnect ticks onto an owned TCP peer runtime.
///
/// Potentially blocking transport operations execute outside synchronous actor
/// turns one at a time. Their completions re-enter through the mailbox, while
/// snapshots are served from the last completed runtime state. Stopping the
/// actor unsubscribes from cluster events and shuts down the owned runtime.
pub struct ClusterToolsTcpPeerConnector<M>
where
    M: RemoteMessage + Send + 'static,
{
    cluster: Cluster,
    runtime: Arc<Mutex<Option<ClusterToolsTcpPeerRuntime<M>>>>,
    runtime_state: ClusterToolsTcpPeerConnectorRuntimeState,
    pending_commands: VecDeque<ClusterToolsTcpPeerConnectorRuntimeCommand>,
    command_in_flight: bool,
    #[cfg(test)]
    runtime_command_gate: Option<ClusterToolsTcpPeerConnectorRuntimeCommandGate>,
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
    /// Creates a connector with the default one-second automatic retry policy.
    pub fn new(cluster: Cluster, runtime: ClusterToolsTcpPeerRuntime<M>) -> Self {
        Self::with_settings(
            cluster,
            runtime,
            ClusterToolsTcpPeerConnectorSettings::default(),
        )
    }

    /// Creates a connector with explicit actor timer settings.
    pub fn with_settings(
        cluster: Cluster,
        runtime: ClusterToolsTcpPeerRuntime<M>,
        settings: ClusterToolsTcpPeerConnectorSettings,
    ) -> Self {
        let runtime_state = ClusterToolsTcpPeerConnectorRuntimeState::from_runtime(&runtime);
        Self {
            cluster,
            runtime: Arc::new(Mutex::new(Some(runtime))),
            runtime_state,
            pending_commands: VecDeque::new(),
            command_in_flight: false,
            #[cfg(test)]
            runtime_command_gate: None,
            settings,
            subscription: None,
            last_report: None,
            last_error: None,
            retry_clock: Duration::ZERO,
        }
    }

    fn snapshot(&self) -> ClusterToolsTcpPeerConnectorSnapshot {
        ClusterToolsTcpPeerConnectorSnapshot {
            self_node: self.runtime_state.self_node.clone(),
            active_targets: self.runtime_state.active_targets.clone(),
            route_count: self.runtime_state.route_count,
            pending_reconnects: self.runtime_state.pending_reconnects.clone(),
            last_report: self.last_report.clone(),
            last_error: self.last_error.clone(),
        }
    }

    #[cfg(test)]
    fn with_runtime_command_gate(
        mut self,
        gate: ClusterToolsTcpPeerConnectorRuntimeCommandGate,
    ) -> Self {
        self.runtime_command_gate = Some(gate);
        self
    }
}

#[derive(Debug, Clone)]
/// Commands accepted by the cluster-tools TCP peer connector actor.
pub enum ClusterToolsTcpPeerConnectorMsg {
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
    RuntimeCommandComplete(ClusterToolsTcpPeerConnectorRuntimeCommandResult),
    /// Requests the connector's last completed runtime and route diagnostics.
    Snapshot {
        /// Recipient for the diagnostic snapshot.
        reply_to: ActorRef<ClusterToolsTcpPeerConnectorSnapshot>,
    },
}

/// Opaque completion value produced by connector-owned background runtime work.
#[derive(Debug, Clone)]
pub struct ClusterToolsTcpPeerConnectorRuntimeCommandResult {
    outcome: Result<ClusterToolsTcpPeerRouteReport, String>,
    state: Option<ClusterToolsTcpPeerConnectorRuntimeState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ClusterToolsTcpPeerConnectorRuntimeState {
    self_node: Option<UniqueAddress>,
    active_targets: Vec<ClusterAssociationPeerTarget>,
    route_count: usize,
    pending_reconnects: Vec<ClusterToolsTcpPeerReconnectPending>,
}

impl ClusterToolsTcpPeerConnectorRuntimeState {
    fn from_runtime<M>(runtime: &ClusterToolsTcpPeerRuntime<M>) -> Self
    where
        M: RemoteMessage + Send + 'static,
    {
        Self {
            self_node: Some(runtime.self_node().clone()),
            active_targets: runtime.active_peer_targets(),
            route_count: runtime.peer_route_count(),
            pending_reconnects: runtime.pending_peer_reconnects(),
        }
    }
}

#[derive(Debug)]
enum ClusterToolsTcpPeerConnectorRuntimeCommand {
    ApplyClusterEvent(Box<ClusterSubscriptionEvent>),
    RetryDuePeerRoutes { now: Duration },
    ClearRoutes,
}

#[cfg(test)]
#[derive(Clone)]
struct ClusterToolsTcpPeerConnectorRuntimeCommandGate {
    started: Arc<AtomicBool>,
    released: Arc<AtomicBool>,
}

#[cfg(test)]
impl ClusterToolsTcpPeerConnectorRuntimeCommandGate {
    fn block_until_released(&self) {
        self.started.store(true, Ordering::SeqCst);
        while !self.released.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Diagnostic snapshot of connector-owned TCP peer state.
pub struct ClusterToolsTcpPeerConnectorSnapshot {
    /// Local cluster identity last mirrored from the owned runtime, when available.
    pub self_node: Option<UniqueAddress>,
    /// Exact member incarnations with managed route entries.
    pub active_targets: Vec<ClusterAssociationPeerTarget>,
    /// Number of managed peer route entries.
    pub route_count: usize,
    /// Failed peer dials waiting for a reconnect deadline.
    pub pending_reconnects: Vec<ClusterToolsTcpPeerReconnectPending>,
    /// Outcome of the most recently successful runtime command.
    pub last_report: Option<ClusterToolsTcpPeerRouteReport>,
    /// Most recent runtime-command failure, cleared by the next success.
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
        self.pending_commands.clear();
        if let Some(runtime) = self
            .runtime
            .lock()
            .expect("cluster-tools tcp peer connector runtime lock poisoned")
            .take()
        {
            let _ = runtime.shutdown();
        }
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterToolsTcpPeerConnectorMsg::Cluster(event) => self.enqueue_runtime_command(
                ctx,
                ClusterToolsTcpPeerConnectorRuntimeCommand::ApplyClusterEvent(Box::new(event)),
            ),
            ClusterToolsTcpPeerConnectorMsg::RetryDuePeerRoutes { now } => self
                .enqueue_runtime_command(
                    ctx,
                    ClusterToolsTcpPeerConnectorRuntimeCommand::RetryDuePeerRoutes { now },
                ),
            ClusterToolsTcpPeerConnectorMsg::RetryTimerTick => {
                self.retry_clock = self
                    .retry_clock
                    .saturating_add(self.settings.retry_interval);
                self.enqueue_runtime_command(
                    ctx,
                    ClusterToolsTcpPeerConnectorRuntimeCommand::RetryDuePeerRoutes {
                        now: self.retry_clock,
                    },
                )
            }
            ClusterToolsTcpPeerConnectorMsg::ClearRoutes => self.enqueue_runtime_command(
                ctx,
                ClusterToolsTcpPeerConnectorRuntimeCommand::ClearRoutes,
            ),
            ClusterToolsTcpPeerConnectorMsg::RuntimeCommandComplete(result) => {
                self.finish_runtime_command(ctx, result)
            }
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
    fn enqueue_runtime_command(
        &mut self,
        ctx: &Context<ClusterToolsTcpPeerConnectorMsg>,
        command: ClusterToolsTcpPeerConnectorRuntimeCommand,
    ) -> ActorResult {
        self.pending_commands.push_back(command);
        self.start_next_runtime_command(ctx)
    }

    fn start_next_runtime_command(
        &mut self,
        ctx: &Context<ClusterToolsTcpPeerConnectorMsg>,
    ) -> ActorResult {
        if self.command_in_flight {
            return Ok(());
        }
        let Some(command) = self.pending_commands.pop_front() else {
            return Ok(());
        };
        self.command_in_flight = true;
        let runtime = Arc::clone(&self.runtime);
        #[cfg(test)]
        let runtime_command_gate = self.runtime_command_gate.clone();
        ctx.spawn_task(move |myself| {
            #[cfg(test)]
            if let Some(gate) = runtime_command_gate {
                gate.block_until_released();
            }
            let result = run_runtime_command(runtime, command);
            let _ = myself.tell(ClusterToolsTcpPeerConnectorMsg::RuntimeCommandComplete(
                result,
            ));
        })?;
        Ok(())
    }

    fn finish_runtime_command(
        &mut self,
        ctx: &Context<ClusterToolsTcpPeerConnectorMsg>,
        result: ClusterToolsTcpPeerConnectorRuntimeCommandResult,
    ) -> ActorResult {
        self.command_in_flight = false;
        if let Some(state) = result.state {
            self.runtime_state = state;
        }
        self.record_route_outcome(result.outcome);
        self.start_next_runtime_command(ctx)
    }

    fn record_route_outcome(&mut self, result: Result<ClusterToolsTcpPeerRouteReport, String>) {
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

fn run_runtime_command<M>(
    runtime: Arc<Mutex<Option<ClusterToolsTcpPeerRuntime<M>>>>,
    command: ClusterToolsTcpPeerConnectorRuntimeCommand,
) -> ClusterToolsTcpPeerConnectorRuntimeCommandResult
where
    M: RemoteMessage + Send + 'static,
{
    let mut guard = runtime
        .lock()
        .expect("cluster-tools tcp peer connector runtime lock poisoned");
    let Some(runtime) = guard.as_mut() else {
        return ClusterToolsTcpPeerConnectorRuntimeCommandResult {
            outcome: Err("cluster-tools tcp peer connector runtime is stopped".to_string()),
            state: None,
        };
    };

    let outcome = match command {
        ClusterToolsTcpPeerConnectorRuntimeCommand::ApplyClusterEvent(event) => match *event {
            ClusterSubscriptionEvent::CurrentState(state) => runtime.apply_snapshot(state),
            ClusterSubscriptionEvent::Event(event) => runtime.apply_event(event),
        },
        ClusterToolsTcpPeerConnectorRuntimeCommand::RetryDuePeerRoutes { now } => {
            runtime.retry_due_peer_routes(now)
        }
        ClusterToolsTcpPeerConnectorRuntimeCommand::ClearRoutes => Ok(runtime.clear_peer_routes()),
    }
    .map_err(|error| error.to_string());

    ClusterToolsTcpPeerConnectorRuntimeCommandResult {
        outcome,
        state: Some(ClusterToolsTcpPeerConnectorRuntimeState::from_runtime(
            runtime,
        )),
    }
}

#[cfg(test)]
mod tests;
