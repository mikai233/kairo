use std::collections::BTreeMap;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, PHASE_CLUSTER_EXITING,
    PHASE_CLUSTER_EXITING_DONE, PHASE_CLUSTER_LEAVE, PHASE_CLUSTER_SHUTDOWN,
};
use kairo_remote::RemoteOutbound;
use kairo_serialization::Registry;

use crate::{
    Cluster, ClusterEvent, ClusterMembershipMsg, ClusterMembershipRemoteEnvelopeOutbound,
    ClusterSerializedMembership, ClusterSubscriptionEvent, ClusterSubscriptionInitialState,
    ExitingConfirmed, Member, MemberEvent, MemberStatus, UniqueAddress,
};

pub(crate) type ClusterLeaveCompletion = mpsc::Sender<Result<(), String>>;

#[derive(Debug, Clone)]
pub(crate) enum ClusterLeaveCoordinatorMsg {
    Cluster(Box<ClusterSubscriptionEvent>),
    BeginLeave { completion: ClusterLeaveCompletion },
    WaitForExiting { completion: ClusterLeaveCompletion },
    CompleteExiting { completion: ClusterLeaveCompletion },
}

/// Bridges synchronous coordinated-shutdown tasks to the actor-owned cluster
/// lifecycle without making shutdown threads mutate gossip directly.
pub(crate) struct ClusterLeaveCoordinator {
    cluster: Cluster,
    self_node: UniqueAddress,
    membership: ActorRef<ClusterMembershipMsg>,
    registry: Arc<Registry>,
    remote: ClusterMembershipRemoteEnvelopeOutbound,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    snapshot_received: bool,
    members: BTreeMap<String, Member>,
    leave_waiters: Vec<ClusterLeaveCompletion>,
    exiting_waiters: Vec<ClusterLeaveCompletion>,
    shutdown_started: bool,
}

impl ClusterLeaveCoordinator {
    pub(crate) fn new(
        cluster: Cluster,
        self_node: UniqueAddress,
        membership: ActorRef<ClusterMembershipMsg>,
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        Self {
            cluster,
            self_node,
            membership,
            registry,
            remote: ClusterMembershipRemoteEnvelopeOutbound::from_arc(outbound),
            subscription: None,
            snapshot_received: false,
            members: BTreeMap::new(),
            leave_waiters: Vec::new(),
            exiting_waiters: Vec::new(),
            shutdown_started: false,
        }
    }

    fn apply_cluster_event(
        &mut self,
        ctx: &Context<ClusterLeaveCoordinatorMsg>,
        event: ClusterSubscriptionEvent,
    ) -> ActorResult {
        match event {
            ClusterSubscriptionEvent::CurrentState(state) => {
                self.members = state
                    .members
                    .into_iter()
                    .map(|member| (member.unique_address.ordering_key(), member))
                    .collect();
                self.snapshot_received = true;
            }
            ClusterSubscriptionEvent::Event(ClusterEvent::Member(event)) => {
                self.apply_member_event(event);
            }
            ClusterSubscriptionEvent::Event(_) => {}
        }
        self.resolve_waiters();
        self.start_shutdown_when_exiting(ctx)
    }

    fn apply_member_event(&mut self, event: MemberEvent) {
        match event {
            MemberEvent::Removed { member, .. } => {
                self.members.remove(&member.unique_address.ordering_key());
            }
            MemberEvent::Joined(member)
            | MemberEvent::WeaklyUp(member)
            | MemberEvent::Up(member)
            | MemberEvent::Left(member)
            | MemberEvent::Exited(member)
            | MemberEvent::Downed(member) => {
                self.members
                    .insert(member.unique_address.ordering_key(), member);
            }
        }
    }

    fn begin_leave(&mut self, completion: ClusterLeaveCompletion) {
        if let Err(error) = self.membership.tell(ClusterMembershipMsg::Leave {
            address: self.self_node.address.clone(),
        }) {
            let _ = completion.send(Err(error.reason().to_string()));
            return;
        }
        self.leave_waiters.push(completion);
        self.resolve_waiters();
    }

    fn wait_for_exiting(&mut self, completion: ClusterLeaveCompletion) {
        self.exiting_waiters.push(completion);
        self.resolve_waiters();
    }

    fn complete_exiting(&self, completion: ClusterLeaveCompletion) {
        let confirmation = ExitingConfirmed {
            node: self.self_node.clone(),
        };
        let serialized = match self.registry.serialize(&confirmation) {
            Ok(serialized) => serialized,
            Err(error) => {
                let _ = completion.send(Err(error.to_string()));
                return;
            }
        };
        for member in self.members.values().filter(|member| {
            member.unique_address != self.self_node && member.status != MemberStatus::Removed
        }) {
            let _ = self
                .remote
                .send_serialized(ClusterSerializedMembership::new(
                    member.unique_address.clone(),
                    serialized.clone(),
                ));
        }
        let _ = completion.send(Ok(()));
    }

    fn resolve_waiters(&mut self) {
        if !self.snapshot_received {
            return;
        }
        let status = self
            .members
            .get(&self.self_node.ordering_key())
            .map(|member| member.status);
        if status.is_none_or(|status| {
            matches!(
                status,
                MemberStatus::Leaving
                    | MemberStatus::Exiting
                    | MemberStatus::Down
                    | MemberStatus::Removed
            )
        }) {
            complete_all(&mut self.leave_waiters);
        }
        if status.is_none_or(|status| {
            matches!(
                status,
                MemberStatus::Exiting | MemberStatus::Down | MemberStatus::Removed
            )
        }) {
            complete_all(&mut self.exiting_waiters);
        }
    }

    fn start_shutdown_when_exiting(
        &mut self,
        ctx: &Context<ClusterLeaveCoordinatorMsg>,
    ) -> ActorResult {
        if self.shutdown_started
            || self
                .members
                .get(&self.self_node.ordering_key())
                .is_none_or(|member| member.status != MemberStatus::Exiting)
        {
            return Ok(());
        }
        let shutdown = ctx.system().coordinated_shutdown();
        self.shutdown_started = true;
        if shutdown.reason().is_some() {
            return Ok(());
        }
        ctx.spawn_task(move |_| {
            let _ = shutdown.run("cluster member exiting");
        })?;
        Ok(())
    }
}

impl Actor for ClusterLeaveCoordinator {
    type Msg = ClusterLeaveCoordinatorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription =
            ctx.message_adapter(|event| ClusterLeaveCoordinatorMsg::Cluster(Box::new(event)))?;
        self.cluster
            .subscribe_with_initial_state(
                subscription.clone(),
                ClusterSubscriptionInitialState::Snapshot,
            )
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.subscription = Some(subscription);
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        if let Some(subscription) = self.subscription.take() {
            let _ = self.cluster.unsubscribe(subscription);
        }
        fail_all(&mut self.leave_waiters, "cluster leave coordinator stopped");
        fail_all(
            &mut self.exiting_waiters,
            "cluster leave coordinator stopped",
        );
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterLeaveCoordinatorMsg::Cluster(event) => self.apply_cluster_event(ctx, *event)?,
            ClusterLeaveCoordinatorMsg::BeginLeave { completion } => self.begin_leave(completion),
            ClusterLeaveCoordinatorMsg::WaitForExiting { completion } => {
                self.wait_for_exiting(completion)
            }
            ClusterLeaveCoordinatorMsg::CompleteExiting { completion } => {
                self.complete_exiting(completion)
            }
        }
        Ok(())
    }
}

fn complete_all(waiters: &mut Vec<ClusterLeaveCompletion>) {
    for waiter in waiters.drain(..) {
        let _ = waiter.send(Ok(()));
    }
}

fn fail_all(waiters: &mut Vec<ClusterLeaveCompletion>, reason: &str) {
    for waiter in waiters.drain(..) {
        let _ = waiter.send(Err(reason.to_string()));
    }
}

pub(crate) fn register_cluster_coordinated_shutdown(
    system: &ActorSystem,
    root: ActorRef<()>,
    coordinator: ActorRef<ClusterLeaveCoordinatorMsg>,
    timeout: Duration,
) -> Result<(), ActorError> {
    if timeout.is_zero() {
        return Err(ActorError::Message(
            "cluster shutdown timeout must be greater than zero".to_string(),
        ));
    }
    let shutdown = system.coordinated_shutdown();
    let leave = coordinator.clone();
    shutdown.add_task(PHASE_CLUSTER_LEAVE, "cluster-leave", move || {
        request_and_wait(&leave, timeout, |completion| {
            ClusterLeaveCoordinatorMsg::BeginLeave { completion }
        })
    })?;
    let exiting = coordinator.clone();
    shutdown.add_task(PHASE_CLUSTER_EXITING, "cluster-wait-exiting", move || {
        request_and_wait(&exiting, timeout, |completion| {
            ClusterLeaveCoordinatorMsg::WaitForExiting { completion }
        })
    })?;
    shutdown.add_task(
        PHASE_CLUSTER_EXITING_DONE,
        "cluster-exiting-confirmed",
        move || {
            request_and_wait(&coordinator, timeout, |completion| {
                ClusterLeaveCoordinatorMsg::CompleteExiting { completion }
            })
        },
    )?;
    let system = system.clone();
    shutdown.add_task(PHASE_CLUSTER_SHUTDOWN, "cluster-shutdown", move || {
        system.stop(&root);
        if root.wait_for_stop(timeout) {
            Ok(())
        } else {
            Err(ActorError::ShutdownTaskFailed(
                "cluster root shutdown timed out".to_string(),
            ))
        }
    })?;
    Ok(())
}

fn request_and_wait(
    coordinator: &ActorRef<ClusterLeaveCoordinatorMsg>,
    timeout: Duration,
    message: impl FnOnce(ClusterLeaveCompletion) -> ClusterLeaveCoordinatorMsg,
) -> Result<(), ActorError> {
    let (completion, result) = mpsc::channel();
    coordinator
        .tell(message(completion))
        .map_err(|error| ActorError::ShutdownTaskFailed(error.reason().to_string()))?;
    match result.recv_timeout(timeout) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(reason)) => Err(ActorError::ShutdownTaskFailed(reason)),
        Err(mpsc::RecvTimeoutError::Timeout) => Err(ActorError::ShutdownTaskFailed(format!(
            "cluster lifecycle transition timed out after {timeout:?}"
        ))),
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(ActorError::ShutdownTaskFailed(
            "cluster lifecycle coordinator disconnected".to_string(),
        )),
    }
}
