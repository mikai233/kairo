use std::any::Any;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;

mod lifecycle;

use crate::actor::{Actor, Context, Props};
use crate::dead_letters::DeadLetters;
use crate::death_watch::TerminationCause;
use crate::error::{ActorError, ActorResult};
use crate::mailbox::{Dequeued, SystemMessage, UserEnvelope};
use crate::path::ActorPath;
use crate::receive_timeout::ReceiveTimeoutState;
use crate::refs::ActorRef;
use crate::signal::Signal;
use crate::stash::StashState;
use crate::supervision::{SupervisionFailure, SupervisionState, SupervisorStrategy};
use crate::system::{ActorSystem, ActorSystemInner};
use crate::timers::TimerState;

pub(crate) use lifecycle::stop_child_roots_until_deadline;
use lifecycle::{
    restart_children_after_parent_restart, stop_adapter_refs, stop_children,
    stop_children_for_restart,
};

pub(crate) fn run_actor<A>(
    mut props: Props<A>,
    actor_ref: ActorRef<A::Msg>,
    dead_letters: DeadLetters,
    system_inner: Arc<ActorSystemInner>,
    registry_key: String,
    thread_system: ActorSystem,
    parent_path: ActorPath,
) where
    A: Actor,
{
    let mut actor = props.build();
    let throughput = thread_system.dispatcher_settings().throughput();
    let mut context = Context {
        myself: actor_ref.clone(),
        parent: parent_path.clone(),
        system: thread_system,
        stop_requested: false,
        timers: TimerState::default(),
        receive_timeout: ReceiveTimeoutState::default(),
        stash: StashState::new(props.stash_capacity()),
        tasks: Default::default(),
        asks: Default::default(),
        adapters: Default::default(),
    };
    let mut run_state = ActorRunState::default();

    if let Some(reason) = apply_start_result(
        &mut actor,
        &actor_ref,
        &mut context,
        &props,
        &system_inner,
        &mut run_state.supervision,
    ) {
        run_state.termination_cause = TerminationCause::Failed(reason);
        actor_ref.target.stopped.store(true, Ordering::Release);
    } else if context.stop_requested {
        actor_ref.target.stopped.store(true, Ordering::Release);
    } else {
        context.after_influencing_message();
    }

    let mailbox = actor_ref
        .target
        .mailbox
        .as_ref()
        .expect("live actor ref must have a mailbox");
    while !actor_ref.target.stopped.load(Ordering::Acquire) {
        let processed = process_dequeued(
            mailbox.dequeue(),
            &actor_ref,
            &mut actor,
            &mut context,
            &props,
            &system_inner,
            &mut run_state,
        );
        let mut processed_user = usize::from(processed);

        while processed_user < throughput && !actor_ref.target.stopped.load(Ordering::Acquire) {
            let Some(next) = mailbox.try_dequeue() else {
                break;
            };
            if process_dequeued(
                next,
                &actor_ref,
                &mut actor,
                &mut context,
                &props,
                &system_inner,
                &mut run_state,
            ) {
                processed_user += 1;
            }
        }

        if processed_user >= throughput && !actor_ref.target.stopped.load(Ordering::Acquire) {
            thread::yield_now();
        }
    }

    context.cancel_all_timers();
    context.cancel_receive_timeout();
    context.cancel_tasks();
    context.cancel_asks();
    stop_adapter_refs(&system_inner, &mut context);
    let _ = context.drain_stash_to_mailbox();
    for _ in 0..mailbox.close_and_drain_user() {
        dead_letters.publish::<A::Msg>(actor_ref.path.clone(), "actor is stopped");
    }

    system_inner.death_watch.remove_watcher(actor_ref.path());
    stop_children(&system_inner, actor_ref.path.as_str());
    let _ = invoke_signal(&mut actor, &mut context, Signal::PostStop);
    system_inner.registry.remove_ref(actor_ref.path());
    system_inner.registry.release_name(&registry_key);
    system_inner
        .registry
        .remove_child(parent_path.as_str(), actor_ref.path());
    system_inner.registry.remove_handle(actor_ref.path());
    system_inner.receptionist.remove_actor(actor_ref.path());
    actor_ref.target.terminated.mark_stopped();
    system_inner
        .death_watch
        .notify(actor_ref.path(), run_state.termination_cause);
}

#[derive(Debug)]
struct ActorRunState {
    supervision: SupervisionState,
    termination_cause: TerminationCause,
}

impl Default for ActorRunState {
    fn default() -> Self {
        Self {
            supervision: SupervisionState::default(),
            termination_cause: TerminationCause::Stopped,
        }
    }
}

fn process_dequeued<A>(
    dequeued: Dequeued<A::Msg>,
    actor_ref: &ActorRef<A::Msg>,
    actor: &mut A,
    context: &mut Context<A::Msg>,
    props: &Props<A>,
    system_inner: &ActorSystemInner,
    run_state: &mut ActorRunState,
) -> bool
where
    A: Actor,
{
    match dequeued {
        Dequeued::System(SystemMessage::Stop) | Dequeued::Closed => {
            actor_ref.target.stopped.store(true, Ordering::Release);
            false
        }
        Dequeued::System(SystemMessage::Restart) => {
            let stop_reason = apply_receive_result(
                restart_actor(
                    actor_ref,
                    actor,
                    context,
                    props,
                    system_inner,
                    props.supervisor().stop_children_on_restart(),
                ),
                actor_ref,
                actor,
                context,
                props,
                system_inner,
                &mut run_state.supervision,
            );
            if stop_reason.is_some() || context.stop_requested {
                if let Some(reason) = stop_reason {
                    run_state.termination_cause = TerminationCause::Failed(reason);
                }
                actor_ref.target.stopped.store(true, Ordering::Release);
            }
            false
        }
        Dequeued::System(SystemMessage::Signal(signal)) => {
            let queued_subject = match &signal {
                Signal::Terminated(actor) => Some(actor.path().clone()),
                Signal::ChildFailed { actor, .. } => Some(actor.path().clone()),
                Signal::PreRestart | Signal::PostStop => None,
            };
            let stop_reason = apply_receive_result(
                invoke_signal(actor, context, signal),
                actor_ref,
                actor,
                context,
                props,
                system_inner,
                &mut run_state.supervision,
            );
            if let Some(subject) = queued_subject {
                system_inner
                    .death_watch
                    .clear_queued_signal(&subject, actor_ref.path());
            }
            if stop_reason.is_some() || context.stop_requested {
                if let Some(reason) = stop_reason {
                    run_state.termination_cause = TerminationCause::Failed(reason);
                }
                actor_ref.target.stopped.store(true, Ordering::Release);
            }
            false
        }
        Dequeued::System(SystemMessage::SupervisionFailure(failure)) => {
            let reason = format!(
                "child `{}` escalated failure: {}",
                failure.child(),
                failure.reason()
            );
            if apply_actor_failure(
                ActorError::Message(reason.clone()),
                actor_ref,
                actor,
                context,
                props,
                system_inner,
                &mut run_state.supervision,
            ) || context.stop_requested
            {
                run_state.termination_cause = TerminationCause::Failed(reason);
                actor_ref.target.stopped.store(true, Ordering::Release);
            }
            false
        }
        Dequeued::User(UserEnvelope::Message(message)) => {
            context.before_influencing_message();
            let stop_reason = apply_receive_result(
                invoke_receive(actor, context, message),
                actor_ref,
                actor,
                context,
                props,
                system_inner,
                &mut run_state.supervision,
            );
            if stop_reason.is_some() || context.stop_requested {
                if let Some(reason) = stop_reason {
                    run_state.termination_cause = TerminationCause::Failed(reason);
                }
                actor_ref.target.stopped.store(true, Ordering::Release);
            }
            context.after_influencing_message();
            true
        }
        Dequeued::User(UserEnvelope::Adapted(adapt)) => {
            let Some(message) = adapt() else {
                return false;
            };
            context.before_influencing_message();
            let stop_reason = apply_receive_result(
                invoke_receive(actor, context, message),
                actor_ref,
                actor,
                context,
                props,
                system_inner,
                &mut run_state.supervision,
            );
            if stop_reason.is_some() || context.stop_requested {
                if let Some(reason) = stop_reason {
                    run_state.termination_cause = TerminationCause::Failed(reason);
                }
                actor_ref.target.stopped.store(true, Ordering::Release);
            }
            context.after_influencing_message();
            true
        }
        Dequeued::User(UserEnvelope::Timer(timer)) => {
            if context.accept_timer(&timer) {
                context.before_influencing_message();
                let stop_reason = apply_receive_result(
                    invoke_receive(actor, context, timer.into_message()),
                    actor_ref,
                    actor,
                    context,
                    props,
                    system_inner,
                    &mut run_state.supervision,
                );
                if stop_reason.is_some() || context.stop_requested {
                    if let Some(reason) = stop_reason {
                        run_state.termination_cause = TerminationCause::Failed(reason);
                    }
                    actor_ref.target.stopped.store(true, Ordering::Release);
                }
                context.after_influencing_message();
                true
            } else {
                false
            }
        }
        Dequeued::User(UserEnvelope::ReceiveTimeout(timeout)) => {
            if context.accept_receive_timeout(&timeout) {
                context.before_influencing_message();
                let stop_reason = apply_receive_result(
                    invoke_receive(actor, context, timeout.into_message()),
                    actor_ref,
                    actor,
                    context,
                    props,
                    system_inner,
                    &mut run_state.supervision,
                );
                if stop_reason.is_some() || context.stop_requested {
                    if let Some(reason) = stop_reason {
                        run_state.termination_cause = TerminationCause::Failed(reason);
                    }
                    actor_ref.target.stopped.store(true, Ordering::Release);
                }
                context.after_influencing_message();
                true
            } else {
                false
            }
        }
    }
}

fn invoke_started<A>(actor: &mut A, context: &mut Context<A::Msg>) -> ActorResult
where
    A: Actor,
{
    panic::catch_unwind(AssertUnwindSafe(|| actor.started(context)))
        .unwrap_or_else(|panic| Err(panic_to_actor_error("started", panic)))
}

fn invoke_receive<A>(actor: &mut A, context: &mut Context<A::Msg>, message: A::Msg) -> ActorResult
where
    A: Actor,
{
    panic::catch_unwind(AssertUnwindSafe(|| actor.receive(context, message)))
        .unwrap_or_else(|panic| Err(panic_to_actor_error("receive", panic)))
}

fn invoke_signal<A>(actor: &mut A, context: &mut Context<A::Msg>, signal: Signal) -> ActorResult
where
    A: Actor,
{
    panic::catch_unwind(AssertUnwindSafe(|| actor.signal(context, signal)))
        .unwrap_or_else(|panic| Err(panic_to_actor_error("signal", panic)))
}

fn panic_to_actor_error(callback: &str, panic: Box<dyn Any + Send>) -> ActorError {
    let message = if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    };
    ActorError::Message(format!("actor {callback} panicked: {message}"))
}

fn apply_start_result<A>(
    actor: &mut A,
    actor_ref: &ActorRef<A::Msg>,
    context: &mut Context<A::Msg>,
    props: &Props<A>,
    system_inner: &ActorSystemInner,
    supervision_state: &mut SupervisionState,
) -> Option<String>
where
    A: Actor,
{
    loop {
        let Err(error) = invoke_started(actor, context) else {
            return None;
        };
        let reason = error.to_string();

        match props.supervisor() {
            SupervisorStrategy::Escalate => {
                escalate_failure_to_parent(
                    system_inner,
                    context.parent.clone(),
                    actor_ref.path.clone(),
                    error,
                );
                return Some(reason);
            }
            SupervisorStrategy::RestartWithLimit {
                max_restarts,
                within,
                ..
            } => {
                if !supervision_state.startup_restart_allowed(max_restarts, within, Instant::now())
                    || restart_after_start_failure(actor, actor_ref, context, props, system_inner)
                        .is_err()
                {
                    return Some(reason);
                }
            }
            SupervisorStrategy::Stop
            | SupervisorStrategy::Resume
            | SupervisorStrategy::Restart
            | SupervisorStrategy::RestartPreservingChildren => return Some(reason),
        }
    }
}

fn apply_receive_result<A>(
    result: ActorResult,
    actor_ref: &ActorRef<A::Msg>,
    actor: &mut A,
    context: &mut Context<A::Msg>,
    props: &Props<A>,
    system_inner: &ActorSystemInner,
    supervision_state: &mut SupervisionState,
) -> Option<String>
where
    A: Actor,
{
    let Err(error) = result else {
        return None;
    };
    let reason = error.to_string();

    if apply_actor_failure(
        error,
        actor_ref,
        actor,
        context,
        props,
        system_inner,
        supervision_state,
    ) {
        Some(reason)
    } else {
        None
    }
}

fn apply_actor_failure<A>(
    error: ActorError,
    actor_ref: &ActorRef<A::Msg>,
    actor: &mut A,
    context: &mut Context<A::Msg>,
    props: &Props<A>,
    system_inner: &ActorSystemInner,
    supervision_state: &mut SupervisionState,
) -> bool
where
    A: Actor,
{
    match props.supervisor() {
        SupervisorStrategy::Stop => true,
        SupervisorStrategy::Resume => false,
        SupervisorStrategy::Escalate => {
            escalate_failure_to_parent(
                system_inner,
                context.parent.clone(),
                actor_ref.path.clone(),
                error,
            );
            true
        }
        strategy @ SupervisorStrategy::Restart
        | strategy @ SupervisorStrategy::RestartPreservingChildren => restart_actor(
            actor_ref,
            actor,
            context,
            props,
            system_inner,
            strategy.stop_children_on_restart(),
        )
        .is_err(),
        strategy @ SupervisorStrategy::RestartWithLimit {
            max_restarts,
            within,
            ..
        } => {
            !supervision_state.restart_allowed(max_restarts, within, Instant::now())
                || restart_actor_with_limit(
                    actor_ref,
                    actor,
                    context,
                    props,
                    system_inner,
                    supervision_state,
                    strategy,
                )
                .is_err()
        }
    }
}

fn escalate_failure_to_parent(
    system_inner: &ActorSystemInner,
    parent: ActorPath,
    child: ActorPath,
    error: ActorError,
) {
    if let Some(parent) = system_inner.registry.handle_of(&parent) {
        parent.request_supervision(SupervisionFailure::new(child, error.to_string()));
    }
}

fn restart_actor<A>(
    actor_ref: &ActorRef<A::Msg>,
    actor: &mut A,
    context: &mut Context<A::Msg>,
    props: &Props<A>,
    system_inner: &ActorSystemInner,
    stop_children_on_restart: bool,
) -> ActorResult
where
    A: Actor,
{
    context.cancel_all_timers();
    context.cancel_receive_timeout();
    context.cancel_tasks();
    context.cancel_asks();
    stop_adapter_refs(system_inner, context);
    let _ = context.drain_stash_to_mailbox();
    let _ = invoke_pre_restart(actor, context);
    if stop_children_on_restart {
        stop_children_for_restart(system_inner, actor_ref.path());
    }
    let Some(mut restarted) = props.restart() else {
        return Err(ActorError::Message(
            "restart supervision requires restartable props".to_string(),
        ));
    };
    context.stop_requested = false;
    invoke_started(&mut restarted, context)?;
    *actor = restarted;
    if !stop_children_on_restart {
        restart_children_after_parent_restart(system_inner, actor_ref.path());
    }
    Ok(())
}

fn restart_actor_with_limit<A>(
    actor_ref: &ActorRef<A::Msg>,
    actor: &mut A,
    context: &mut Context<A::Msg>,
    props: &Props<A>,
    system_inner: &ActorSystemInner,
    supervision_state: &mut SupervisionState,
    strategy: SupervisorStrategy,
) -> ActorResult
where
    A: Actor,
{
    let SupervisorStrategy::RestartWithLimit {
        max_restarts,
        within,
        stop_children: stop_children_on_restart,
    } = strategy
    else {
        return Err(ActorError::Message(
            "bounded restart requires RestartWithLimit strategy".to_string(),
        ));
    };
    context.cancel_all_timers();
    context.cancel_receive_timeout();
    context.cancel_tasks();
    context.cancel_asks();
    stop_adapter_refs(system_inner, context);
    let _ = context.drain_stash_to_mailbox();
    let _ = invoke_pre_restart(actor, context);
    if stop_children_on_restart {
        stop_children_for_restart(system_inner, actor_ref.path());
    }

    loop {
        let Some(mut restarted) = props.restart() else {
            return Err(ActorError::Message(
                "restart supervision requires restartable props".to_string(),
            ));
        };
        context.stop_requested = false;
        match invoke_started(&mut restarted, context) {
            Ok(()) => {
                *actor = restarted;
                if !stop_children_on_restart {
                    restart_children_after_parent_restart(system_inner, actor_ref.path());
                }
                return Ok(());
            }
            Err(error) => {
                context.cancel_all_timers();
                context.cancel_receive_timeout();
                context.cancel_tasks();
                context.cancel_asks();
                stop_adapter_refs(system_inner, context);
                if stop_children_on_restart {
                    stop_children_for_restart(system_inner, actor_ref.path());
                }
                if !supervision_state.restart_allowed(max_restarts, within, Instant::now()) {
                    return Err(error);
                }
            }
        }
    }
}

fn restart_after_start_failure<A>(
    actor: &mut A,
    actor_ref: &ActorRef<A::Msg>,
    context: &mut Context<A::Msg>,
    props: &Props<A>,
    system_inner: &ActorSystemInner,
) -> ActorResult
where
    A: Actor,
{
    context.cancel_all_timers();
    context.cancel_receive_timeout();
    context.cancel_tasks();
    context.cancel_asks();
    stop_adapter_refs(system_inner, context);
    stop_children_for_restart(system_inner, actor_ref.path());
    let Some(restarted) = props.restart() else {
        return Err(ActorError::Message(
            "restart supervision requires restartable props".to_string(),
        ));
    };
    context.stop_requested = false;
    *actor = restarted;
    Ok(())
}

fn invoke_pre_restart<A>(actor: &mut A, context: &mut Context<A::Msg>) -> ActorResult
where
    A: Actor,
{
    let previous_stop_requested = context.stop_requested;
    context.stop_requested = true;
    let result = invoke_signal(actor, context, Signal::PreRestart);
    context.stop_requested = previous_stop_requested;
    result
}
