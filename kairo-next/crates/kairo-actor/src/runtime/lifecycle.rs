use std::time::{Duration, Instant};

use crate::actor::Context;
use crate::death_watch::TerminationCause;
use crate::error::ActorError;
use crate::path::ActorPath;
use crate::refs::LocalActorHandle;
use crate::system::ActorSystemInner;

pub(super) fn stop_adapter_refs<M>(system_inner: &ActorSystemInner, context: &mut Context<M>)
where
    M: Send + 'static,
{
    for adapter_path in context.stop_adapters() {
        system_inner
            .death_watch
            .notify(&adapter_path, TerminationCause::Stopped);
    }
}

pub(super) fn stop_children(system_inner: &ActorSystemInner, parent_path: &str) {
    let _ = stop_children_with_timeout(system_inner, parent_path, Duration::MAX);
}

pub(super) fn stop_children_for_restart(system_inner: &ActorSystemInner, parent_path: &ActorPath) {
    let children = system_inner.registry.child_handles(parent_path.as_str());

    for child in &children {
        system_inner.death_watch.unwatch(child.path(), parent_path);
    }

    let _ = stop_child_handles_with_timeout(children, Duration::MAX);
}

pub(super) fn stop_children_except_for_restart(
    system_inner: &ActorSystemInner,
    parent_path: &ActorPath,
    preserved_children: &[LocalActorHandle],
) {
    let children = system_inner
        .registry
        .child_handles(parent_path.as_str())
        .into_iter()
        .filter(|child| {
            !preserved_children
                .iter()
                .any(|preserved| preserved.path() == child.path())
        })
        .collect::<Vec<_>>();

    for child in &children {
        system_inner.death_watch.unwatch(child.path(), parent_path);
    }

    let _ = stop_child_handles_with_timeout(children, Duration::MAX);
}

pub(super) fn restart_children_after_parent_restart(children: &[LocalActorHandle]) {
    for child in children {
        child.request_restart();
    }
}

pub(crate) fn stop_children_with_timeout(
    system_inner: &ActorSystemInner,
    parent_path: &str,
    timeout: Duration,
) -> Result<(), ActorError> {
    let children = system_inner.registry.child_handles(parent_path);
    stop_child_handles_until_deadline(children, deadline_after(timeout))
}

pub(crate) fn stop_child_roots_until_deadline(
    system_inner: &ActorSystemInner,
    parent_paths: &[&str],
    deadline: Instant,
) -> Result<(), ActorError> {
    let children = parent_paths
        .iter()
        .flat_map(|parent_path| system_inner.registry.child_handles(parent_path))
        .collect();
    stop_child_handles_until_deadline(children, deadline)
}

fn stop_child_handles_with_timeout(
    children: Vec<LocalActorHandle>,
    timeout: Duration,
) -> Result<(), ActorError> {
    stop_child_handles_until_deadline(children, deadline_after(timeout))
}

fn stop_child_handles_until_deadline(
    children: Vec<LocalActorHandle>,
    deadline: Instant,
) -> Result<(), ActorError> {
    for child in &children {
        child.request_stop();
    }

    for child in children {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(ActorError::TerminationTimeout)?;
        if !child.wait_for_stop(remaining) {
            return Err(ActorError::TerminationTimeout);
        }
    }
    Ok(())
}

fn deadline_after(timeout: Duration) -> Instant {
    Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(60 * 60 * 24 * 365))
}
