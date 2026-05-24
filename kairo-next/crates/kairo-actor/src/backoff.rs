use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::actor::{Actor, Context, Props};
use crate::error::ActorResult;
use crate::refs::ActorRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffSupervisorSettings {
    min_backoff: Duration,
    max_backoff: Duration,
    reset: BackoffReset,
}

impl BackoffSupervisorSettings {
    pub fn new(min_backoff: Duration, max_backoff: Duration) -> Result<Self, BackoffSettingsError> {
        validate_backoff(min_backoff, max_backoff)?;
        Ok(Self {
            min_backoff,
            max_backoff,
            reset: BackoffReset::Auto { after: min_backoff },
        })
    }

    pub fn min_backoff(&self) -> Duration {
        self.min_backoff
    }

    pub fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    pub fn reset(&self) -> BackoffReset {
        self.reset
    }

    pub fn with_auto_reset_after(mut self, after: Duration) -> Result<Self, BackoffSettingsError> {
        if after < self.min_backoff || after > self.max_backoff {
            return Err(BackoffSettingsError::InvalidReset {
                reset_after: after,
                min_backoff: self.min_backoff,
                max_backoff: self.max_backoff,
            });
        }
        self.reset = BackoffReset::Auto { after };
        Ok(self)
    }

    pub fn with_manual_reset(mut self) -> Self {
        self.reset = BackoffReset::Manual;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffReset {
    Auto { after: Duration },
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackoffSettingsError {
    MinBackoffIsZero,
    MaxBackoffBeforeMin {
        min_backoff: Duration,
        max_backoff: Duration,
    },
    InvalidReset {
        reset_after: Duration,
        min_backoff: Duration,
        max_backoff: Duration,
    },
}

impl Display for BackoffSettingsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MinBackoffIsZero => write!(f, "min_backoff must be greater than zero"),
            Self::MaxBackoffBeforeMin {
                min_backoff,
                max_backoff,
            } => write!(
                f,
                "max_backoff ({max_backoff:?}) must be greater than or equal to min_backoff ({min_backoff:?})"
            ),
            Self::InvalidReset {
                reset_after,
                min_backoff,
                max_backoff,
            } => write!(
                f,
                "reset_after ({reset_after:?}) must be between min_backoff ({min_backoff:?}) and max_backoff ({max_backoff:?})"
            ),
        }
    }
}

impl std::error::Error for BackoffSettingsError {}

pub enum BackoffSupervisorMsg<M: Send + 'static> {
    Tell(M),
    GetCurrentChild {
        reply_to: ActorRef<CurrentChild<M>>,
    },
    GetRestartCount {
        reply_to: ActorRef<RestartCount>,
    },
    Reset,
    #[doc(hidden)]
    ChildTerminated,
    #[doc(hidden)]
    StartChild {
        token: u64,
    },
    #[doc(hidden)]
    ResetRestartCount {
        restart_count: u32,
    },
}

impl<M: Send + 'static> fmt::Debug for BackoffSupervisorMsg<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tell(_) => f.write_str("Tell(..)"),
            Self::GetCurrentChild { reply_to } => f
                .debug_struct("GetCurrentChild")
                .field("reply_to", reply_to)
                .finish(),
            Self::GetRestartCount { reply_to } => f
                .debug_struct("GetRestartCount")
                .field("reply_to", reply_to)
                .finish(),
            Self::Reset => f.write_str("Reset"),
            Self::ChildTerminated => f.write_str("ChildTerminated"),
            Self::StartChild { token } => {
                f.debug_struct("StartChild").field("token", token).finish()
            }
            Self::ResetRestartCount { restart_count } => f
                .debug_struct("ResetRestartCount")
                .field("restart_count", restart_count)
                .finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CurrentChild<M: Send + 'static> {
    child: Option<ActorRef<M>>,
}

impl<M: Send + 'static> CurrentChild<M> {
    pub fn child(&self) -> Option<ActorRef<M>> {
        self.child.clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestartCount {
    count: u32,
}

impl RestartCount {
    pub fn count(&self) -> u32 {
        self.count
    }
}

pub struct BackoffSupervisor<A>
where
    A: Actor,
{
    child_name: String,
    child_factory: Arc<dyn Fn() -> Props<A> + Send + Sync>,
    settings: BackoffSupervisorSettings,
    state: BackoffState,
    child: Option<ActorRef<A::Msg>>,
    _actor: PhantomData<fn(A)>,
}

impl<A> BackoffSupervisor<A>
where
    A: Actor,
{
    pub fn on_stop<F>(
        child_name: impl Into<String>,
        child_factory: F,
        settings: BackoffSupervisorSettings,
    ) -> Props<Self>
    where
        F: Fn() -> Props<A> + Send + Sync + 'static,
    {
        let child_name = child_name.into();
        let child_factory: Arc<dyn Fn() -> Props<A> + Send + Sync> = Arc::new(child_factory);
        Props::new(move || Self {
            child_name,
            child_factory,
            settings,
            state: BackoffState::default(),
            child: None,
            _actor: PhantomData,
        })
    }

    fn start_child(&mut self, ctx: &mut Context<BackoffSupervisorMsg<A::Msg>>) -> ActorResult {
        if self.child.is_some() {
            return Ok(());
        }

        let child = ctx.spawn(&self.child_name, (self.child_factory)())?;
        ctx.watch_with(&child, BackoffSupervisorMsg::ChildTerminated)?;
        self.child = Some(child);

        if let Some((delay, restart_count)) = self.state.child_started(self.settings.reset) {
            ctx.schedule_once_self(
                delay,
                BackoffSupervisorMsg::ResetRestartCount { restart_count },
            );
        }

        Ok(())
    }

    fn schedule_restart(&mut self, ctx: &Context<BackoffSupervisorMsg<A::Msg>>) {
        let (delay, token) = self.state.child_terminated(self.settings);
        ctx.schedule_once_self(delay, BackoffSupervisorMsg::StartChild { token });
    }
}

impl<A> Actor for BackoffSupervisor<A>
where
    A: Actor,
{
    type Msg = BackoffSupervisorMsg<A::Msg>;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.start_child(ctx)
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            BackoffSupervisorMsg::Tell(message) => {
                if let Some(child) = &self.child {
                    let _ = child.tell(message);
                } else {
                    ctx.system()
                        .dead_letters()
                        .publish::<A::Msg>(ctx.myself().path().clone(), "backoff child is stopped");
                }
            }
            BackoffSupervisorMsg::GetCurrentChild { reply_to } => {
                let _ = reply_to.tell(CurrentChild {
                    child: self.child.clone(),
                });
            }
            BackoffSupervisorMsg::GetRestartCount { reply_to } => {
                let _ = reply_to.tell(RestartCount {
                    count: self.state.restart_count(),
                });
            }
            BackoffSupervisorMsg::Reset => {
                self.state.reset_restart_count();
            }
            BackoffSupervisorMsg::ChildTerminated => {
                self.child = None;
                self.schedule_restart(ctx);
            }
            BackoffSupervisorMsg::StartChild { token } => {
                if self.state.accept_start(token) {
                    self.start_child(ctx)?;
                }
            }
            BackoffSupervisorMsg::ResetRestartCount { restart_count } => {
                self.state.reset_if_unchanged(restart_count);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct BackoffState {
    restart_count: u32,
    next_start_token: u64,
}

impl BackoffState {
    fn restart_count(&self) -> u32 {
        self.restart_count
    }

    fn reset_restart_count(&mut self) {
        self.restart_count = 0;
    }

    fn child_started(&self, reset: BackoffReset) -> Option<(Duration, u32)> {
        match reset {
            BackoffReset::Auto { after } if self.restart_count > 0 => {
                Some((after, self.restart_count))
            }
            _ => None,
        }
    }

    fn child_terminated(&mut self, settings: BackoffSupervisorSettings) -> (Duration, u64) {
        let delay = calculate_delay(
            self.restart_count,
            settings.min_backoff,
            settings.max_backoff,
        );
        self.restart_count = self.restart_count.saturating_add(1);
        self.next_start_token = self.next_start_token.saturating_add(1);
        (delay, self.next_start_token)
    }

    fn accept_start(&self, token: u64) -> bool {
        token == self.next_start_token
    }

    fn reset_if_unchanged(&mut self, restart_count: u32) {
        if self.restart_count == restart_count && self.restart_count > 0 {
            self.restart_count = 0;
        }
    }
}

fn calculate_delay(restart_count: u32, min_backoff: Duration, max_backoff: Duration) -> Duration {
    let multiplier = 1u32.checked_shl(restart_count).unwrap_or(u32::MAX);
    min_backoff.saturating_mul(multiplier).min(max_backoff)
}

fn validate_backoff(
    min_backoff: Duration,
    max_backoff: Duration,
) -> Result<(), BackoffSettingsError> {
    if min_backoff == Duration::ZERO {
        return Err(BackoffSettingsError::MinBackoffIsZero);
    }
    if max_backoff < min_backoff {
        return Err(BackoffSettingsError::MaxBackoffBeforeMin {
            min_backoff,
            max_backoff,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_state_calculates_exponential_delay_with_cap() {
        let settings =
            BackoffSupervisorSettings::new(Duration::from_millis(100), Duration::from_millis(250))
                .unwrap();
        let mut state = BackoffState::default();

        assert_eq!(
            state.child_terminated(settings).0,
            Duration::from_millis(100)
        );
        assert_eq!(
            state.child_terminated(settings).0,
            Duration::from_millis(200)
        );
        assert_eq!(
            state.child_terminated(settings).0,
            Duration::from_millis(250)
        );
        assert_eq!(state.restart_count(), 3);
    }

    #[test]
    fn auto_reset_only_resets_matching_restart_generation() {
        let mut state = BackoffState {
            restart_count: 2,
            next_start_token: 4,
        };

        state.reset_if_unchanged(1);
        assert_eq!(state.restart_count(), 2);

        state.reset_if_unchanged(2);
        assert_eq!(state.restart_count(), 0);
    }
}
