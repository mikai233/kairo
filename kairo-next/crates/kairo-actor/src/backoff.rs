use std::collections::hash_map::RandomState;
use std::fmt::{self, Display, Formatter};
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::actor::{Actor, Context, Props};
use crate::error::ActorResult;
use crate::refs::ActorRef;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BackoffSupervisorSettings {
    min_backoff: Duration,
    max_backoff: Duration,
    random_factor: f64,
    max_restarts: Option<u32>,
    reset: BackoffReset,
}

impl BackoffSupervisorSettings {
    pub fn new(min_backoff: Duration, max_backoff: Duration) -> Result<Self, BackoffSettingsError> {
        validate_backoff(min_backoff, max_backoff)?;
        Ok(Self {
            min_backoff,
            max_backoff,
            random_factor: 0.0,
            max_restarts: None,
            reset: BackoffReset::Auto {
                after: default_reset_after(min_backoff, max_backoff),
            },
        })
    }

    pub fn min_backoff(&self) -> Duration {
        self.min_backoff
    }

    pub fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    pub fn random_factor(&self) -> f64 {
        self.random_factor
    }

    pub fn max_restarts(&self) -> Option<u32> {
        self.max_restarts
    }

    pub fn reset(&self) -> BackoffReset {
        self.reset
    }

    pub fn with_random_factor(mut self, factor: f64) -> Result<Self, BackoffSettingsError> {
        if !factor.is_finite() || factor < 0.0 {
            return Err(BackoffSettingsError::InvalidRandomFactor { factor });
        }
        self.random_factor = factor;
        Ok(self)
    }

    pub fn with_max_restarts(mut self, max_restarts: u32) -> Self {
        self.max_restarts = if max_restarts == 0 {
            None
        } else {
            Some(max_restarts)
        };
        self
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

#[derive(Debug, Clone, PartialEq)]
pub enum BackoffSettingsError {
    MinBackoffIsZero,
    MaxBackoffBeforeMin {
        min_backoff: Duration,
        max_backoff: Duration,
    },
    InvalidRandomFactor {
        factor: f64,
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
            Self::InvalidRandomFactor { factor } => {
                write!(
                    f,
                    "random_factor ({factor}) must be finite and non-negative"
                )
            }
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

    fn schedule_restart(&mut self, ctx: &mut Context<BackoffSupervisorMsg<A::Msg>>) -> ActorResult {
        if self.state.restart_limit_reached(self.settings) {
            ctx.stop(ctx.myself())?;
            return Ok(());
        }

        let (delay, token) = self.state.child_terminated(self.settings);
        ctx.schedule_once_self(delay, BackoffSupervisorMsg::StartChild { token });
        Ok(())
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
                self.schedule_restart(ctx)?;
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
            settings.random_factor,
            self.jitter_fraction(),
        );
        self.restart_count = self.restart_count.saturating_add(1);
        self.next_start_token = self.next_start_token.saturating_add(1);
        (delay, self.next_start_token)
    }

    fn restart_limit_reached(&self, settings: BackoffSupervisorSettings) -> bool {
        settings
            .max_restarts
            .is_some_and(|max_restarts| self.restart_count >= max_restarts)
    }

    fn accept_start(&self, token: u64) -> bool {
        token == self.next_start_token
    }

    fn reset_if_unchanged(&mut self, restart_count: u32) {
        if self.restart_count == restart_count && self.restart_count > 0 {
            self.restart_count = 0;
        }
    }

    fn jitter_fraction(&self) -> f64 {
        let mut hasher = RandomState::new().build_hasher();
        self.restart_count.hash(&mut hasher);
        self.next_start_token.hash(&mut hasher);
        let hash = hasher.finish();
        (hash as f64) / (u64::MAX as f64)
    }
}

fn calculate_delay(
    restart_count: u32,
    min_backoff: Duration,
    max_backoff: Duration,
    random_factor: f64,
    jitter_fraction: f64,
) -> Duration {
    let multiplier = 1u32.checked_shl(restart_count).unwrap_or(u32::MAX);
    let base = min_backoff.saturating_mul(multiplier).min(max_backoff);
    if random_factor == 0.0 {
        return base;
    }

    let multiplier = 1.0 + jitter_fraction.clamp(0.0, 1.0) * random_factor;
    duration_mul_f64_saturating(base, multiplier)
}

fn duration_mul_f64_saturating(duration: Duration, multiplier: f64) -> Duration {
    let seconds = duration.as_secs_f64() * multiplier;
    if !seconds.is_finite() || seconds >= Duration::MAX.as_secs_f64() {
        Duration::MAX
    } else {
        Duration::from_secs_f64(seconds)
    }
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

fn default_reset_after(min_backoff: Duration, max_backoff: Duration) -> Duration {
    min_backoff.saturating_add(max_backoff) / 2
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
    fn backoff_settings_default_reset_uses_min_max_midpoint() {
        let settings =
            BackoffSupervisorSettings::new(Duration::from_millis(100), Duration::from_millis(300))
                .unwrap();

        assert_eq!(
            settings.reset(),
            BackoffReset::Auto {
                after: Duration::from_millis(200)
            }
        );
    }

    #[test]
    fn backoff_settings_default_to_unlimited_restarts() {
        let settings =
            BackoffSupervisorSettings::new(Duration::from_millis(100), Duration::from_millis(300))
                .unwrap();

        assert_eq!(settings.max_restarts(), None);
        assert_eq!(settings.with_max_restarts(2).max_restarts(), Some(2));
        assert_eq!(settings.with_max_restarts(0).max_restarts(), None);
    }

    #[test]
    fn backoff_state_applies_random_factor_after_exponential_cap() {
        let settings =
            BackoffSupervisorSettings::new(Duration::from_millis(100), Duration::from_millis(250))
                .unwrap()
                .with_random_factor(0.2)
                .unwrap();

        assert_eq!(
            calculate_delay(
                0,
                settings.min_backoff(),
                settings.max_backoff(),
                settings.random_factor(),
                0.0
            ),
            Duration::from_millis(100)
        );
        assert_eq!(
            calculate_delay(
                0,
                settings.min_backoff(),
                settings.max_backoff(),
                settings.random_factor(),
                1.0
            ),
            Duration::from_millis(120)
        );
        assert_eq!(
            calculate_delay(
                3,
                settings.min_backoff(),
                settings.max_backoff(),
                settings.random_factor(),
                1.0
            ),
            Duration::from_millis(300)
        );
    }

    #[test]
    fn backoff_settings_reject_invalid_random_factor() {
        let settings =
            BackoffSupervisorSettings::new(Duration::from_millis(100), Duration::from_millis(250))
                .unwrap();

        assert!(matches!(
            settings.with_random_factor(-0.1),
            Err(BackoffSettingsError::InvalidRandomFactor { .. })
        ));
        assert!(matches!(
            settings.with_random_factor(f64::NAN),
            Err(BackoffSettingsError::InvalidRandomFactor { .. })
        ));
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

    #[test]
    fn backoff_state_reports_restart_limit_before_scheduling_next_restart() {
        let settings =
            BackoffSupervisorSettings::new(Duration::from_millis(100), Duration::from_millis(250))
                .unwrap()
                .with_max_restarts(2);
        let mut state = BackoffState::default();

        assert!(!state.restart_limit_reached(settings));
        state.child_terminated(settings);
        assert!(!state.restart_limit_reached(settings));
        state.child_terminated(settings);
        assert!(state.restart_limit_reached(settings));
    }
}
