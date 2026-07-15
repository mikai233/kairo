use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};

use crate::{
    ClusterSeedJoinEffect, ClusterSeedJoinPhase, ClusterSeedJoinState, InitJoinAck, InitJoinNack,
};

const SEED_RETRY_TIMER: &str = "cluster-seed-join-retry";
const SEED_TIMEOUT_TIMER: &str = "cluster-seed-join-timeout";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterSeedJoinProcessSettings {
    retry_interval: Duration,
    seed_timeout: Duration,
    automatic_ticks: bool,
}

impl ClusterSeedJoinProcessSettings {
    pub fn new(
        retry_interval: Duration,
        seed_timeout: Duration,
    ) -> Result<Self, ClusterSeedJoinProcessSettingsError> {
        if retry_interval.is_zero() {
            return Err(ClusterSeedJoinProcessSettingsError::ZeroRetryInterval);
        }
        if seed_timeout.is_zero() {
            return Err(ClusterSeedJoinProcessSettingsError::ZeroSeedTimeout);
        }
        if seed_timeout < retry_interval {
            return Err(
                ClusterSeedJoinProcessSettingsError::SeedTimeoutBeforeRetry {
                    retry_interval,
                    seed_timeout,
                },
            );
        }
        Ok(Self {
            retry_interval,
            seed_timeout,
            automatic_ticks: true,
        })
    }

    pub fn retry_interval(self) -> Duration {
        self.retry_interval
    }

    pub fn seed_timeout(self) -> Duration {
        self.seed_timeout
    }

    pub fn automatic_ticks(self) -> bool {
        self.automatic_ticks
    }

    pub fn with_automatic_ticks(mut self, automatic_ticks: bool) -> Self {
        self.automatic_ticks = automatic_ticks;
        self
    }
}

impl Default for ClusterSeedJoinProcessSettings {
    fn default() -> Self {
        Self {
            retry_interval: Duration::from_secs(1),
            seed_timeout: Duration::from_secs(5),
            automatic_ticks: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterSeedJoinProcessSettingsError {
    ZeroRetryInterval,
    ZeroSeedTimeout,
    SeedTimeoutBeforeRetry {
        retry_interval: Duration,
        seed_timeout: Duration,
    },
}

impl Display for ClusterSeedJoinProcessSettingsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRetryInterval => {
                write!(f, "cluster seed retry interval must be greater than zero")
            }
            Self::ZeroSeedTimeout => {
                write!(f, "cluster seed timeout must be greater than zero")
            }
            Self::SeedTimeoutBeforeRetry {
                retry_interval,
                seed_timeout,
            } => write!(
                f,
                "cluster seed timeout {seed_timeout:?} is less than retry interval {retry_interval:?}"
            ),
        }
    }
}

impl Error for ClusterSeedJoinProcessSettingsError {}

#[derive(Debug, Clone)]
pub enum ClusterSeedJoinProcessMsg {
    Ack {
        origin: kairo_actor::Address,
        message: InitJoinAck,
    },
    Nack {
        origin: kairo_actor::Address,
        message: InitJoinNack,
    },
    Welcome {
        from: kairo_actor::Address,
    },
    RetryTick,
    SeedTimeoutTick,
    Snapshot {
        reply_to: ActorRef<ClusterSeedJoinProcessSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSeedJoinProcessSnapshot {
    pub phase: ClusterSeedJoinPhase,
    pub attempts: u32,
}

pub struct ClusterSeedJoinProcess {
    state: ClusterSeedJoinState,
    effects: ActorRef<ClusterSeedJoinEffect>,
    settings: ClusterSeedJoinProcessSettings,
}

impl ClusterSeedJoinProcess {
    pub fn new(
        state: ClusterSeedJoinState,
        effects: ActorRef<ClusterSeedJoinEffect>,
        settings: ClusterSeedJoinProcessSettings,
    ) -> Self {
        Self {
            state,
            effects,
            settings,
        }
    }

    pub fn state(&self) -> &ClusterSeedJoinState {
        &self.state
    }

    fn emit(&self, effects: Vec<ClusterSeedJoinEffect>) -> ActorResult {
        for effect in effects {
            self.effects
                .tell(effect)
                .map_err(|error| ActorError::Message(error.reason().to_string()))?;
        }
        Ok(())
    }

    fn cancel_terminal_timers(&self, ctx: &mut Context<ClusterSeedJoinProcessMsg>) {
        if matches!(
            self.state.phase(),
            ClusterSeedJoinPhase::Complete { .. } | ClusterSeedJoinPhase::Incompatible { .. }
        ) {
            ctx.cancel_timer(SEED_RETRY_TIMER);
            ctx.cancel_timer(SEED_TIMEOUT_TIMER);
        }
    }

    fn snapshot(&self) -> ClusterSeedJoinProcessSnapshot {
        ClusterSeedJoinProcessSnapshot {
            phase: self.state.phase().clone(),
            attempts: self.state.attempts(),
        }
    }
}

impl Actor for ClusterSeedJoinProcess {
    type Msg = ClusterSeedJoinProcessMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let initial_effects = self.state.start();
        self.emit(initial_effects)?;
        if self.settings.automatic_ticks
            && !matches!(
                self.state.phase(),
                ClusterSeedJoinPhase::Complete { .. } | ClusterSeedJoinPhase::Incompatible { .. }
            )
        {
            ctx.start_timer_with_fixed_delay(
                SEED_RETRY_TIMER,
                self.settings.retry_interval,
                self.settings.retry_interval,
                ClusterSeedJoinProcessMsg::RetryTick,
            );
            ctx.start_timer_with_fixed_delay(
                SEED_TIMEOUT_TIMER,
                self.settings.seed_timeout,
                self.settings.seed_timeout,
                ClusterSeedJoinProcessMsg::SeedTimeoutTick,
            );
        }
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        let effects = match msg {
            ClusterSeedJoinProcessMsg::Ack { origin, message } => {
                self.state.receive_ack(&origin, message)
            }
            ClusterSeedJoinProcessMsg::Nack { origin, message } => {
                self.state.receive_nack(&origin, message)
            }
            ClusterSeedJoinProcessMsg::Welcome { from } => {
                self.state.receive_welcome(&from);
                Vec::new()
            }
            ClusterSeedJoinProcessMsg::RetryTick => self.state.retry(),
            ClusterSeedJoinProcessMsg::SeedTimeoutTick => self.state.seed_timeout(),
            ClusterSeedJoinProcessMsg::Snapshot { reply_to } => {
                reply_to
                    .tell(self.snapshot())
                    .map_err(|error| ActorError::Message(error.reason().to_string()))?;
                Vec::new()
            }
        };
        self.emit(effects)?;
        self.cancel_terminal_timers(ctx);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kairo_actor::{Address, Props};
    use kairo_testkit::ActorSystemTestKit;

    use super::*;
    use crate::ClusterConfigCheck;

    #[test]
    fn process_automatically_contacts_retries_and_self_forms_first_seed() {
        let (kit, time) = ActorSystemTestKit::with_manual_time("seed-process-first").unwrap();
        let effects = kit
            .create_probe::<ClusterSeedJoinEffect>("effects")
            .unwrap();
        let self_address = address("seed-1", 2551);
        let seed_2 = address("seed-2", 2552);
        let state = ClusterSeedJoinState::new(
            self_address,
            vec![address("seed-1", 2551), seed_2.clone()],
            Bytes::from_static(b"digest"),
        )
        .unwrap();
        let settings = ClusterSeedJoinProcessSettings::new(
            Duration::from_secs(1),
            Duration::from_millis(2500),
        )
        .unwrap();
        let effect_ref = effects.actor_ref();
        let process = kit
            .system()
            .spawn(
                "seed-process",
                Props::new(move || {
                    ClusterSeedJoinProcess::new(state.clone(), effect_ref.clone(), settings)
                }),
            )
            .unwrap();

        expect_contact(&effects, &seed_2);
        time.advance(Duration::from_secs(1));
        expect_contact(&effects, &seed_2);
        time.advance(Duration::from_millis(1500));
        expect_contact(&effects, &seed_2);
        assert_eq!(
            effects.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterSeedJoinEffect::JoinSelf
        );
        time.advance(Duration::from_secs(5));
        effects.expect_no_msg(Duration::from_millis(50)).unwrap();

        let snapshots = kit
            .create_probe::<ClusterSeedJoinProcessSnapshot>("snapshots")
            .unwrap();
        process
            .tell(ClusterSeedJoinProcessMsg::Snapshot {
                reply_to: snapshots.actor_ref(),
            })
            .unwrap();
        assert!(matches!(
            snapshots.expect_msg(Duration::from_secs(1)).unwrap().phase,
            ClusterSeedJoinPhase::Complete { .. }
        ));
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn process_selects_first_ack_recontacts_after_lost_welcome_and_completes() {
        let (kit, time) = ActorSystemTestKit::with_manual_time("seed-process-ack").unwrap();
        let effects = kit
            .create_probe::<ClusterSeedJoinEffect>("effects")
            .unwrap();
        let seed_1 = address("seed-1", 2551);
        let seed_2 = address("seed-2", 2552);
        let canonical = address("canonical", 2553);
        let state = ClusterSeedJoinState::new(
            address("node", 2554),
            vec![seed_1.clone(), seed_2.clone()],
            Bytes::new(),
        )
        .unwrap();
        let settings =
            ClusterSeedJoinProcessSettings::new(Duration::from_secs(1), Duration::from_secs(3))
                .unwrap();
        let effect_ref = effects.actor_ref();
        let process = kit
            .system()
            .spawn(
                "seed-process",
                Props::new(move || {
                    ClusterSeedJoinProcess::new(state.clone(), effect_ref.clone(), settings)
                }),
            )
            .unwrap();
        expect_contact(&effects, &seed_1);
        expect_contact(&effects, &seed_2);

        process
            .tell(ClusterSeedJoinProcessMsg::Ack {
                origin: seed_2.clone(),
                message: InitJoinAck {
                    address: canonical.clone(),
                    config_check: ClusterConfigCheck::Compatible,
                },
            })
            .unwrap();
        assert_eq!(
            effects.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterSeedJoinEffect::Join {
                target: canonical.clone()
            }
        );
        time.advance(Duration::from_secs(3));
        expect_contact(&effects, &seed_1);
        expect_contact(&effects, &seed_2);
        process
            .tell(ClusterSeedJoinProcessMsg::Ack {
                origin: seed_1,
                message: InitJoinAck {
                    address: canonical.clone(),
                    config_check: ClusterConfigCheck::Unchecked,
                },
            })
            .unwrap();
        assert_eq!(
            effects.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterSeedJoinEffect::Join {
                target: canonical.clone()
            }
        );
        process
            .tell(ClusterSeedJoinProcessMsg::Welcome {
                from: canonical.clone(),
            })
            .unwrap();
        time.advance(Duration::from_secs(6));
        effects.expect_no_msg(Duration::from_millis(50)).unwrap();
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn process_stop_cancels_automatic_seed_timers() {
        let (kit, time) = ActorSystemTestKit::with_manual_time("seed-process-stop").unwrap();
        let effects = kit
            .create_probe::<ClusterSeedJoinEffect>("effects")
            .unwrap();
        let seed = address("seed", 2551);
        let state =
            ClusterSeedJoinState::new(address("node", 2554), vec![seed.clone()], Bytes::new())
                .unwrap();
        let effect_ref = effects.actor_ref();
        let process = kit
            .system()
            .spawn(
                "seed-process",
                Props::new(move || {
                    ClusterSeedJoinProcess::new(
                        state.clone(),
                        effect_ref.clone(),
                        ClusterSeedJoinProcessSettings::default(),
                    )
                }),
            )
            .unwrap();
        expect_contact(&effects, &seed);

        kit.system().stop(&process);
        assert!(process.wait_for_stop(Duration::from_secs(1)));
        time.advance(Duration::from_secs(10));
        effects.expect_no_msg(Duration::from_millis(50)).unwrap();
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn process_settings_reject_invalid_durations() {
        assert!(matches!(
            ClusterSeedJoinProcessSettings::new(Duration::ZERO, Duration::from_secs(1)),
            Err(ClusterSeedJoinProcessSettingsError::ZeroRetryInterval)
        ));
        assert!(matches!(
            ClusterSeedJoinProcessSettings::new(Duration::from_secs(1), Duration::ZERO),
            Err(ClusterSeedJoinProcessSettingsError::ZeroSeedTimeout)
        ));
        assert!(matches!(
            ClusterSeedJoinProcessSettings::new(Duration::from_secs(2), Duration::from_secs(1)),
            Err(ClusterSeedJoinProcessSettingsError::SeedTimeoutBeforeRetry { .. })
        ));
    }

    fn expect_contact(
        effects: &kairo_testkit::TestProbe<ClusterSeedJoinEffect>,
        expected: &Address,
    ) {
        assert!(matches!(
            effects.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterSeedJoinEffect::Contact { target, .. } if target == *expected
        ));
    }

    fn address(system: &str, port: u16) -> Address {
        Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port))
    }
}
