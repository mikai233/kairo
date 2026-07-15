use std::error::Error;
use std::fmt::{self, Display, Formatter};

use bytes::Bytes;
use kairo_actor::Address;

use crate::{ClusterConfigCheck, InitJoin, InitJoinAck, InitJoinNack};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterSeedJoinPhase {
    Ready,
    Contacting,
    Joining { target: Address },
    Complete { joined_to: Address },
    Incompatible { target: Address },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterSeedJoinEffect {
    Contact { target: Address, message: InitJoin },
    Join { target: Address },
    JoinSelf,
    RejectIncompatible { target: Address },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterSeedJoinError {
    EmptySeedNodes,
    DuplicateSeedNode { address: Address },
}

impl Display for ClusterSeedJoinError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySeedNodes => write!(f, "cluster seed nodes must not be empty"),
            Self::DuplicateSeedNode { address } => {
                write!(f, "duplicate cluster seed node `{address}`")
            }
        }
    }
}

impl Error for ClusterSeedJoinError {}

#[derive(Debug, Clone)]
pub struct ClusterSeedJoinState {
    self_address: Address,
    seed_nodes: Vec<Address>,
    contact_nodes: Vec<Address>,
    config_digest: Bytes,
    phase: ClusterSeedJoinPhase,
    attempts: u32,
    nacked_origins: Vec<Address>,
}

impl ClusterSeedJoinState {
    pub fn new(
        self_address: Address,
        seed_nodes: Vec<Address>,
        config_digest: Bytes,
    ) -> Result<Self, ClusterSeedJoinError> {
        if seed_nodes.is_empty() {
            return Err(ClusterSeedJoinError::EmptySeedNodes);
        }
        for (index, address) in seed_nodes.iter().enumerate() {
            if seed_nodes[..index].contains(address) {
                return Err(ClusterSeedJoinError::DuplicateSeedNode {
                    address: address.clone(),
                });
            }
        }
        let contact_nodes = seed_nodes
            .iter()
            .filter(|address| *address != &self_address)
            .cloned()
            .collect();
        Ok(Self {
            self_address,
            seed_nodes,
            contact_nodes,
            config_digest,
            phase: ClusterSeedJoinPhase::Ready,
            attempts: 0,
            nacked_origins: Vec::new(),
        })
    }

    pub fn self_address(&self) -> &Address {
        &self.self_address
    }

    pub fn seed_nodes(&self) -> &[Address] {
        &self.seed_nodes
    }

    pub fn phase(&self) -> &ClusterSeedJoinPhase {
        &self.phase
    }

    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    pub fn start(&mut self) -> Vec<ClusterSeedJoinEffect> {
        if self.phase != ClusterSeedJoinPhase::Ready {
            return Vec::new();
        }
        if self.is_first_seed() && self.contact_nodes.is_empty() {
            self.phase = ClusterSeedJoinPhase::Complete {
                joined_to: self.self_address.clone(),
            };
            return vec![ClusterSeedJoinEffect::JoinSelf];
        }
        self.begin_contact_attempt()
    }

    pub fn receive_ack(
        &mut self,
        origin: &Address,
        ack: InitJoinAck,
    ) -> Vec<ClusterSeedJoinEffect> {
        if self.phase != ClusterSeedJoinPhase::Contacting || !self.contact_nodes.contains(origin) {
            return Vec::new();
        }
        match ack.config_check {
            ClusterConfigCheck::Compatible | ClusterConfigCheck::Unchecked => {
                self.phase = ClusterSeedJoinPhase::Joining {
                    target: ack.address.clone(),
                };
                vec![ClusterSeedJoinEffect::Join {
                    target: ack.address,
                }]
            }
            ClusterConfigCheck::Incompatible => {
                self.phase = ClusterSeedJoinPhase::Incompatible {
                    target: ack.address.clone(),
                };
                vec![ClusterSeedJoinEffect::RejectIncompatible {
                    target: ack.address,
                }]
            }
        }
    }

    pub fn receive_nack(
        &mut self,
        origin: &Address,
        _nack: InitJoinNack,
    ) -> Vec<ClusterSeedJoinEffect> {
        if self.phase != ClusterSeedJoinPhase::Contacting
            || !self.contact_nodes.contains(origin)
            || self.nacked_origins.contains(origin)
        {
            return Vec::new();
        }
        self.nacked_origins.push(origin.clone());
        if self.is_first_seed() && self.nacked_origins.len() == self.contact_nodes.len() {
            self.phase = ClusterSeedJoinPhase::Complete {
                joined_to: self.self_address.clone(),
            };
            vec![ClusterSeedJoinEffect::JoinSelf]
        } else {
            Vec::new()
        }
    }

    pub fn retry(&mut self) -> Vec<ClusterSeedJoinEffect> {
        match &self.phase {
            ClusterSeedJoinPhase::Ready => self.start(),
            ClusterSeedJoinPhase::Contacting => self.begin_contact_attempt(),
            ClusterSeedJoinPhase::Joining { .. } => Vec::new(),
            ClusterSeedJoinPhase::Complete { .. } | ClusterSeedJoinPhase::Incompatible { .. } => {
                Vec::new()
            }
        }
    }

    pub fn seed_timeout(&mut self) -> Vec<ClusterSeedJoinEffect> {
        match &self.phase {
            ClusterSeedJoinPhase::Contacting if self.is_first_seed() => {
                self.phase = ClusterSeedJoinPhase::Complete {
                    joined_to: self.self_address.clone(),
                };
                vec![ClusterSeedJoinEffect::JoinSelf]
            }
            ClusterSeedJoinPhase::Ready | ClusterSeedJoinPhase::Contacting => self.retry(),
            ClusterSeedJoinPhase::Joining { .. } => self.begin_contact_attempt(),
            ClusterSeedJoinPhase::Complete { .. } | ClusterSeedJoinPhase::Incompatible { .. } => {
                Vec::new()
            }
        }
    }

    pub fn receive_welcome(&mut self, from: &Address) -> bool {
        let ClusterSeedJoinPhase::Joining { target } = &self.phase else {
            return false;
        };
        if target != from {
            return false;
        }
        self.phase = ClusterSeedJoinPhase::Complete {
            joined_to: from.clone(),
        };
        true
    }

    fn begin_contact_attempt(&mut self) -> Vec<ClusterSeedJoinEffect> {
        self.phase = ClusterSeedJoinPhase::Contacting;
        self.attempts = self.attempts.saturating_add(1);
        self.nacked_origins.clear();
        self.contact_nodes
            .iter()
            .cloned()
            .map(|target| ClusterSeedJoinEffect::Contact {
                target,
                message: InitJoin {
                    joining_config_digest: self.config_digest.clone(),
                },
            })
            .collect()
    }

    fn is_first_seed(&self) -> bool {
        self.seed_nodes.first() == Some(&self.self_address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_first_seed_self_forms_without_contacting_itself() {
        let self_address = address("seed-1", 2551);
        let mut state = ClusterSeedJoinState::new(
            self_address.clone(),
            vec![self_address.clone()],
            Bytes::from_static(b"digest"),
        )
        .unwrap();

        assert_eq!(state.start(), vec![ClusterSeedJoinEffect::JoinSelf]);
        assert_eq!(
            state.phase(),
            &ClusterSeedJoinPhase::Complete {
                joined_to: self_address
            }
        );
    }

    #[test]
    fn first_seed_self_forms_only_after_every_other_seed_nacks() {
        let self_address = address("seed-1", 2551);
        let seed_2 = address("seed-2", 2552);
        let seed_3 = address("seed-3", 2553);
        let mut state = ClusterSeedJoinState::new(
            self_address.clone(),
            vec![self_address, seed_2.clone(), seed_3.clone()],
            Bytes::from_static(b"digest"),
        )
        .unwrap();

        let effects = state.start();
        assert_eq!(effects.len(), 2);
        assert!(
            matches!(&effects[0], ClusterSeedJoinEffect::Contact { target, .. } if target == &seed_2)
        );
        assert!(
            matches!(&effects[1], ClusterSeedJoinEffect::Contact { target, .. } if target == &seed_3)
        );
        assert!(
            state
                .receive_nack(
                    &seed_2,
                    InitJoinNack {
                        address: seed_2.clone()
                    }
                )
                .is_empty()
        );
        assert_eq!(
            state.receive_nack(
                &seed_3,
                InitJoinNack {
                    address: seed_3.clone()
                }
            ),
            vec![ClusterSeedJoinEffect::JoinSelf]
        );
    }

    #[test]
    fn first_seed_self_forms_after_seed_timeout_without_replies() {
        let self_address = address("seed-1", 2551);
        let seed_2 = address("seed-2", 2552);
        let mut state = ClusterSeedJoinState::new(
            self_address.clone(),
            vec![self_address.clone(), seed_2],
            Bytes::new(),
        )
        .unwrap();
        state.start();

        assert_eq!(state.seed_timeout(), vec![ClusterSeedJoinEffect::JoinSelf]);
        assert_eq!(
            state.phase(),
            &ClusterSeedJoinPhase::Complete {
                joined_to: self_address
            }
        );
    }

    #[test]
    fn non_first_seed_retries_after_nacks_without_self_forming() {
        let self_address = address("node", 2554);
        let seed_1 = address("seed-1", 2551);
        let seed_2 = address("seed-2", 2552);
        let mut state = ClusterSeedJoinState::new(
            self_address,
            vec![seed_1.clone(), seed_2.clone()],
            Bytes::new(),
        )
        .unwrap();

        state.start();
        assert!(
            state
                .receive_nack(
                    &seed_1,
                    InitJoinNack {
                        address: seed_1.clone()
                    }
                )
                .is_empty()
        );
        assert!(
            state
                .receive_nack(
                    &seed_2,
                    InitJoinNack {
                        address: seed_2.clone()
                    }
                )
                .is_empty()
        );
        assert_eq!(state.phase(), &ClusterSeedJoinPhase::Contacting);

        let retry = state.retry();
        assert_eq!(retry.len(), 2);
        assert_eq!(state.attempts(), 2);
        assert_eq!(state.seed_timeout().len(), 2);
        assert_eq!(state.attempts(), 3);
    }

    #[test]
    fn first_compatible_ack_selects_target_and_timeout_restarts_seed_contact() {
        let self_address = address("node", 2554);
        let seed_1 = address("seed-1", 2551);
        let seed_2 = address("seed-2", 2552);
        let advertised = address("canonical-seed", 2555);
        let mut state = ClusterSeedJoinState::new(
            self_address,
            vec![seed_1.clone(), seed_2.clone()],
            Bytes::new(),
        )
        .unwrap();
        state.start();

        assert_eq!(
            state.receive_ack(
                &seed_2,
                InitJoinAck {
                    address: advertised.clone(),
                    config_check: ClusterConfigCheck::Compatible,
                }
            ),
            vec![ClusterSeedJoinEffect::Join {
                target: advertised.clone()
            }]
        );
        assert!(
            state
                .receive_ack(
                    &seed_1,
                    InitJoinAck {
                        address: seed_1.clone(),
                        config_check: ClusterConfigCheck::Compatible,
                    }
                )
                .is_empty()
        );
        assert!(state.retry().is_empty());
        assert!(!state.receive_welcome(&seed_2));
        let retry_contacts = state.seed_timeout();
        assert_eq!(retry_contacts.len(), 2);
        assert_eq!(state.phase(), &ClusterSeedJoinPhase::Contacting);
        assert_eq!(
            state.receive_ack(
                &seed_2,
                InitJoinAck {
                    address: advertised.clone(),
                    config_check: ClusterConfigCheck::Compatible,
                }
            ),
            vec![ClusterSeedJoinEffect::Join {
                target: advertised.clone()
            }]
        );
        assert!(state.receive_welcome(&advertised));
        assert!(state.retry().is_empty());
    }

    #[test]
    fn unchecked_ack_is_accepted_but_incompatible_ack_is_terminal() {
        let self_address = address("node", 2554);
        let seed = address("seed", 2551);
        let mut unchecked =
            ClusterSeedJoinState::new(self_address.clone(), vec![seed.clone()], Bytes::new())
                .unwrap();
        unchecked.start();
        assert!(matches!(
            unchecked.receive_ack(
                &seed,
                InitJoinAck {
                    address: seed.clone(),
                    config_check: ClusterConfigCheck::Unchecked,
                }
            )[..],
            [ClusterSeedJoinEffect::Join { .. }]
        ));

        let mut incompatible =
            ClusterSeedJoinState::new(self_address, vec![seed.clone()], Bytes::new()).unwrap();
        incompatible.start();
        assert_eq!(
            incompatible.receive_ack(
                &seed,
                InitJoinAck {
                    address: seed.clone(),
                    config_check: ClusterConfigCheck::Incompatible,
                }
            ),
            vec![ClusterSeedJoinEffect::RejectIncompatible {
                target: seed.clone()
            }]
        );
        assert_eq!(
            incompatible.phase(),
            &ClusterSeedJoinPhase::Incompatible { target: seed }
        );
        assert!(incompatible.retry().is_empty());
    }

    #[test]
    fn ignores_unknown_origins_duplicate_nacks_and_repeated_start() {
        let self_address = address("node", 2554);
        let seed = address("seed", 2551);
        let unknown = address("unknown", 2599);
        let mut state =
            ClusterSeedJoinState::new(self_address, vec![seed.clone()], Bytes::new()).unwrap();
        state.start();

        assert!(state.start().is_empty());
        assert!(
            state
                .receive_ack(
                    &unknown,
                    InitJoinAck {
                        address: unknown.clone(),
                        config_check: ClusterConfigCheck::Compatible,
                    }
                )
                .is_empty()
        );
        assert!(
            state
                .receive_nack(
                    &seed,
                    InitJoinNack {
                        address: seed.clone()
                    }
                )
                .is_empty()
        );
        assert!(
            state
                .receive_nack(
                    &seed,
                    InitJoinNack {
                        address: seed.clone(),
                    },
                )
                .is_empty()
        );
    }

    #[test]
    fn rejects_empty_and_duplicate_seed_lists() {
        let self_address = address("node", 2554);
        assert_eq!(
            ClusterSeedJoinState::new(self_address.clone(), Vec::new(), Bytes::new()).unwrap_err(),
            ClusterSeedJoinError::EmptySeedNodes
        );
        assert!(matches!(
            ClusterSeedJoinState::new(
                self_address,
                vec![address("seed", 2551), address("seed", 2551)],
                Bytes::new()
            ),
            Err(ClusterSeedJoinError::DuplicateSeedNode { .. })
        ));
    }

    fn address(system: &str, port: u16) -> Address {
        Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port))
    }
}
