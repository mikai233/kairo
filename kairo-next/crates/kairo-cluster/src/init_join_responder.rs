use bytes::Bytes;
use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, Address, Context, Recipient, SendError,
};

use crate::{
    ClusterConfigCheck, ClusterInitJoinRequest, ClusterInitJoinResponse,
    ClusterSeedJoinWireOutbound, InitJoinAck, InitJoinNack, MemberStatus,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterInitJoinLifecycle {
    Uninitialized,
    Initialized { self_status: MemberStatus },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterInitJoinResponderState {
    self_address: Address,
    local_config_digest: Option<Bytes>,
    lifecycle: ClusterInitJoinLifecycle,
}

impl ClusterInitJoinResponderState {
    pub fn new(self_address: Address, local_config_digest: Option<Bytes>) -> Self {
        Self {
            self_address,
            local_config_digest,
            lifecycle: ClusterInitJoinLifecycle::Uninitialized,
        }
    }

    pub fn lifecycle(&self) -> ClusterInitJoinLifecycle {
        self.lifecycle
    }

    pub fn set_lifecycle(&mut self, lifecycle: ClusterInitJoinLifecycle) {
        self.lifecycle = lifecycle;
    }

    pub fn respond(&self, request: &ClusterInitJoinRequest) -> ClusterInitJoinResponse {
        let accepts_join = matches!(
            self.lifecycle,
            ClusterInitJoinLifecycle::Initialized { self_status }
                if !matches!(self_status, MemberStatus::Down | MemberStatus::Exiting)
        );
        if !accepts_join {
            return ClusterInitJoinResponse::Nack(InitJoinNack {
                address: self.self_address.clone(),
            });
        }

        let config_check = match &self.local_config_digest {
            None => ClusterConfigCheck::Unchecked,
            Some(local) if local == &request.message.joining_config_digest => {
                ClusterConfigCheck::Compatible
            }
            Some(_) => ClusterConfigCheck::Incompatible,
        };
        ClusterInitJoinResponse::Ack(InitJoinAck {
            address: self.self_address.clone(),
            config_check,
        })
    }
}

#[derive(Debug, Clone)]
pub enum ClusterInitJoinResponderMsg {
    Request(ClusterInitJoinRequest),
    SetLifecycle(ClusterInitJoinLifecycle),
}

#[derive(Clone)]
pub struct ClusterInitJoinResponderPort {
    responder: ActorRef<ClusterInitJoinResponderMsg>,
}

impl ClusterInitJoinResponderPort {
    pub fn new(responder: ActorRef<ClusterInitJoinResponderMsg>) -> Self {
        Self { responder }
    }
}

impl Recipient<ClusterInitJoinRequest> for ClusterInitJoinResponderPort {
    fn tell(
        &self,
        message: ClusterInitJoinRequest,
    ) -> Result<(), SendError<ClusterInitJoinRequest>> {
        let rejected = message.clone();
        self.responder
            .tell(ClusterInitJoinResponderMsg::Request(message))
            .map_err(|error| SendError::new(rejected, error.reason().to_string()))
    }
}

pub struct ClusterInitJoinResponder {
    state: ClusterInitJoinResponderState,
    outbound: ClusterSeedJoinWireOutbound,
}

impl ClusterInitJoinResponder {
    pub fn new(
        state: ClusterInitJoinResponderState,
        outbound: ClusterSeedJoinWireOutbound,
    ) -> Self {
        Self { state, outbound }
    }

    pub fn state(&self) -> &ClusterInitJoinResponderState {
        &self.state
    }
}

impl Actor for ClusterInitJoinResponder {
    type Msg = ClusterInitJoinResponderMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterInitJoinResponderMsg::Request(request) => self
                .outbound
                .send_init_join_response(&request.origin, self.state.respond(&request))
                .map_err(|error| ActorError::Message(error.to_string())),
            ClusterInitJoinResponderMsg::SetLifecycle(lifecycle) => {
                self.state.set_lifecycle(lifecycle);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use kairo_actor::Props;
    use kairo_remote::{RemoteOutbound, Result as RemoteResult};
    use kairo_serialization::{Registry, RemoteEnvelope};
    use kairo_testkit::{ActorSystemTestKit, await_assert};

    use super::*;
    use crate::{
        ClusterMembershipMsg, ClusterSeedJoinIncompatible, InitJoin, UniqueAddress,
        register_cluster_control_codecs,
    };

    #[derive(Default)]
    struct CollectingOutbound {
        envelopes: Mutex<Vec<RemoteEnvelope>>,
    }

    impl CollectingOutbound {
        fn envelopes(&self) -> Vec<RemoteEnvelope> {
            self.envelopes.lock().unwrap().clone()
        }
    }

    impl RemoteOutbound for CollectingOutbound {
        fn send(&self, envelope: RemoteEnvelope) -> RemoteResult<()> {
            self.envelopes.lock().unwrap().push(envelope);
            Ok(())
        }
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_cluster_control_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn address() -> Address {
        Address::new("kairo", "seed", Some("127.0.0.1".to_string()), Some(2552))
    }

    fn request(digest: &'static [u8]) -> ClusterInitJoinRequest {
        ClusterInitJoinRequest {
            origin: Address::new(
                "kairo",
                "joining",
                Some("127.0.0.1".to_string()),
                Some(2551),
            ),
            message: InitJoin {
                joining_config_digest: Bytes::from_static(digest),
            },
        }
    }

    #[test]
    fn uninitialized_down_and_exiting_nodes_nack_seed_contact() {
        let mut state =
            ClusterInitJoinResponderState::new(address(), Some(Bytes::from_static(b"same")));
        assert!(matches!(
            state.respond(&request(b"same")),
            ClusterInitJoinResponse::Nack(_)
        ));
        for status in [MemberStatus::Down, MemberStatus::Exiting] {
            state.set_lifecycle(ClusterInitJoinLifecycle::Initialized {
                self_status: status,
            });
            assert!(matches!(
                state.respond(&request(b"same")),
                ClusterInitJoinResponse::Nack(_)
            ));
        }
    }

    #[test]
    fn initialized_joinable_nodes_ack_with_explicit_config_result() {
        for status in [
            MemberStatus::Joining,
            MemberStatus::WeaklyUp,
            MemberStatus::Up,
            MemberStatus::Leaving,
        ] {
            let mut state =
                ClusterInitJoinResponderState::new(address(), Some(Bytes::from_static(b"same")));
            state.set_lifecycle(ClusterInitJoinLifecycle::Initialized {
                self_status: status,
            });
            assert!(matches!(
                state.respond(&request(b"same")),
                ClusterInitJoinResponse::Ack(InitJoinAck {
                    config_check: ClusterConfigCheck::Compatible,
                    ..
                })
            ));
            assert!(matches!(
                state.respond(&request(b"different")),
                ClusterInitJoinResponse::Ack(InitJoinAck {
                    config_check: ClusterConfigCheck::Incompatible,
                    ..
                })
            ));
        }
    }

    #[test]
    fn disabled_config_check_acknowledges_as_unchecked() {
        let mut state = ClusterInitJoinResponderState::new(address(), None);
        state.set_lifecycle(ClusterInitJoinLifecycle::Initialized {
            self_status: MemberStatus::Up,
        });
        assert!(matches!(
            state.respond(&request(b"anything")),
            ClusterInitJoinResponse::Ack(InitJoinAck {
                config_check: ClusterConfigCheck::Unchecked,
                ..
            })
        ));
    }

    #[test]
    fn actor_sends_nack_then_compatible_ack_over_seed_wire() {
        let kit = ActorSystemTestKit::new("init-join-responder").unwrap();
        let membership = kit
            .create_probe::<ClusterMembershipMsg>("membership")
            .unwrap();
        let incompatible = kit
            .create_probe::<ClusterSeedJoinIncompatible>("incompatible")
            .unwrap();
        let registry = registry();
        let collected = Arc::new(CollectingOutbound::default());
        let state =
            ClusterInitJoinResponderState::new(address(), Some(Bytes::from_static(b"same")));
        let outbound = ClusterSeedJoinWireOutbound::new(
            UniqueAddress::new(address(), 7),
            Vec::new(),
            registry.clone(),
            collected.clone(),
            membership.actor_ref(),
            incompatible.actor_ref(),
        );
        let responder = kit
            .system()
            .spawn(
                "responder",
                Props::new(move || ClusterInitJoinResponder::new(state.clone(), outbound.clone())),
            )
            .unwrap();
        let port = ClusterInitJoinResponderPort::new(responder.clone());

        port.tell(request(b"same")).unwrap();
        await_assert(Duration::from_secs(1), Duration::from_millis(1), || {
            (collected.envelopes().len() == 1)
                .then_some(())
                .ok_or_else(|| "expected InitJoinNack".to_string())
        })
        .unwrap();
        assert_eq!(
            registry
                .deserialize::<InitJoinNack>(collected.envelopes()[0].message.clone())
                .unwrap()
                .address,
            address()
        );

        responder
            .tell(ClusterInitJoinResponderMsg::SetLifecycle(
                ClusterInitJoinLifecycle::Initialized {
                    self_status: MemberStatus::Up,
                },
            ))
            .unwrap();
        port.tell(request(b"same")).unwrap();
        await_assert(Duration::from_secs(1), Duration::from_millis(1), || {
            (collected.envelopes().len() == 2)
                .then_some(())
                .ok_or_else(|| "expected InitJoinAck".to_string())
        })
        .unwrap();
        assert_eq!(
            registry
                .deserialize::<InitJoinAck>(collected.envelopes()[1].message.clone())
                .unwrap()
                .config_check,
            ClusterConfigCheck::Compatible
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }
}
