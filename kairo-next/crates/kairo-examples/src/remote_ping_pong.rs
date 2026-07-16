use std::error::Error;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use kairo::actor::{Actor, ActorError, ActorResult, ActorSystem, Context, Props};
use kairo::remote::{
    RemoteActorRef, RemoteActorRefProvider, RemoteAssociationAddress,
    RemoteAssociationRouteRegistration, RemoteSettings, TcpRemoteActorSystem,
    register_remote_protocol_codecs,
};
use kairo::serialization::{
    ActorRefWireData, Registry, RemoteMessage, SerializationError, SerializerId,
};

const REMOTE_PING_PONG_SERIALIZER_ID: SerializerId = 12_001;
const ROUTE_WAIT_TIMEOUT: Duration = Duration::from_secs(1);
const ROUTE_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemotePingPongMsg {
    Ping {
        value: u64,
        reply_to: ActorRefWireData,
    },
    Pong {
        value: u64,
    },
}

impl RemoteMessage for RemotePingPongMsg {
    const MANIFEST: &'static str = "kairo.example.RemotePingPong";
    const VERSION: u16 = 1;
}

fn encode_remote_ping_pong(message: &RemotePingPongMsg) -> kairo::serialization::Result<Bytes> {
    match message {
        RemotePingPongMsg::Ping { value, reply_to } => {
            let reply_to = reply_to.path().as_bytes();
            let reply_to_len: u16 = reply_to.len().try_into().map_err(|_| {
                SerializationError::Message(
                    "remote ping-pong reply path exceeds u16 length".to_string(),
                )
            })?;
            let mut bytes = Vec::with_capacity(1 + 8 + 2 + reply_to.len());
            bytes.push(1);
            bytes.extend_from_slice(&value.to_be_bytes());
            bytes.extend_from_slice(&reply_to_len.to_be_bytes());
            bytes.extend_from_slice(reply_to);
            Ok(Bytes::from(bytes))
        }
        RemotePingPongMsg::Pong { value } => {
            let mut bytes = Vec::with_capacity(1 + 8);
            bytes.push(2);
            bytes.extend_from_slice(&value.to_be_bytes());
            Ok(Bytes::from(bytes))
        }
    }
}

fn decode_remote_ping_pong(
    payload: Bytes,
    version: u16,
) -> kairo::serialization::Result<RemotePingPongMsg> {
    if version != RemotePingPongMsg::VERSION {
        return Err(SerializationError::Message(format!(
            "unsupported RemotePingPongMsg version {version}"
        )));
    }

    let payload = payload.as_ref();
    let Some((&tag, rest)) = payload.split_first() else {
        return Err(SerializationError::Message(
            "remote ping-pong payload is empty".to_string(),
        ));
    };

    match tag {
        1 => decode_ping(rest),
        2 => decode_pong(rest),
        other => Err(SerializationError::Message(format!(
            "unknown remote ping-pong tag {other}"
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePingPongObservation {
    pub ping_value: u64,
    pub pong_value: u64,
    pub responder_path: String,
    pub reply_path: String,
}

struct PingResponder {
    provider: RemoteActorRefProvider,
    observed: mpsc::Sender<u64>,
}

impl Actor for PingResponder {
    type Msg = RemotePingPongMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        let RemotePingPongMsg::Ping { value, reply_to } = msg else {
            return Err(ActorError::Message(
                "ping responder received a pong".to_string(),
            ));
        };

        self.observed
            .send(value)
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.provider
            .resolve_wire::<RemotePingPongMsg>(reply_to)
            .map_err(|error| ActorError::Message(error.to_string()))?
            .tell(RemotePingPongMsg::Pong { value: value + 1 })
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

struct PongCollector {
    observed: mpsc::Sender<u64>,
}

impl Actor for PongCollector {
    type Msg = RemotePingPongMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        let RemotePingPongMsg::Pong { value } = msg else {
            return Err(ActorError::Message(
                "pong collector received a ping".to_string(),
            ));
        };

        self.observed
            .send(value)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

pub struct RemotePingPongExample {
    sender_system: ActorSystem,
    receiver_system: ActorSystem,
    sender_remote: TcpRemoteActorSystem<RemotePingPongMsg>,
    receiver_remote: TcpRemoteActorSystem<RemotePingPongMsg>,
    _registration: RemoteAssociationRouteRegistration,
    remote_responder: RemoteActorRef<RemotePingPongMsg>,
    reply_to: ActorRefWireData,
    ping_rx: mpsc::Receiver<u64>,
    pong_rx: mpsc::Receiver<u64>,
}

impl RemotePingPongExample {
    pub fn start(system_prefix: &str) -> Result<Self, Box<dyn Error>> {
        let sender_system = ActorSystem::builder(format!("{system_prefix}-sender")).build()?;
        let receiver_system = ActorSystem::builder(format!("{system_prefix}-receiver")).build()?;
        let registry = registry()?;
        let sender_remote = TcpRemoteActorSystem::<RemotePingPongMsg>::bind(
            sender_system.clone(),
            Arc::clone(&registry),
            RemoteSettings::new("127.0.0.1", 0),
            11,
        )?;
        let receiver_remote = TcpRemoteActorSystem::<RemotePingPongMsg>::bind(
            receiver_system.clone(),
            registry,
            RemoteSettings::new("127.0.0.1", 0),
            22,
        )?;
        let (ping_tx, ping_rx) = mpsc::channel();
        let (pong_tx, pong_rx) = mpsc::channel();
        let receiver_provider = receiver_remote.provider().clone();
        let responder = receiver_system.spawn(
            "ping-responder",
            Props::new(move || PingResponder {
                provider: receiver_provider.clone(),
                observed: ping_tx.clone(),
            }),
        )?;
        let reply_actor = sender_system.spawn(
            "pong-collector",
            Props::new(move || PongCollector {
                observed: pong_tx.clone(),
            }),
        )?;
        let receiver_address = RemoteAssociationAddress::new(
            "kairo",
            receiver_system.name(),
            receiver_remote.settings().canonical_hostname.clone(),
            Some(receiver_remote.settings().canonical_port),
        )?;
        let registration = sender_remote.dial(receiver_address)?;
        wait_for_route_count(|| receiver_remote.association_cache().route_count(), 1)?;
        let responder_path = receiver_remote
            .provider()
            .local_actor_ref_to_wire_data(&responder)?;
        let reply_to = sender_remote
            .provider()
            .local_actor_ref_to_wire_data(&reply_actor)?;
        let remote_responder =
            sender_remote.resolve::<RemotePingPongMsg>(responder_path.path().to_string())?;

        Ok(Self {
            sender_system,
            receiver_system,
            sender_remote,
            receiver_remote,
            _registration: registration,
            remote_responder,
            reply_to,
            ping_rx,
            pong_rx,
        })
    }

    pub fn ping(
        &self,
        value: u64,
        timeout: Duration,
    ) -> Result<RemotePingPongObservation, Box<dyn Error>> {
        self.remote_responder.tell(RemotePingPongMsg::Ping {
            value,
            reply_to: self.reply_to.clone(),
        })?;
        let ping_value = self.ping_rx.recv_timeout(timeout)?;
        let pong_value = self.pong_rx.recv_timeout(timeout)?;

        Ok(RemotePingPongObservation {
            ping_value,
            pong_value,
            responder_path: self.remote_responder.path().as_str().to_string(),
            reply_path: self.reply_to.path().to_string(),
        })
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), Box<dyn Error>> {
        let sender_system = self.sender_system.clone();
        let receiver_system = self.receiver_system.clone();
        self.sender_remote.shutdown_with_timeout(timeout)?;
        self.receiver_remote.shutdown_with_timeout(timeout)?;
        sender_system.terminate(timeout)?;
        receiver_system.terminate(timeout)?;
        Ok(())
    }
}

pub fn run_remote_ping_pong(
    system_prefix: &str,
    value: u64,
) -> Result<RemotePingPongObservation, Box<dyn Error>> {
    let example = RemotePingPongExample::start(system_prefix)?;
    let observation = example.ping(value, Duration::from_secs(2))?;
    example.shutdown(Duration::from_secs(1))?;
    Ok(observation)
}

fn registry() -> Result<Arc<Registry>, Box<dyn Error>> {
    let mut registry = Registry::new();
    registry.register_with::<RemotePingPongMsg, _, _>(
        REMOTE_PING_PONG_SERIALIZER_ID,
        encode_remote_ping_pong,
        decode_remote_ping_pong,
    )?;
    register_remote_protocol_codecs(&mut registry)?;
    Ok(Arc::new(registry))
}

fn decode_ping(payload: &[u8]) -> kairo::serialization::Result<RemotePingPongMsg> {
    if payload.len() < 10 {
        return Err(SerializationError::Message(
            "remote ping payload is truncated".to_string(),
        ));
    }
    let value = read_u64(&payload[..8])?;
    let reply_to_len = u16::from_be_bytes([payload[8], payload[9]]) as usize;
    let reply_to = payload.get(10..).ok_or_else(|| {
        SerializationError::Message("remote ping reply path is missing".to_string())
    })?;
    if reply_to.len() != reply_to_len {
        return Err(SerializationError::Message(format!(
            "remote ping reply path length mismatch: header {reply_to_len}, payload {}",
            reply_to.len()
        )));
    }
    let reply_to = String::from_utf8(reply_to.to_vec())
        .map_err(|error| SerializationError::Message(error.to_string()))?;

    Ok(RemotePingPongMsg::Ping {
        value,
        reply_to: ActorRefWireData::new(reply_to)?,
    })
}

fn decode_pong(payload: &[u8]) -> kairo::serialization::Result<RemotePingPongMsg> {
    if payload.len() != 8 {
        return Err(SerializationError::Message(format!(
            "remote pong payload length mismatch: expected 8, got {}",
            payload.len()
        )));
    }
    Ok(RemotePingPongMsg::Pong {
        value: read_u64(payload)?,
    })
}

fn read_u64(bytes: &[u8]) -> kairo::serialization::Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| SerializationError::Message("expected eight u64 bytes".to_string()))?;
    Ok(u64::from_be_bytes(bytes))
}

fn wait_for_route_count(
    route_count: impl Fn() -> usize,
    expected: usize,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + ROUTE_WAIT_TIMEOUT;
    loop {
        if route_count() == expected {
            return Ok(());
        }
        let Some(remaining) = remaining_until(deadline) else {
            return Err(format!("timed out waiting for {expected} remote route(s)").into());
        };
        thread::sleep(ROUTE_WAIT_POLL_INTERVAL.min(remaining));
    }
}

fn remaining_until(deadline: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    (!remaining.is_zero()).then_some(remaining)
}
