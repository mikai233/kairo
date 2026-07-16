use std::env;
use std::hint::black_box;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::{Duration, Instant};

use bytes::Bytes;
use kairo::actor::{Actor, ActorError, ActorResult, ActorSystem, Context, Props};
use kairo::cluster::{Gossip, Member, MemberStatus, UniqueAddress};
use kairo::cluster_sharding::{EntityRef, ShardRegionMsg, ShardingEnvelopeRouter, shard_id_for};
use kairo::remote::{RemoteActorRef, RemoteOutbound};
use kairo::serialization::{
    ActorRefWireData, MessageCodec, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
    SerializationRegistry, SerializerId,
};

const DEFAULT_ITERATIONS: usize = 100_000;
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const USAGE: &str = "usage: cargo run -p kairo-benchmarks --release -- [--help|all|actor-tell|remote-send|gossip-merge|sharding-route]";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkCommand {
    Help,
    All,
    ActorTell,
    RemoteSend,
    GossipMerge,
    ShardingRoute,
}

impl BenchmarkCommand {
    fn parse(value: Option<&str>) -> Result<Self, String> {
        match value.unwrap_or("all") {
            "--help" | "-h" | "help" => Ok(Self::Help),
            "all" => Ok(Self::All),
            "actor-tell" => Ok(Self::ActorTell),
            "remote-send" => Ok(Self::RemoteSend),
            "gossip-merge" => Ok(Self::GossipMerge),
            "sharding-route" => Ok(Self::ShardingRoute),
            other => Err(format!("unknown benchmark scenario `{other}`\n{USAGE}")),
        }
    }
}

fn parse_benchmark_command(args: &[String]) -> Result<BenchmarkCommand, String> {
    if args.len() > 1 {
        return Err(format!(
            "unexpected benchmark argument `{}`\n{USAGE}",
            args[1]
        ));
    }
    BenchmarkCommand::parse(args.first().map(String::as_str))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let command_args: Vec<String> = env::args().skip(1).collect();
    let command = match parse_benchmark_command(&command_args) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    if command == BenchmarkCommand::Help {
        println!("{USAGE}");
        return Ok(());
    }
    let iterations_env = env::var("KAIRO_BENCH_ITERS").ok();
    let iterations = match parse_benchmark_iterations(iterations_env.as_deref()) {
        Ok(iterations) => iterations,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };

    let mut results = Vec::new();
    match command {
        BenchmarkCommand::Help => unreachable!("help returns before running benchmarks"),
        BenchmarkCommand::All => {
            results.push(bench_actor_tell(iterations)?);
            results.push(bench_remote_send(iterations)?);
            results.push(bench_gossip_merge(iterations));
            results.push(bench_sharding_route(iterations)?);
        }
        BenchmarkCommand::ActorTell => results.push(bench_actor_tell(iterations)?),
        BenchmarkCommand::RemoteSend => results.push(bench_remote_send(iterations)?),
        BenchmarkCommand::GossipMerge => results.push(bench_gossip_merge(iterations)),
        BenchmarkCommand::ShardingRoute => results.push(bench_sharding_route(iterations)?),
    }

    for result in results {
        println!(
            "{:<16} {:>10} ops in {:>10.3?} ({:>10.1} ops/s)",
            result.name,
            result.iterations,
            result.elapsed,
            result.ops_per_second()
        );
    }

    Ok(())
}

fn parse_benchmark_iterations(value: Option<&str>) -> Result<usize, String> {
    let Some(value) = value else {
        return Ok(DEFAULT_ITERATIONS);
    };
    let iterations = value.parse::<usize>().map_err(|error| {
        format!("KAIRO_BENCH_ITERS must be a positive integer, got `{value}`: {error}")
    })?;
    if iterations == 0 {
        return Err("KAIRO_BENCH_ITERS must be greater than zero".to_string());
    }
    Ok(iterations)
}

struct BenchResult {
    name: &'static str,
    iterations: usize,
    elapsed: Duration,
}

impl BenchResult {
    fn new(name: &'static str, iterations: usize, elapsed: Duration) -> Self {
        Self {
            name,
            iterations,
            elapsed,
        }
    }

    fn ops_per_second(&self) -> f64 {
        self.iterations as f64 / self.elapsed.as_secs_f64()
    }
}

#[derive(Clone)]
enum CounterMsg {
    Increment,
    Get { reply_to: mpsc::Sender<usize> },
}

struct CounterBenchActor {
    count: usize,
}

impl Actor for CounterBenchActor {
    type Msg = CounterMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            CounterMsg::Increment => self.count += 1,
            CounterMsg::Get { reply_to } => reply_to
                .send(self.count)
                .map_err(|error| ActorError::Message(error.to_string()))?,
        }
        Ok(())
    }
}

fn bench_actor_tell(iterations: usize) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let system = ActorSystem::builder("bench-actor-tell").build()?;
    let actor = system.spawn("counter", Props::new(|| CounterBenchActor { count: 0 }))?;

    let started = Instant::now();
    for _ in 0..iterations {
        actor.tell(CounterMsg::Increment)?;
    }
    let (reply_to, replies) = mpsc::channel();
    actor.tell(CounterMsg::Get { reply_to })?;
    let observed = replies.recv_timeout(WAIT_TIMEOUT)?;
    let elapsed = started.elapsed();
    assert_eq!(observed, iterations);

    system.stop(&actor);
    assert!(actor.wait_for_stop(WAIT_TIMEOUT));
    system.terminate(WAIT_TIMEOUT)?;

    Ok(BenchResult::new("actor-tell", iterations, elapsed))
}

#[derive(Default)]
struct CountingOutbound {
    sent: AtomicUsize,
    last: Mutex<Option<RemoteEnvelope>>,
}

impl CountingOutbound {
    fn sent(&self) -> usize {
        self.sent.load(Ordering::SeqCst)
    }

    fn last(&self) -> Option<RemoteEnvelope> {
        self.last
            .lock()
            .expect("benchmark outbound lock poisoned")
            .clone()
    }
}

impl RemoteOutbound for CountingOutbound {
    fn send(&self, envelope: RemoteEnvelope) -> kairo::remote::Result<()> {
        self.sent.fetch_add(1, Ordering::SeqCst);
        *self.last.lock().expect("benchmark outbound lock poisoned") = Some(envelope);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RemoteBenchMessage {
    value: u8,
}

impl RemoteMessage for RemoteBenchMessage {
    const MANIFEST: &'static str = "kairo.benchmark.RemoteSend";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy)]
struct RemoteBenchMessageCodec;

impl MessageCodec<RemoteBenchMessage> for RemoteBenchMessageCodec {
    fn serializer_id(&self) -> SerializerId {
        17
    }

    fn encode(&self, message: &RemoteBenchMessage) -> kairo::serialization::Result<Bytes> {
        Ok(Bytes::copy_from_slice(&[message.value]))
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo::serialization::Result<RemoteBenchMessage> {
        if version != RemoteBenchMessage::VERSION {
            return Err(SerializationError::Message(format!(
                "unsupported remote benchmark message version {version}"
            )));
        }
        let [value] = payload.as_ref() else {
            return Err(SerializationError::Message(
                "remote benchmark message payload must contain one byte".to_string(),
            ));
        };
        Ok(RemoteBenchMessage { value: *value })
    }
}

fn bench_remote_send(iterations: usize) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let outbound = Arc::new(CountingOutbound::default());
    let mut registry = Registry::new();
    registry.register::<RemoteBenchMessage, _>(RemoteBenchMessageCodec)?;
    let remote_ref = RemoteActorRef::new(
        ActorRefWireData::new("kairo://bench-remote@127.0.0.1:25521/user/target#1")?,
        Arc::new(registry),
        outbound.clone() as Arc<dyn RemoteOutbound>,
    );

    let started = Instant::now();
    for index in 0..iterations {
        remote_ref.tell(black_box(RemoteBenchMessage {
            value: (index & 0xff) as u8,
        }))?;
    }
    let elapsed = started.elapsed();
    assert_eq!(outbound.sent(), iterations);
    let last = outbound
        .last()
        .expect("remote send benchmark must emit an envelope");
    assert_eq!(last.recipient.path(), remote_ref.recipient().path());
    assert_eq!(last.message.serializer_id, 17);
    assert_eq!(last.message.manifest.as_str(), RemoteBenchMessage::MANIFEST);
    assert_eq!(last.message.version, RemoteBenchMessage::VERSION);
    assert_eq!(
        last.message.payload.as_ref(),
        &[((iterations - 1) & 0xff) as u8]
    );

    Ok(BenchResult::new("remote-send", iterations, elapsed))
}

fn bench_gossip_merge(iterations: usize) -> BenchResult {
    let left = gossip_view(0, 32);
    let right = gossip_view(16, 48);

    let started = Instant::now();
    let mut merged = Gossip::new();
    for _ in 0..iterations {
        merged = black_box(&left).merge(black_box(&right));
        black_box(&merged);
    }
    let elapsed = started.elapsed();
    assert!(!merged.members().is_empty());

    BenchResult::new("gossip-merge", iterations, elapsed)
}

fn gossip_view(start: u64, end: u64) -> Gossip {
    Gossip::from_members((start..end).map(|uid| {
        Member::new(
            UniqueAddress::new(
                kairo::actor::Address::new(
                    "kairo",
                    "bench-cluster",
                    Some("127.0.0.1".to_string()),
                    Some(25_520 + uid as u16),
                ),
                uid,
            ),
            vec!["bench".to_string()],
        )
        .with_status(MemberStatus::Up)
        .with_up_number(uid + 1)
    }))
}

struct RouteSink {
    count: usize,
    observed: mpsc::Sender<usize>,
    expected_entity_id: String,
    expected_shard: String,
}

impl Actor for RouteSink {
    type Msg = ShardRegionMsg<String>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        if let ShardRegionMsg::RouteToLocalShard { shard, message, .. } = msg {
            let (entity_id, business_message) = message.into_parts();
            if shard != self.expected_shard
                || entity_id != self.expected_entity_id
                || business_message != "hit"
            {
                return Err(ActorError::Message(
                    "sharding benchmark observed an invalid routed command".to_string(),
                ));
            }
            self.count += 1;
            self.observed
                .send(self.count)
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }
}

fn bench_sharding_route(iterations: usize) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let system = ActorSystem::builder("bench-sharding-route").build()?;
    let (observed_tx, observed_rx) = mpsc::channel();
    let entity_id = "entity-1";
    let expected_shard = shard_id_for(entity_id, 128)?;
    let region = system.spawn(
        "region-sink",
        Props::new(move || RouteSink {
            count: 0,
            observed: observed_tx.clone(),
            expected_entity_id: entity_id.to_string(),
            expected_shard: expected_shard.clone(),
        }),
    )?;
    let router = system.spawn("router", ShardingEnvelopeRouter::props(region.clone(), 128))?;
    let entity_ref = EntityRef::new(entity_id, router.clone());

    let started = Instant::now();
    for _ in 0..iterations {
        entity_ref.tell(black_box("hit".to_string()))?;
    }
    wait_for_count(&observed_rx, iterations)?;
    let elapsed = started.elapsed();

    system.stop(&router);
    system.stop(&region);
    assert!(router.wait_for_stop(WAIT_TIMEOUT));
    assert!(region.wait_for_stop(WAIT_TIMEOUT));
    system.terminate(WAIT_TIMEOUT)?;

    Ok(BenchResult::new("sharding-route", iterations, elapsed))
}

fn wait_for_count(
    observed_rx: &mpsc::Receiver<usize>,
    expected: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    let mut last = 0;
    while last < expected {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(format!("timed out waiting for {expected} routed messages").into());
        };
        last = observed_rx.recv_timeout(remaining)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kairo::serialization::MessageCodec;

    use super::{
        BenchmarkCommand, DEFAULT_ITERATIONS, RemoteBenchMessage, RemoteBenchMessageCodec,
        parse_benchmark_command, parse_benchmark_iterations,
    };

    #[test]
    fn benchmark_command_defaults_to_all() {
        assert_eq!(parse_benchmark_command(&[]), Ok(BenchmarkCommand::All));
    }

    #[test]
    fn benchmark_command_accepts_documented_scenarios() {
        assert_eq!(
            parse_benchmark_command(&["--help".to_string()]),
            Ok(BenchmarkCommand::Help)
        );
        assert_eq!(
            parse_benchmark_command(&["-h".to_string()]),
            Ok(BenchmarkCommand::Help)
        );
        assert_eq!(
            parse_benchmark_command(&["help".to_string()]),
            Ok(BenchmarkCommand::Help)
        );
        assert_eq!(
            parse_benchmark_command(&["all".to_string()]),
            Ok(BenchmarkCommand::All)
        );
        assert_eq!(
            parse_benchmark_command(&["actor-tell".to_string()]),
            Ok(BenchmarkCommand::ActorTell)
        );
        assert_eq!(
            parse_benchmark_command(&["remote-send".to_string()]),
            Ok(BenchmarkCommand::RemoteSend)
        );
        assert_eq!(
            parse_benchmark_command(&["gossip-merge".to_string()]),
            Ok(BenchmarkCommand::GossipMerge)
        );
        assert_eq!(
            parse_benchmark_command(&["sharding-route".to_string()]),
            Ok(BenchmarkCommand::ShardingRoute)
        );
    }

    #[test]
    fn benchmark_command_rejects_unknown_scenario_with_usage() {
        let error = parse_benchmark_command(&["everything".to_string()])
            .expect_err("unknown command must fail");
        assert!(error.contains("unknown benchmark scenario `everything`"));
        assert!(error.contains("--help"));
        assert!(error.contains("actor-tell"));
        assert!(error.contains("sharding-route"));
    }

    #[test]
    fn benchmark_command_rejects_extra_arguments_with_usage() {
        let error = parse_benchmark_command(&["all".to_string(), "extra".to_string()])
            .expect_err("extra arguments must fail");
        assert!(error.contains("unexpected benchmark argument `extra`"));
        assert!(error.contains("--help"));
        assert!(error.contains("actor-tell"));
        assert!(error.contains("sharding-route"));
    }

    #[test]
    fn benchmark_iterations_default_when_unset() {
        assert_eq!(parse_benchmark_iterations(None), Ok(DEFAULT_ITERATIONS));
    }

    #[test]
    fn benchmark_iterations_accept_positive_values() {
        assert_eq!(parse_benchmark_iterations(Some("100")), Ok(100));
    }

    #[test]
    fn benchmark_iterations_reject_zero() {
        let error = parse_benchmark_iterations(Some("0")).expect_err("zero must be rejected");
        assert!(error.contains("greater than zero"));
    }

    #[test]
    fn benchmark_iterations_reject_invalid_values() {
        let error =
            parse_benchmark_iterations(Some("many")).expect_err("non-numeric input must fail");
        assert!(error.contains("positive integer"));
    }

    #[test]
    fn remote_benchmark_codec_validates_its_stable_wire_shape() {
        assert_eq!(
            RemoteBenchMessageCodec
                .decode(Bytes::from_static(&[9]), 1)
                .unwrap(),
            RemoteBenchMessage { value: 9 }
        );
        assert!(
            RemoteBenchMessageCodec
                .decode(Bytes::from_static(&[9]), 2)
                .unwrap_err()
                .to_string()
                .contains("unsupported remote benchmark message version")
        );
        assert!(
            RemoteBenchMessageCodec
                .decode(Bytes::new(), 1)
                .unwrap_err()
                .to_string()
                .contains("must contain one byte")
        );
    }
}
