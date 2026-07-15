use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use kairo_actor::{Actor, ActorResult, ActorSystem, Context, Props};
use kairo_remote::{
    RemoteAssociationAddress, RemoteSettings, TcpRemoteActorRuntime,
    register_remote_protocol_codecs,
};
use kairo_serialization::{
    MessageCodec, Registry, RemoteMessage, SerializationError, SerializationRegistry,
};

const CHILD_ROLE_ENV: &str = "KAIRO_REMOTE_PROCESS_TEST_RECEIVER";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(10);
const PROCESS_PING_MANIFEST: &str = "kairo.remote.test.ProcessPing";
const PROCESS_PING_SERIALIZER_ID: u32 = 19_901;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcessPing {
    value: u8,
    tag: u8,
}

impl RemoteMessage for ProcessPing {
    const MANIFEST: &'static str = PROCESS_PING_MANIFEST;
    const VERSION: u16 = 2;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcessPingV1 {
    value: u8,
}

impl RemoteMessage for ProcessPingV1 {
    const MANIFEST: &'static str = PROCESS_PING_MANIFEST;
    const VERSION: u16 = 1;
}

struct ProcessPingCodec;

impl MessageCodec<ProcessPing> for ProcessPingCodec {
    fn serializer_id(&self) -> u32 {
        PROCESS_PING_SERIALIZER_ID
    }

    fn encode(&self, message: &ProcessPing) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value, message.tag]))
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<ProcessPing> {
        match (version, payload.as_ref()) {
            (1, [value]) => Ok(ProcessPing {
                value: *value,
                tag: 0,
            }),
            (2, [value, tag]) => Ok(ProcessPing {
                value: *value,
                tag: *tag,
            }),
            _ => Err(SerializationError::Message(format!(
                "invalid process ping version {version} or payload length {}",
                payload.len()
            ))),
        }
    }
}

struct ProcessPingV1Codec;

impl MessageCodec<ProcessPingV1> for ProcessPingV1Codec {
    fn serializer_id(&self) -> u32 {
        PROCESS_PING_SERIALIZER_ID
    }

    fn encode(&self, message: &ProcessPingV1) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value]))
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<ProcessPingV1> {
        match (version, payload.as_ref()) {
            (1, [value]) => Ok(ProcessPingV1 { value: *value }),
            (2, [value, _tag]) => Ok(ProcessPingV1 { value: *value }),
            _ => Err(SerializationError::Message(format!(
                "invalid process ping v1 version {version} or payload length {}",
                payload.len()
            ))),
        }
    }
}

struct Target {
    received: mpsc::Sender<ProcessPing>,
}

impl Actor for Target {
    type Msg = ProcessPing;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.received
            .send(msg)
            .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
    }
}

struct V1Target {
    received: mpsc::Sender<ProcessPingV1>,
}

impl Actor for V1Target {
    type Msg = ProcessPingV1;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.received
            .send(msg)
            .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
    }
}

fn current_registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    registry
        .register::<ProcessPing, _>(ProcessPingCodec)
        .unwrap();
    register_remote_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

fn v1_registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    registry
        .register::<ProcessPingV1, _>(ProcessPingV1Codec)
        .unwrap();
    register_remote_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

#[test]
fn process_receiver_child() {
    if std::env::var_os(CHILD_ROLE_ENV).is_none() {
        return;
    }

    let system = ActorSystem::builder("receiver").build().unwrap();
    let (received_tx, received_rx) = mpsc::channel();
    let target = system
        .spawn(
            "target",
            Props::new(move || Target {
                received: received_tx.clone(),
            }),
        )
        .unwrap();
    let mut builder = TcpRemoteActorRuntime::builder(
        system.clone(),
        current_registry(),
        RemoteSettings::new("127.0.0.1", 0),
        11,
    );
    builder.register::<ProcessPing>().unwrap();
    let runtime = builder.bind().unwrap();
    let port = runtime.settings().canonical_port;
    let remote_path = target.path().as_str().replacen(
        "kairo://receiver",
        &format!("kairo://receiver@127.0.0.1:{port}"),
        1,
    );

    println!("KAIRO_PROCESS_READY {port} {remote_path}");
    std::io::stdout().flush().unwrap();

    let received = received_rx
        .recv_timeout(PROCESS_TIMEOUT)
        .expect("receiver child timed out waiting for remote message");
    println!(
        "KAIRO_PROCESS_DELIVERED {} {}",
        received.value, received.tag
    );
    std::io::stdout().flush().unwrap();

    runtime.shutdown().unwrap();
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn process_v1_receiver_child() {
    if std::env::var_os(CHILD_ROLE_ENV).is_none() {
        return;
    }

    let system = ActorSystem::builder("v1-receiver").build().unwrap();
    let (received_tx, received_rx) = mpsc::channel();
    let target = system
        .spawn(
            "target",
            Props::new(move || V1Target {
                received: received_tx.clone(),
            }),
        )
        .unwrap();
    let mut builder = TcpRemoteActorRuntime::builder(
        system.clone(),
        v1_registry(),
        RemoteSettings::new("127.0.0.1", 0),
        12,
    );
    builder.register::<ProcessPingV1>().unwrap();
    let runtime = builder.bind().unwrap();
    let port = runtime.settings().canonical_port;
    let remote_path = target.path().as_str().replacen(
        "kairo://v1-receiver",
        &format!("kairo://v1-receiver@127.0.0.1:{port}"),
        1,
    );

    println!("KAIRO_PROCESS_READY {port} {remote_path}");
    std::io::stdout().flush().unwrap();

    let received = received_rx
        .recv_timeout(PROCESS_TIMEOUT)
        .expect("v1 receiver child timed out waiting for remote message");
    println!("KAIRO_PROCESS_DELIVERED {}", received.value);
    std::io::stdout().flush().unwrap();

    runtime.shutdown().unwrap();
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn composed_runtime_delivers_typed_message_across_os_processes() {
    let (mut child, lines, port, remote_path) = spawn_ready_receiver("process_receiver_child");

    let system = ActorSystem::builder("sender").build().unwrap();
    let mut builder = TcpRemoteActorRuntime::builder(
        system.clone(),
        current_registry(),
        RemoteSettings::new("127.0.0.1", 0),
        22,
    );
    builder.register::<ProcessPing>().unwrap();
    let runtime = builder.bind().unwrap();
    let receiver_address =
        RemoteAssociationAddress::new("kairo", "receiver", "127.0.0.1", Some(port)).unwrap();
    let registration = runtime.dial(receiver_address).unwrap();
    let remote_target = runtime.resolve::<ProcessPing>(remote_path).unwrap();

    remote_target
        .tell(ProcessPing { value: 73, tag: 9 })
        .unwrap();
    let delivered = wait_for_marker(&lines, "KAIRO_PROCESS_DELIVERED", PROCESS_TIMEOUT);
    assert_eq!(delivered, "73 9");

    registration.close_owned_route("process remoting test done");
    runtime.shutdown().unwrap();
    system.terminate(Duration::from_secs(1)).unwrap();
    child.wait_success(PROCESS_TIMEOUT);
}

#[test]
fn composed_runtime_decodes_v1_message_into_v2_type_across_os_processes() {
    let (mut child, lines, port, remote_path) = spawn_ready_receiver("process_receiver_child");

    let system = ActorSystem::builder("v1-sender").build().unwrap();
    let mut builder = TcpRemoteActorRuntime::builder(
        system.clone(),
        v1_registry(),
        RemoteSettings::new("127.0.0.1", 0),
        33,
    );
    builder.register::<ProcessPingV1>().unwrap();
    let runtime = builder.bind().unwrap();
    let receiver_address =
        RemoteAssociationAddress::new("kairo", "receiver", "127.0.0.1", Some(port)).unwrap();
    let registration = runtime.dial(receiver_address).unwrap();
    let remote_target = runtime.resolve::<ProcessPingV1>(remote_path).unwrap();

    remote_target.tell(ProcessPingV1 { value: 42 }).unwrap();
    let delivered = wait_for_marker(&lines, "KAIRO_PROCESS_DELIVERED", PROCESS_TIMEOUT);
    assert_eq!(delivered, "42 0");

    registration.close_owned_route("rolling process remoting test done");
    runtime.shutdown().unwrap();
    system.terminate(Duration::from_secs(1)).unwrap();
    child.wait_success(PROCESS_TIMEOUT);
}

#[test]
fn composed_runtime_decodes_v2_message_into_forward_compatible_v1_type_across_processes() {
    let (mut child, lines, port, remote_path) = spawn_ready_receiver("process_v1_receiver_child");

    let system = ActorSystem::builder("v2-sender").build().unwrap();
    let mut builder = TcpRemoteActorRuntime::builder(
        system.clone(),
        current_registry(),
        RemoteSettings::new("127.0.0.1", 0),
        44,
    );
    builder.register::<ProcessPing>().unwrap();
    let runtime = builder.bind().unwrap();
    let receiver_address =
        RemoteAssociationAddress::new("kairo", "v1-receiver", "127.0.0.1", Some(port)).unwrap();
    let registration = runtime.dial(receiver_address).unwrap();
    let remote_target = runtime.resolve::<ProcessPing>(remote_path).unwrap();

    remote_target
        .tell(ProcessPing { value: 61, tag: 7 })
        .unwrap();
    let delivered = wait_for_marker(&lines, "KAIRO_PROCESS_DELIVERED", PROCESS_TIMEOUT);
    assert_eq!(delivered, "61");

    registration.close_owned_route("forward-compatible process remoting test done");
    runtime.shutdown().unwrap();
    system.terminate(Duration::from_secs(1)).unwrap();
    child.wait_success(PROCESS_TIMEOUT);
}

fn spawn_ready_receiver(receiver_test: &str) -> (ChildGuard, mpsc::Receiver<String>, u16, String) {
    let mut child = ChildGuard::spawn_receiver(receiver_test);
    let lines = child.take_stdout_lines();
    let ready = wait_for_marker(&lines, "KAIRO_PROCESS_READY", PROCESS_TIMEOUT);
    let mut ready_parts = ready.split_whitespace();
    let port = ready_parts
        .next()
        .expect("receiver child omitted port")
        .parse::<u16>()
        .expect("receiver child emitted invalid port");
    let remote_path = ready_parts
        .next()
        .expect("receiver child omitted target path")
        .to_string();
    assert!(
        ready_parts.next().is_none(),
        "unexpected receiver ready data"
    );
    (child, lines, port, remote_path)
}

fn wait_for_marker(lines: &mpsc::Receiver<String>, marker: &str, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let line = lines
            .recv_timeout(remaining)
            .unwrap_or_else(|error| panic!("timed out waiting for {marker}: {error}"));
        if let Some(index) = line.find(marker) {
            return line[index + marker.len()..].trim().to_string();
        }
    }
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn spawn_receiver(receiver_test: &str) -> Self {
        let child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(receiver_test)
            .arg("--nocapture")
            .env(CHILD_ROLE_ENV, "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to spawn receiver child process");
        Self { child }
    }

    fn take_stdout_lines(&mut self) -> mpsc::Receiver<String> {
        let stdout = self.child.stdout.take().expect("receiver stdout missing");
        let (lines_tx, lines_rx) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(line) => {
                        if lines_tx.send(line).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        lines_rx
    }

    fn wait_success(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            match self.child.try_wait().expect("receiver child wait failed") {
                Some(status) => {
                    assert!(status.success(), "receiver child failed with {status}");
                    return;
                }
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
                None => panic!("receiver child did not exit within {timeout:?}"),
            }
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
