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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcessPing {
    value: u8,
}

impl RemoteMessage for ProcessPing {
    const MANIFEST: &'static str = "kairo.remote.test.ProcessPing";
    const VERSION: u16 = 1;
}

struct ProcessPingCodec;

impl MessageCodec<ProcessPing> for ProcessPingCodec {
    fn serializer_id(&self) -> u32 {
        19_901
    }

    fn encode(&self, message: &ProcessPing) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value]))
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<ProcessPing> {
        if version != ProcessPing::VERSION || payload.len() != 1 {
            return Err(SerializationError::Message(format!(
                "invalid process ping version {version} or payload length {}",
                payload.len()
            )));
        }
        Ok(ProcessPing { value: payload[0] })
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

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    registry
        .register::<ProcessPing, _>(ProcessPingCodec)
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
        registry(),
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
    println!("KAIRO_PROCESS_DELIVERED {}", received.value);
    std::io::stdout().flush().unwrap();

    runtime.shutdown().unwrap();
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn composed_runtime_delivers_typed_message_across_os_processes() {
    let mut child = ChildGuard::spawn_receiver();
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

    let system = ActorSystem::builder("sender").build().unwrap();
    let mut builder = TcpRemoteActorRuntime::builder(
        system.clone(),
        registry(),
        RemoteSettings::new("127.0.0.1", 0),
        22,
    );
    builder.register::<ProcessPing>().unwrap();
    let runtime = builder.bind().unwrap();
    let receiver_address =
        RemoteAssociationAddress::new("kairo", "receiver", "127.0.0.1", Some(port)).unwrap();
    let registration = runtime.dial(receiver_address).unwrap();
    let remote_target = runtime.resolve::<ProcessPing>(remote_path).unwrap();

    remote_target.tell(ProcessPing { value: 73 }).unwrap();
    let delivered = wait_for_marker(&lines, "KAIRO_PROCESS_DELIVERED", PROCESS_TIMEOUT);
    assert_eq!(delivered, "73");

    registration.close_owned_route("process remoting test done");
    runtime.shutdown().unwrap();
    system.terminate(Duration::from_secs(1)).unwrap();
    child.wait_success(PROCESS_TIMEOUT);
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
    fn spawn_receiver() -> Self {
        let child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("process_receiver_child")
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
