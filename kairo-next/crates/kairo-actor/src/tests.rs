use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::*;

mod adapters;
mod asks;
mod backoff_supervisor;
mod coordinated_shutdown;
mod event_stream;
mod extensions;
mod local_core;
mod receive_timeout;
mod receptionist;
mod scheduler;
mod stash;
mod supervision;
mod tasks;
mod timers;
mod tree_lifecycle;
mod watch;

#[test]
fn actor_crate_does_not_expose_async_actor_api() -> Result<(), Box<dyn std::error::Error>> {
    let crate_src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let forbidden_declarations = [
        "pub trait AsyncActor",
        "trait AsyncActor",
        "pub struct AsyncActor",
        "struct AsyncActor",
        "pub enum AsyncActor",
        "enum AsyncActor",
        "pub type AsyncActor",
        "type AsyncActor",
        "pub use AsyncActor",
        "pub use crate::AsyncActor",
    ];

    let mut files = Vec::new();
    collect_rs_files(&crate_src, &mut files)?;

    for file in files {
        let source = std::fs::read_to_string(&file)?.replace("\r\n", "\n");
        for declaration in forbidden_declarations {
            assert!(
                !source.contains(declaration),
                "{} must not expose `{declaration}`; async work should re-enter actors through mailbox messages",
                file.display()
            );
        }
    }

    Ok(())
}

fn collect_rs_files(
    directory: &std::path::Path,
    files: &mut Vec<std::path::PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = path.file_name().and_then(|name| name.to_str());
        if path.is_dir() {
            if file_name == Some("tests") {
                continue;
            }
            collect_rs_files(&path, files)?;
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs")
            && file_name != Some("tests.rs")
        {
            files.push(path);
        }
    }

    Ok(())
}

enum CounterMsg {
    Increment,
    Get(mpsc::Sender<usize>),
    Stop,
}

struct Counter {
    value: usize,
}

struct ChannelProbe<T> {
    observed: mpsc::Sender<T>,
}

impl<T: Send + 'static> Actor for ChannelProbe<T> {
    type Msg = T;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.observed
            .send(msg)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

impl Actor for Counter {
    type Msg = CounterMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            CounterMsg::Increment => self.value += 1,
            CounterMsg::Get(reply_to) => {
                reply_to
                    .send(self.value)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            CounterMsg::Stop => ctx.stop(ctx.myself())?,
        }
        Ok(())
    }
}

struct StopProbe {
    stopped: mpsc::Sender<()>,
}

impl Actor for StopProbe {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.stopped
            .send(())
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

struct BlockingStart {
    release: mpsc::Receiver<()>,
    received: Arc<AtomicU64>,
}

impl Actor for BlockingStart {
    type Msg = ();

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.release
            .recv()
            .map_err(|error| ActorError::Message(error.to_string()))?;
        Ok(())
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        self.received.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct Noop;

impl Actor for Noop {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }
}
