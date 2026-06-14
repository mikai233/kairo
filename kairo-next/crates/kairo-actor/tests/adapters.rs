use std::sync::mpsc;
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorPath, ActorRef, ActorResult, ActorSystem, Context, Props, Signal,
};

struct ExternalMsg {
    reply_to: mpsc::Sender<&'static str>,
}

enum OwnerMsg {
    CreateAdapter(mpsc::Sender<ActorRef<ExternalMsg>>),
    Adapted(ExternalMsg),
    Fail,
    Ping(mpsc::Sender<()>),
}

struct Owner;

impl Actor for Owner {
    type Msg = OwnerMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            OwnerMsg::CreateAdapter(reply_to) => {
                let adapter = ctx.message_adapter(OwnerMsg::Adapted)?;
                reply_to
                    .send(adapter)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            OwnerMsg::Adapted(message) => message
                .reply_to
                .send("adapted")
                .map_err(|error| ActorError::Message(error.to_string()))?,
            OwnerMsg::Fail => return Err(ActorError::Message("boom".to_string())),
            OwnerMsg::Ping(reply_to) => reply_to
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))?,
        }
        Ok(())
    }
}

enum WatcherMsg {
    Watch {
        target: ActorRef<ExternalMsg>,
        reply_to: mpsc::Sender<()>,
    },
}

struct Watcher {
    terminated: mpsc::Sender<ActorPath>,
}

impl Actor for Watcher {
    type Msg = WatcherMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            WatcherMsg::Watch { target, reply_to } => {
                ctx.watch(&target)?;
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }

    fn signal(&mut self, _ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        if let Signal::Terminated(actor) = signal {
            self.terminated
                .send(actor.path().clone())
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }
}

#[test]
fn message_adapter_terminates_with_owner_actor() {
    let system = ActorSystem::builder("adapters").build().unwrap();
    let owner = system.spawn("owner", Props::new(|| Owner)).unwrap();
    let (adapter_tx, adapter_rx) = mpsc::channel();

    owner.tell(OwnerMsg::CreateAdapter(adapter_tx)).unwrap();
    let adapter = adapter_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let adapter_path = adapter.path().clone();

    let (terminated_tx, terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || Watcher {
                terminated: terminated_tx,
            }),
        )
        .unwrap();
    let (watch_tx, watch_rx) = mpsc::channel();
    watcher
        .tell(WatcherMsg::Watch {
            target: adapter.clone(),
            reply_to: watch_tx,
        })
        .unwrap();
    watch_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&owner);

    assert!(owner.wait_for_stop(Duration::from_secs(1)));
    assert!(adapter.is_stopped());
    assert!(adapter.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        adapter_path
    );
}

#[test]
fn message_adapter_terminates_when_owner_restarts() {
    let system = ActorSystem::builder("adapter-restart").build().unwrap();
    let owner = system.spawn("owner", Props::restartable(|| Owner)).unwrap();
    let (adapter_tx, adapter_rx) = mpsc::channel();

    owner.tell(OwnerMsg::CreateAdapter(adapter_tx)).unwrap();
    let adapter = adapter_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let adapter_path = adapter.path().clone();

    let (terminated_tx, terminated_rx) = mpsc::channel();
    let watcher = system
        .spawn(
            "watcher",
            Props::new(move || Watcher {
                terminated: terminated_tx,
            }),
        )
        .unwrap();
    let (watch_tx, watch_rx) = mpsc::channel();
    watcher
        .tell(WatcherMsg::Watch {
            target: adapter.clone(),
            reply_to: watch_tx,
        })
        .unwrap();
    watch_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    owner.tell(OwnerMsg::Fail).unwrap();
    let (ping_tx, ping_rx) = mpsc::channel();
    owner.tell(OwnerMsg::Ping(ping_tx)).unwrap();
    ping_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert!(adapter.is_stopped());
    assert!(adapter.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        adapter_path
    );
}
