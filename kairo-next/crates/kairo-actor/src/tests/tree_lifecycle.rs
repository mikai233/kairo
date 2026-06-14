use super::*;

enum ParentMsg {
    SpawnNamed(mpsc::Sender<ActorPath>),
    SpawnAnonymous(mpsc::Sender<(ActorPath, ActorPath)>),
    SystemName(mpsc::Sender<String>),
    ParentPath(mpsc::Sender<ActorPath>),
    Children(mpsc::Sender<Vec<ActorPath>>),
    ChildNamed {
        name: String,
        reply_to: mpsc::Sender<Option<ActorPath>>,
    },
}

struct Parent;

impl Actor for Parent {
    type Msg = ParentMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ParentMsg::SpawnNamed(reply_to) => {
                let child = ctx.spawn("child", Props::new(|| Noop))?;
                reply_to
                    .send(child.path().clone())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ParentMsg::SpawnAnonymous(reply_to) => {
                let first = ctx.spawn_anonymous(Props::new(|| Noop))?;
                let second = ctx.spawn_anonymous(Props::new(|| Noop))?;
                reply_to
                    .send((first.path().clone(), second.path().clone()))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ParentMsg::SystemName(reply_to) => {
                reply_to
                    .send(ctx.system().name().to_string())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ParentMsg::ParentPath(reply_to) => {
                reply_to
                    .send(ctx.parent().path().clone())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ParentMsg::Children(reply_to) => {
                let paths = ctx
                    .children()
                    .into_iter()
                    .map(|child| child.path().clone())
                    .collect();
                reply_to
                    .send(paths)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ParentMsg::ChildNamed { name, reply_to } => {
                let path = ctx.child(&name).map(|child| child.path().clone());
                reply_to
                    .send(path)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[test]
fn context_spawn_places_children_under_parent_path() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system.spawn("parent", Props::new(|| Parent)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    parent.tell(ParentMsg::SpawnNamed(reply_tx)).unwrap();

    let child_path = reply_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(
        child_path
            .as_str()
            .starts_with(&format!("{}/child#", parent.path()))
    );
}

#[test]
fn context_spawn_anonymous_creates_unique_child_names() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system.spawn("parent", Props::new(|| Parent)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    parent.tell(ParentMsg::SpawnAnonymous(reply_tx)).unwrap();

    let (first, second) = reply_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_ne!(first, second);
    assert!(
        first
            .as_str()
            .starts_with(&format!("{}/$anon-", parent.path()))
    );
    assert!(
        second
            .as_str()
            .starts_with(&format!("{}/$anon-", parent.path()))
    );
}

#[test]
fn context_exposes_actor_system_handle() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system.spawn("parent", Props::new(|| Parent)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    parent.tell(ParentMsg::SystemName(reply_tx)).unwrap();

    assert_eq!(
        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "test"
    );
}

#[test]
fn context_parent_points_to_user_root_for_top_level_actor() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system.spawn("parent", Props::new(|| Parent)).unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    parent.tell(ParentMsg::ParentPath(reply_tx)).unwrap();

    assert_eq!(
        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .as_str(),
        "kairo://test/user"
    );
}

#[test]
fn actor_path_exposes_structured_address_name_parent_and_uid() {
    let path = ActorPath::new("kairo://test/user/parent#7/child#9");

    assert_eq!(path.address().protocol(), "kairo");
    assert_eq!(path.address().system(), "test");
    assert_eq!(path.name(), Some("child"));
    assert_eq!(path.uid(), Some(9));
    assert_eq!(
        path.parent().unwrap().as_str(),
        "kairo://test/user/parent#7"
    );
}

#[test]
fn context_children_and_child_lookup_reflect_live_children() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system.spawn("parent", Props::new(|| Parent)).unwrap();
    let (spawn_tx, spawn_rx) = mpsc::channel();
    let (children_tx, children_rx) = mpsc::channel();
    let (child_tx, child_rx) = mpsc::channel();
    let (missing_tx, missing_rx) = mpsc::channel();

    parent.tell(ParentMsg::SpawnNamed(spawn_tx)).unwrap();
    let child_path = spawn_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent.tell(ParentMsg::Children(children_tx)).unwrap();
    let children = children_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent
        .tell(ParentMsg::ChildNamed {
            name: "child".to_string(),
            reply_to: child_tx,
        })
        .unwrap();
    parent
        .tell(ParentMsg::ChildNamed {
            name: "missing".to_string(),
            reply_to: missing_tx,
        })
        .unwrap();

    assert!(children.contains(&child_path));
    assert_eq!(
        child_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Some(child_path)
    );
    assert_eq!(
        missing_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        None
    );
}

enum ChildStopMsg {
    SpawnChild {
        stopped: mpsc::Sender<()>,
        reply_to: mpsc::Sender<ActorPath>,
    },
    SpawnBlockingChild {
        entered_stop: mpsc::Sender<()>,
        release_stop: mpsc::Receiver<()>,
        reply_to: mpsc::Sender<ActorPath>,
    },
    StopChild {
        reply_to: mpsc::Sender<()>,
    },
    SpawnReplacement {
        reply_to: mpsc::Sender<Result<ActorPath, String>>,
    },
    Fail,
    StopOther {
        other: ActorRef<()>,
        reply_to: mpsc::Sender<String>,
    },
    ChildPath(mpsc::Sender<Option<ActorPath>>),
    Ping(mpsc::Sender<&'static str>),
}

struct ChildStoppingParent {
    child: Option<ActorRef<()>>,
}

impl Actor for ChildStoppingParent {
    type Msg = ChildStopMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ChildStopMsg::SpawnChild { stopped, reply_to } => {
                let child = ctx.spawn("child", Props::new(move || StopProbe { stopped }))?;
                reply_to
                    .send(child.path().clone())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                self.child = Some(child);
            }
            ChildStopMsg::SpawnBlockingChild {
                entered_stop,
                release_stop,
                reply_to,
            } => {
                let child = ctx.spawn(
                    "child",
                    Props::new(move || BlockingStopChild {
                        entered_stop,
                        release_stop,
                    }),
                )?;
                reply_to
                    .send(child.path().clone())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                self.child = Some(child);
            }
            ChildStopMsg::StopChild { reply_to } => {
                if let Some(child) = self.child.clone() {
                    ctx.stop(child)?;
                }
                reply_to
                    .send(())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ChildStopMsg::SpawnReplacement { reply_to } => {
                let result = ctx
                    .spawn("child", Props::new(|| Noop))
                    .map(|child| child.path().clone())
                    .map_err(|error| error.to_string());
                reply_to
                    .send(result)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ChildStopMsg::Fail => {
                return Err(ActorError::Message("restart parent".to_string()));
            }
            ChildStopMsg::StopOther { other, reply_to } => {
                let error = ctx.stop(other).unwrap_err();
                reply_to
                    .send(error.to_string())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ChildStopMsg::ChildPath(reply_to) => {
                let path = ctx.child("child").map(|child| child.path().clone());
                reply_to
                    .send(path)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            ChildStopMsg::Ping(reply_to) => {
                reply_to
                    .send("alive")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

struct BlockingStopChild {
    entered_stop: mpsc::Sender<()>,
    release_stop: mpsc::Receiver<()>,
}

impl Actor for BlockingStopChild {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.entered_stop
            .send(())
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.release_stop
            .recv_timeout(Duration::from_secs(1))
            .map_err(|error| ActorError::Message(error.to_string()))?;
        Ok(())
    }
}

#[test]
fn restart_supervision_waits_for_stopping_children_before_processing_messages() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn(
            "parent",
            Props::restartable(|| ChildStoppingParent { child: None })
                .with_supervisor(SupervisorStrategy::Restart),
        )
        .unwrap();
    let (entered_stop_tx, entered_stop_rx) = mpsc::channel();
    let (release_stop_tx, release_stop_rx) = mpsc::channel();
    let (spawn_tx, spawn_rx) = mpsc::channel();
    let (replacement_tx, replacement_rx) = mpsc::channel();
    let (ping_tx, ping_rx) = mpsc::channel();

    parent
        .tell(ChildStopMsg::SpawnBlockingChild {
            entered_stop: entered_stop_tx,
            release_stop: release_stop_rx,
            reply_to: spawn_tx,
        })
        .unwrap();
    let child_path = spawn_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let child = system.resolve_local::<()>(child_path.as_str()).unwrap();

    parent.tell(ChildStopMsg::Fail).unwrap();
    entered_stop_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    parent
        .tell(ChildStopMsg::SpawnReplacement {
            reply_to: replacement_tx,
        })
        .unwrap();
    assert!(
        replacement_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "parent must not process user messages while restart waits for child termination"
    );

    release_stop_tx.send(()).unwrap();
    assert!(child.wait_for_stop(Duration::from_secs(1)));
    let replacement = replacement_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert!(
        replacement
            .as_str()
            .starts_with(&format!("{}/child#", parent.path()))
    );
    assert_ne!(replacement, child_path);

    parent.tell(ChildStopMsg::Ping(ping_tx)).unwrap();
    assert_eq!(
        ping_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "alive"
    );
}

#[test]
fn context_stop_can_stop_direct_child_without_stopping_parent() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn("parent", Props::new(|| ChildStoppingParent { child: None }))
        .unwrap();
    let (child_stopped_tx, child_stopped_rx) = mpsc::channel();
    let (spawn_tx, spawn_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let (child_lookup_tx, child_lookup_rx) = mpsc::channel();
    let (ping_tx, ping_rx) = mpsc::channel();

    parent
        .tell(ChildStopMsg::SpawnChild {
            stopped: child_stopped_tx,
            reply_to: spawn_tx,
        })
        .unwrap();
    let child_path = spawn_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    parent
        .tell(ChildStopMsg::StopChild { reply_to: stop_tx })
        .unwrap();
    stop_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    child_stopped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    parent.tell(ChildStopMsg::Ping(ping_tx)).unwrap();
    parent
        .tell(ChildStopMsg::ChildPath(child_lookup_tx))
        .unwrap();

    assert_eq!(
        ping_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "alive"
    );
    assert_eq!(
        child_lookup_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        None
    );
    assert!(
        child_path
            .as_str()
            .starts_with(&format!("{}/child#", parent.path()))
    );
}

#[test]
fn stopping_child_name_is_reserved_until_termination_completes() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn("parent", Props::new(|| ChildStoppingParent { child: None }))
        .unwrap();
    let (entered_stop_tx, entered_stop_rx) = mpsc::channel();
    let (release_stop_tx, release_stop_rx) = mpsc::channel();
    let (spawn_tx, spawn_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let (blocked_replacement_tx, blocked_replacement_rx) = mpsc::channel();
    let (replacement_tx, replacement_rx) = mpsc::channel();

    parent
        .tell(ChildStopMsg::SpawnBlockingChild {
            entered_stop: entered_stop_tx,
            release_stop: release_stop_rx,
            reply_to: spawn_tx,
        })
        .unwrap();
    let child_path = spawn_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let child = system.resolve_local::<()>(child_path.as_str()).unwrap();

    parent
        .tell(ChildStopMsg::StopChild { reply_to: stop_tx })
        .unwrap();
    stop_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    entered_stop_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    parent
        .tell(ChildStopMsg::SpawnReplacement {
            reply_to: blocked_replacement_tx,
        })
        .unwrap();
    assert_eq!(
        blocked_replacement_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .expect_err("stopping child must still reserve its name"),
        "actor `child` already exists"
    );

    release_stop_tx.send(()).unwrap();
    assert!(child.wait_for_stop(Duration::from_secs(1)));
    assert!(system.resolve_local::<()>(child_path.as_str()).is_none());

    parent
        .tell(ChildStopMsg::SpawnReplacement {
            reply_to: replacement_tx,
        })
        .unwrap();
    let replacement = replacement_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert!(
        replacement
            .as_str()
            .starts_with(&format!("{}/child#", parent.path()))
    );
    assert_ne!(replacement, child_path);
}

#[test]
fn context_stop_rejects_actor_that_is_not_self_or_direct_child() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn("parent", Props::new(|| ChildStoppingParent { child: None }))
        .unwrap();
    let other = system.spawn("other", Props::new(|| Noop)).unwrap();
    let (error_tx, error_rx) = mpsc::channel();
    let (ping_tx, ping_rx) = mpsc::channel();

    parent
        .tell(ChildStopMsg::StopOther {
            other,
            reply_to: error_tx,
        })
        .unwrap();
    parent.tell(ChildStopMsg::Ping(ping_tx)).unwrap();

    let error = error_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(error.contains("is not self or a direct child"));
    assert_eq!(
        ping_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "alive"
    );
}

enum StartupChildMsg {
    SpawnFailing {
        reply_to: mpsc::Sender<ActorRef<()>>,
    },
    SpawnHealthy {
        reply_to: mpsc::Sender<Result<ActorPath, String>>,
    },
    ChildPath(mpsc::Sender<Option<ActorPath>>),
    Ping(mpsc::Sender<&'static str>),
}

struct StartupChildParent;

impl Actor for StartupChildParent {
    type Msg = StartupChildMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            StartupChildMsg::SpawnFailing { reply_to } => {
                let child = ctx.spawn("child", Props::new(|| StartupFailingChild))?;
                reply_to
                    .send(child)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            StartupChildMsg::SpawnHealthy { reply_to } => {
                let result = ctx
                    .spawn("child", Props::new(|| Noop))
                    .map(|child| child.path().clone())
                    .map_err(|error| error.to_string());
                reply_to
                    .send(result)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            StartupChildMsg::ChildPath(reply_to) => {
                let path = ctx.child("child").map(|child| child.path().clone());
                reply_to
                    .send(path)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            StartupChildMsg::Ping(reply_to) => {
                reply_to
                    .send("alive")
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

struct StartupFailingChild;

impl Actor for StartupFailingChild {
    type Msg = ();

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        Err(ActorError::Message("startup failed".to_string()))
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }
}

#[test]
fn child_startup_failure_cleans_parent_registry_and_releases_name() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn("parent", Props::new(|| StartupChildParent))
        .unwrap();
    let (failing_tx, failing_rx) = mpsc::channel();
    let (child_lookup_tx, child_lookup_rx) = mpsc::channel();
    let (healthy_tx, healthy_rx) = mpsc::channel();
    let (ping_tx, ping_rx) = mpsc::channel();

    parent
        .tell(StartupChildMsg::SpawnFailing {
            reply_to: failing_tx,
        })
        .unwrap();
    let failed_child = failing_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(failed_child.wait_for_stop(Duration::from_secs(1)));

    parent
        .tell(StartupChildMsg::ChildPath(child_lookup_tx))
        .unwrap();
    assert_eq!(
        child_lookup_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        None
    );

    parent
        .tell(StartupChildMsg::SpawnHealthy {
            reply_to: healthy_tx,
        })
        .unwrap();
    let healthy_child = healthy_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert!(
        healthy_child
            .as_str()
            .starts_with(&format!("{}/child#", parent.path()))
    );

    parent.tell(StartupChildMsg::Ping(ping_tx)).unwrap();
    assert_eq!(
        ping_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "alive"
    );
}

#[derive(Debug, PartialEq, Eq)]
enum StopEvent {
    Child,
    Parent,
}

struct LifecycleChild {
    events: mpsc::Sender<StopEvent>,
}

impl Actor for LifecycleChild {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.events
            .send(StopEvent::Child)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

struct LifecycleParent {
    ready: mpsc::Sender<()>,
    events: mpsc::Sender<StopEvent>,
}

impl Actor for LifecycleParent {
    type Msg = ();

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let events = self.events.clone();
        ctx.spawn(
            "child",
            Props::new(move || LifecycleChild {
                events: events.clone(),
            }),
        )?;
        self.ready
            .send(())
            .map_err(|error| ActorError::Message(error.to_string()))
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.events
            .send(StopEvent::Parent)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

#[test]
fn parent_stop_waits_for_children_before_stopped_hook() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (events_tx, events_rx) = mpsc::channel();
    let parent = system
        .spawn(
            "parent",
            Props::new(move || LifecycleParent {
                ready: ready_tx,
                events: events_tx,
            }),
        )
        .unwrap();
    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.stop(&parent);

    let first = events_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let second = events_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(first, StopEvent::Child);
    assert_eq!(second, StopEvent::Parent);
    assert!(parent.wait_for_stop(Duration::from_secs(1)));
}

#[test]
fn parent_stop_does_not_process_user_messages_while_waiting_for_children() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn("parent", Props::new(|| ChildStoppingParent { child: None }))
        .unwrap();
    let (entered_stop_tx, entered_stop_rx) = mpsc::channel();
    let (release_stop_tx, release_stop_rx) = mpsc::channel();
    let (spawn_tx, spawn_rx) = mpsc::channel();
    let (ping_tx, ping_rx) = mpsc::channel();

    parent
        .tell(ChildStopMsg::SpawnBlockingChild {
            entered_stop: entered_stop_tx,
            release_stop: release_stop_rx,
            reply_to: spawn_tx,
        })
        .unwrap();
    let child_path = spawn_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let child = system.resolve_local::<()>(child_path.as_str()).unwrap();

    system.stop(&parent);
    entered_stop_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    assert!(
        parent.tell(ChildStopMsg::Ping(ping_tx)).is_err(),
        "stopping parent should reject new user messages"
    );
    assert!(
        ping_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "parent must not process user messages while stop waits for child termination"
    );

    release_stop_tx.send(()).unwrap();
    assert!(child.wait_for_stop(Duration::from_secs(1)));
    assert!(parent.wait_for_stop(Duration::from_secs(1)));
}

#[test]
fn actor_system_terminate_waits_for_descendant_children_before_terminated() {
    let system = ActorSystem::builder("test").build().unwrap();
    let parent = system
        .spawn("parent", Props::new(|| ChildStoppingParent { child: None }))
        .unwrap();
    let (entered_stop_tx, entered_stop_rx) = mpsc::channel();
    let (release_stop_tx, release_stop_rx) = mpsc::channel();
    let (spawn_tx, spawn_rx) = mpsc::channel();
    let (terminated_tx, terminated_rx) = mpsc::channel();

    parent
        .tell(ChildStopMsg::SpawnBlockingChild {
            entered_stop: entered_stop_tx,
            release_stop: release_stop_rx,
            reply_to: spawn_tx,
        })
        .unwrap();
    let child_path = spawn_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let child = system.resolve_local::<()>(child_path.as_str()).unwrap();

    let terminating_system = system.clone();
    let terminate_thread = std::thread::spawn(move || {
        terminated_tx
            .send(terminating_system.terminate(Duration::from_secs(1)))
            .unwrap();
    });
    entered_stop_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "system termination must wait for descendants before completing"
    );
    assert!(system.is_terminating());
    assert!(!system.is_terminated());

    release_stop_tx.send(()).unwrap();
    terminated_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    terminate_thread.join().unwrap();
    assert!(child.wait_for_stop(Duration::from_secs(1)));
    assert!(parent.wait_for_stop(Duration::from_secs(1)));
    assert!(system.is_terminated());
}

#[derive(Debug)]
struct PostStopSpawnResults {
    named: Result<(), String>,
    anonymous: Result<(), String>,
}

struct PostStopSpawningActor {
    results: mpsc::Sender<PostStopSpawnResults>,
}

impl Actor for PostStopSpawningActor {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let named = ctx
            .spawn("late-child", Props::new(|| Noop))
            .map(|_| ())
            .map_err(|error| error.to_string());
        let anonymous = ctx
            .spawn_anonymous(Props::new(|| Noop))
            .map(|_| ())
            .map_err(|error| error.to_string());
        self.results
            .send(PostStopSpawnResults { named, anonymous })
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

#[test]
fn post_stop_rejects_late_child_spawns() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (results_tx, results_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "post-stop-spawner",
            Props::new(move || PostStopSpawningActor {
                results: results_tx,
            }),
        )
        .unwrap();

    system.stop(&actor);

    let results = results_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(
        results.named.expect_err("named spawn should be rejected"),
        format!("actor `{}` is stopping", actor.path())
    );
    assert_eq!(
        results
            .anonymous
            .expect_err("anonymous spawn should be rejected"),
        format!("actor `{}` is stopping", actor.path())
    );
    assert!(actor.wait_for_stop(Duration::from_secs(1)));
}

struct SignalProbe {
    signals: mpsc::Sender<Signal>,
}

impl Actor for SignalProbe {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn signal(&mut self, _ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        self.signals
            .send(signal)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

#[test]
fn post_stop_signal_is_delivered_during_termination() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (signals_tx, signals_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "signal-probe",
            Props::new(move || SignalProbe {
                signals: signals_tx,
            }),
        )
        .unwrap();

    system.stop(&actor);

    assert_eq!(
        signals_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Signal::PostStop
    );
    assert!(actor.wait_for_stop(Duration::from_secs(1)));
}
