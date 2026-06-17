use super::*;

struct MultiNodeUnitActor;

impl Actor for MultiNodeUnitActor {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MultiNodeTestEvent(&'static str);

enum MultiNodeEchoMsg {
    Ping {
        value: &'static str,
        reply_to: ActorRef<&'static str>,
    },
}

struct MultiNodeEchoActor;

impl Actor for MultiNodeEchoActor {
    type Msg = MultiNodeEchoMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            MultiNodeEchoMsg::Ping { value, reply_to } => reply_to
                .tell(value)
                .map_err(|error| ActorError::Message(error.reason().to_string())),
        }
    }
}

#[test]
fn multi_node_testkit_builds_named_actor_systems() {
    let kit = MultiNodeTestKit::new(["node-a", "node-b"]).expect("nodes should build");

    assert_eq!(kit.len(), 2);
    assert_eq!(
        kit.node_names().collect::<Vec<_>>(),
        vec!["node-a", "node-b"]
    );
    assert_eq!(kit.system("node-a").unwrap().name(), "node-a");
    assert_eq!(kit.system("node-b").unwrap().name(), "node-b");

    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_spawns_user_actors_on_named_nodes() {
    let kit = MultiNodeTestKit::new(["spawn-node-a", "spawn-node-b"]).expect("nodes should build");
    let probe_a = kit
        .create_probe_on::<&'static str>("spawn-node-a", "probe-a")
        .expect("probe on node a should spawn");
    let probe_b = kit
        .create_probe_on::<&'static str>("spawn-node-b", "probe-b")
        .expect("probe on node b should spawn");
    let actor_a = kit
        .spawn_on("spawn-node-a", "echo-a", Props::new(|| MultiNodeEchoActor))
        .expect("actor on node a should spawn");
    let actor_b = kit
        .spawn_on("spawn-node-b", "echo-b", Props::new(|| MultiNodeEchoActor))
        .expect("actor on node b should spawn");

    assert!(
        actor_a
            .path()
            .as_str()
            .starts_with("kairo://spawn-node-a/user/echo-a#")
    );
    assert!(
        actor_b
            .path()
            .as_str()
            .starts_with("kairo://spawn-node-b/user/echo-b#")
    );

    actor_a
        .tell(MultiNodeEchoMsg::Ping {
            value: "from-a",
            reply_to: probe_a.actor_ref(),
        })
        .expect("actor a should accept ping");
    actor_b
        .tell(MultiNodeEchoMsg::Ping {
            value: "from-b",
            reply_to: probe_b.actor_ref(),
        })
        .expect("actor b should accept ping");

    assert_eq!(
        probe_a.expect_msg(Duration::from_millis(50)).unwrap(),
        "from-a"
    );
    assert_eq!(
        probe_b.expect_msg(Duration::from_millis(50)).unwrap(),
        "from-b"
    );

    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_spawns_system_actors_on_named_nodes() {
    let kit = MultiNodeTestKit::new(["system-spawn-node-a", "system-spawn-node-b"])
        .expect("nodes should build");
    let probe_a = kit
        .create_probe_on::<&'static str>("system-spawn-node-a", "probe-a")
        .expect("probe on node a should spawn");
    let probe_b = kit
        .create_probe_on::<&'static str>("system-spawn-node-b", "probe-b")
        .expect("probe on node b should spawn");
    let actor_a = kit
        .spawn_system_on(
            "system-spawn-node-a",
            "system-echo-a",
            Props::new(|| MultiNodeEchoActor),
        )
        .expect("system actor on node a should spawn");
    let actor_b = kit
        .spawn_system_on(
            "system-spawn-node-b",
            "system-echo-b",
            Props::new(|| MultiNodeEchoActor),
        )
        .expect("system actor on node b should spawn");

    assert!(
        actor_a
            .path()
            .as_str()
            .starts_with("kairo://system-spawn-node-a/system/system-echo-a#")
    );
    assert!(
        actor_b
            .path()
            .as_str()
            .starts_with("kairo://system-spawn-node-b/system/system-echo-b#")
    );

    actor_a
        .tell(MultiNodeEchoMsg::Ping {
            value: "system-a",
            reply_to: probe_a.actor_ref(),
        })
        .expect("system actor a should accept ping");
    actor_b
        .tell(MultiNodeEchoMsg::Ping {
            value: "system-b",
            reply_to: probe_b.actor_ref(),
        })
        .expect("system actor b should accept ping");

    assert_eq!(
        probe_a.expect_msg(Duration::from_millis(50)).unwrap(),
        "system-a"
    );
    assert_eq!(
        probe_b.expect_msg(Duration::from_millis(50)).unwrap(),
        "system-b"
    );

    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_creates_probes_on_named_nodes() {
    let kit = MultiNodeTestKit::new(["probe-node-a", "probe-node-b"]).expect("nodes should build");
    let first = kit
        .create_probe_on::<&'static str>("probe-node-a", "first")
        .expect("first probe should spawn");
    let second = kit
        .create_probe_on::<&'static str>("probe-node-b", "second")
        .expect("second probe should spawn");

    first
        .actor_ref()
        .tell("from-a")
        .expect("first probe should accept messages");
    second
        .actor_ref()
        .tell("from-b")
        .expect("second probe should accept messages");

    assert_eq!(
        first.expect_msg(Duration::from_millis(50)).unwrap(),
        "from-a"
    );
    assert_eq!(
        second.expect_msg(Duration::from_millis(50)).unwrap(),
        "from-b"
    );
    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_creates_event_probes_on_named_nodes() {
    let kit = MultiNodeTestKit::new(["event-node-a", "event-node-b"]).expect("nodes should build");
    let probe_a = kit
        .create_event_probe_on::<MultiNodeTestEvent>("event-node-a", "events-a")
        .expect("event probe on node a should spawn");
    let probe_b = kit
        .create_event_probe_on::<MultiNodeTestEvent>("event-node-b", "events-b")
        .expect("event probe on node b should spawn");

    kit.system("event-node-a")
        .unwrap()
        .event_stream()
        .publish(MultiNodeTestEvent("from-a"));

    assert_eq!(
        probe_a.expect_msg(Duration::from_millis(50)).unwrap(),
        MultiNodeTestEvent("from-a")
    );
    assert_eq!(probe_b.expect_no_msg(Duration::ZERO), Ok(()));

    kit.system("event-node-b")
        .unwrap()
        .event_stream()
        .publish(MultiNodeTestEvent("from-b"));

    assert_eq!(
        probe_b.expect_msg(Duration::from_millis(50)).unwrap(),
        MultiNodeTestEvent("from-b")
    );
    assert_eq!(probe_a.expect_no_msg(Duration::ZERO), Ok(()));

    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_creates_dead_letter_probes_on_named_nodes() {
    let kit = MultiNodeTestKit::new(["dead-letter-node-a", "dead-letter-node-b"])
        .expect("nodes should build");
    let probe_a = kit
        .create_dead_letter_probe_on("dead-letter-node-a", "dead-letters-a")
        .expect("dead-letter probe on node a should spawn");
    let probe_b = kit
        .create_dead_letter_probe_on("dead-letter-node-b", "dead-letters-b")
        .expect("dead-letter probe on node b should spawn");
    let subject = kit
        .system("dead-letter-node-a")
        .unwrap()
        .spawn("subject", Props::new(|| MultiNodeUnitActor))
        .expect("subject should spawn");

    kit.system("dead-letter-node-a").unwrap().stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));
    subject.tell(()).expect_err("send after stop should fail");

    let dead_letter = probe_a
        .expect_msg(Duration::from_millis(50))
        .expect("node-local dead-letter probe should observe stopped send");
    assert_eq!(dead_letter.recipient(), subject.path());
    assert_eq!(dead_letter.reason(), "actor is stopped");
    assert_eq!(probe_b.expect_no_msg(Duration::ZERO), Ok(()));

    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_rejects_empty_duplicate_and_unknown_nodes() {
    let empty =
        MultiNodeTestKit::new(Vec::<String>::new()).expect_err("empty node set should be rejected");
    assert!(matches!(empty, MultiNodeError::EmptyNodeSet));

    let blank = MultiNodeTestKit::new(["  "]).expect_err("blank node name should be rejected");
    assert!(matches!(
        blank,
        MultiNodeError::InvalidNodeName(name) if name == "  "
    ));

    let padded =
        MultiNodeTestKit::new([" node "]).expect_err("padded node name should be rejected");
    assert!(matches!(
        padded,
        MultiNodeError::InvalidNodeName(name) if name == " node "
    ));

    let duplicate =
        MultiNodeTestKit::new(["dup", "dup"]).expect_err("duplicate node names should be rejected");
    assert!(matches!(
        duplicate,
        MultiNodeError::DuplicateNode(name) if name == "dup"
    ));

    let kit = MultiNodeTestKit::new(["known"]).expect("node should build");
    let unknown = kit
        .system("missing")
        .expect_err("unknown node should be explicit");
    assert!(matches!(
        unknown,
        MultiNodeError::UnknownNode(name) if name == "missing"
    ));
    kit.shutdown(Duration::from_secs(1))
        .expect("node should terminate");
}

#[test]
fn multi_node_testkit_wires_manual_time_per_node() {
    let kit = MultiNodeTestKit::with_manual_time(["manual-a", "manual-b"])
        .expect("manual-time nodes should build");
    let probe_a = kit
        .create_probe_on::<&'static str>("manual-a", "probe-a")
        .expect("probe-a should spawn");
    let probe_b = kit
        .create_probe_on::<&'static str>("manual-b", "probe-b")
        .expect("probe-b should spawn");

    kit.system("manual-a").unwrap().schedule_once(
        Duration::from_secs(1),
        probe_a.actor_ref(),
        "tick-a",
    );
    kit.system("manual-b").unwrap().schedule_once(
        Duration::from_secs(2),
        probe_b.actor_ref(),
        "tick-b",
    );

    kit.manual_time("manual-a")
        .unwrap()
        .advance(Duration::from_secs(1));
    assert_eq!(
        probe_a.expect_msg(Duration::from_millis(50)).unwrap(),
        "tick-a"
    );
    assert_eq!(probe_b.expect_no_msg(Duration::ZERO), Ok(()));

    kit.manual_time("manual-b")
        .unwrap()
        .advance(Duration::from_secs(2));
    assert_eq!(
        probe_b.expect_msg(Duration::from_millis(50)).unwrap(),
        "tick-b"
    );
    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_can_advance_all_manual_time_nodes() {
    let kit = MultiNodeTestKit::with_manual_time(["advance-a", "advance-b"])
        .expect("manual-time nodes should build");
    let probe_a = kit
        .create_probe_on::<&'static str>("advance-a", "probe-a")
        .expect("probe-a should spawn");
    let probe_b = kit
        .create_probe_on::<&'static str>("advance-b", "probe-b")
        .expect("probe-b should spawn");

    kit.system("advance-a").unwrap().schedule_once(
        Duration::from_secs(1),
        probe_a.actor_ref(),
        "tick-a",
    );
    kit.system("advance-b").unwrap().schedule_once(
        Duration::from_secs(1),
        probe_b.actor_ref(),
        "tick-b",
    );

    kit.advance_all(Duration::from_millis(999))
        .expect("all nodes have manual time");
    assert_eq!(probe_a.expect_no_msg(Duration::ZERO), Ok(()));
    assert_eq!(probe_b.expect_no_msg(Duration::ZERO), Ok(()));

    kit.advance_all(Duration::from_millis(1))
        .expect("all nodes have manual time");
    assert_eq!(
        probe_a.expect_msg(Duration::from_millis(50)).unwrap(),
        "tick-a"
    );
    assert_eq!(
        probe_b.expect_msg(Duration::from_millis(50)).unwrap(),
        "tick-b"
    );

    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_can_advance_all_manual_time_nodes_to_next_deadline() {
    let kit = MultiNodeTestKit::with_manual_time(["next-a", "next-b"])
        .expect("manual-time nodes should build");
    let probe_a = kit
        .create_probe_on::<&'static str>("next-a", "probe-a")
        .expect("probe-a should spawn");
    let probe_b = kit
        .create_probe_on::<&'static str>("next-b", "probe-b")
        .expect("probe-b should spawn");

    kit.system("next-a").unwrap().schedule_once(
        Duration::from_secs(1),
        probe_a.actor_ref(),
        "tick-a-1",
    );
    kit.system("next-b").unwrap().schedule_once(
        Duration::from_secs(3),
        probe_b.actor_ref(),
        "tick-b-3",
    );

    assert!(
        kit.advance_all_to_next()
            .expect("all nodes have manual time")
    );
    assert_eq!(
        kit.manual_time("next-a").unwrap().now(),
        Duration::from_secs(1)
    );
    assert_eq!(
        kit.manual_time("next-b").unwrap().now(),
        Duration::from_secs(1)
    );
    assert_eq!(
        probe_a.expect_msg(Duration::from_millis(50)).unwrap(),
        "tick-a-1"
    );
    assert_eq!(probe_b.expect_no_msg(Duration::ZERO), Ok(()));

    assert!(
        kit.advance_all_to_next()
            .expect("all nodes have manual time")
    );
    assert_eq!(
        kit.manual_time("next-a").unwrap().now(),
        Duration::from_secs(3)
    );
    assert_eq!(
        kit.manual_time("next-b").unwrap().now(),
        Duration::from_secs(3)
    );
    assert_eq!(
        probe_b.expect_msg(Duration::from_millis(50)).unwrap(),
        "tick-b-3"
    );

    assert!(
        !kit.advance_all_to_next()
            .expect("all nodes have manual time")
    );
    assert_eq!(
        kit.manual_time("next-a").unwrap().now(),
        Duration::from_secs(3)
    );
    assert_eq!(
        kit.manual_time("next-b").unwrap().now(),
        Duration::from_secs(3)
    );

    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_can_advance_all_manual_time_nodes_until_idle() {
    let kit = MultiNodeTestKit::with_manual_time(["idle-a", "idle-b"])
        .expect("manual-time nodes should build");
    let probe_a = kit
        .create_probe_on::<&'static str>("idle-a", "probe-a")
        .expect("probe-a should spawn");
    let probe_b = kit
        .create_probe_on::<&'static str>("idle-b", "probe-b")
        .expect("probe-b should spawn");

    kit.system("idle-a").unwrap().schedule_once(
        Duration::from_secs(1),
        probe_a.actor_ref(),
        "tick-a-1",
    );
    kit.system("idle-a").unwrap().schedule_once(
        Duration::from_secs(3),
        probe_a.actor_ref(),
        "tick-a-3",
    );
    kit.system("idle-b").unwrap().schedule_once(
        Duration::from_secs(2),
        probe_b.actor_ref(),
        "tick-b-2",
    );

    assert!(
        !kit.advance_all_until_idle(1)
            .expect("all nodes have manual time")
    );
    assert_eq!(
        kit.manual_time("idle-a").unwrap().now(),
        Duration::from_secs(1)
    );
    assert_eq!(
        kit.manual_time("idle-b").unwrap().now(),
        Duration::from_secs(1)
    );
    assert_eq!(
        probe_a.expect_msg(Duration::from_millis(50)).unwrap(),
        "tick-a-1"
    );
    assert_eq!(probe_b.expect_no_msg(Duration::ZERO), Ok(()));
    assert_eq!(probe_a.expect_no_msg(Duration::ZERO), Ok(()));

    assert!(
        !kit.advance_all_until_idle(1)
            .expect("all nodes have manual time")
    );
    assert_eq!(
        kit.manual_time("idle-a").unwrap().now(),
        Duration::from_secs(2)
    );
    assert_eq!(
        kit.manual_time("idle-b").unwrap().now(),
        Duration::from_secs(2)
    );
    assert_eq!(
        probe_b.expect_msg(Duration::from_millis(50)).unwrap(),
        "tick-b-2"
    );
    assert_eq!(probe_a.expect_no_msg(Duration::ZERO), Ok(()));

    assert!(
        kit.advance_all_until_idle(1)
            .expect("all nodes have manual time")
    );
    assert_eq!(
        kit.manual_time("idle-a").unwrap().now(),
        Duration::from_secs(3)
    );
    assert_eq!(
        kit.manual_time("idle-b").unwrap().now(),
        Duration::from_secs(3)
    );
    assert_eq!(
        probe_a.expect_msg(Duration::from_millis(50)).unwrap(),
        "tick-a-3"
    );
    assert_eq!(probe_b.expect_no_msg(Duration::ZERO), Ok(()));
    assert_eq!(kit.manual_time("idle-a").unwrap().next_deadline(), None);
    assert_eq!(kit.manual_time("idle-b").unwrap().next_deadline(), None);

    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_reports_manual_time_disabled() {
    let kit = MultiNodeTestKit::new(["plain"]).expect("node should build");

    let error = kit
        .manual_time("plain")
        .expect_err("plain node should not expose manual time");
    assert!(matches!(
        error,
        MultiNodeError::ManualTimeDisabled(name) if name == "plain"
    ));
    let advance_error = kit
        .advance_all(Duration::from_secs(1))
        .expect_err("plain node should not advance manual time");
    assert!(matches!(
        advance_error,
        MultiNodeError::ManualTimeDisabled(name) if name == "plain"
    ));
    let advance_to_next_error = kit
        .advance_all_to_next()
        .expect_err("plain node should not step manual time");
    assert!(matches!(
        advance_to_next_error,
        MultiNodeError::ManualTimeDisabled(name) if name == "plain"
    ));
    let advance_until_idle_error = kit
        .advance_all_until_idle(1)
        .expect_err("plain node should not drain manual time");
    assert!(matches!(
        advance_until_idle_error,
        MultiNodeError::ManualTimeDisabled(name) if name == "plain"
    ));
    kit.shutdown(Duration::from_secs(1))
        .expect("node should terminate");
}

#[test]
fn multi_node_testkit_coordinates_named_barriers() {
    let kit = MultiNodeTestKit::new(["barrier-a", "barrier-b"]).expect("nodes should build");

    let first = kit
        .enter_barrier("started", "barrier-a")
        .expect("first node should enter barrier");
    assert_eq!(first.name(), "started");
    assert!(!first.passed());
    assert_eq!(
        first,
        MultiNodeBarrierStatus::Waiting {
            name: "started".to_string(),
            arrived: BTreeSet::from(["barrier-a".to_string()]),
            remaining: BTreeSet::from(["barrier-b".to_string()]),
        }
    );

    let second = kit
        .enter_barrier("started", "barrier-b")
        .expect("second node should pass barrier");
    assert!(second.passed());
    assert_eq!(
        second,
        MultiNodeBarrierStatus::Passed {
            name: "started".to_string(),
            participants: BTreeSet::from(["barrier-a".to_string(), "barrier-b".to_string()]),
        }
    );

    let next = kit
        .enter_barrier("after-start", "barrier-b")
        .expect("completed barrier should allow next barrier");
    assert_eq!(
        next,
        MultiNodeBarrierStatus::Waiting {
            name: "after-start".to_string(),
            arrived: BTreeSet::from(["barrier-b".to_string()]),
            remaining: BTreeSet::from(["barrier-a".to_string()]),
        }
    );
    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_await_barrier_blocks_until_all_nodes_arrive() {
    let kit = Arc::new(MultiNodeTestKit::new(["await-a", "await-b"]).expect("nodes should build"));
    let waiting_kit = Arc::clone(&kit);
    let waiter = thread::spawn(move || {
        waiting_kit.await_barrier("ready", "await-a", Duration::from_secs(1))
    });

    thread::sleep(Duration::from_millis(20));
    let main_status = kit
        .await_barrier("ready", "await-b", Duration::from_secs(1))
        .expect("second node should pass barrier");
    assert_eq!(
        main_status,
        MultiNodeBarrierStatus::Passed {
            name: "ready".to_string(),
            participants: BTreeSet::from(["await-a".to_string(), "await-b".to_string()]),
        }
    );

    let waiter_status = waiter
        .join()
        .expect("waiting thread should not panic")
        .expect("waiting node should pass after second arrival");
    assert_eq!(waiter_status, main_status);

    let kit = Arc::try_unwrap(kit).expect("test should release shared kit refs");
    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_await_barrier_times_out_with_arrivals() {
    let kit = MultiNodeTestKit::new(["timeout-a", "timeout-b"]).expect("nodes should build");

    let error = kit
        .await_barrier("never", "timeout-a", Duration::from_millis(10))
        .expect_err("barrier should time out when another node never arrives");
    assert!(matches!(
        error,
        MultiNodeError::BarrierTimeout {
            name,
            node,
            timeout,
            arrived,
            remaining,
        } if name == "never"
            && node == "timeout-a"
            && timeout == Duration::from_millis(10)
            && arrived == BTreeSet::from(["timeout-a".to_string()])
            && remaining == BTreeSet::from(["timeout-b".to_string()])
    ));

    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_await_barrier_rejects_wrong_barrier_order() {
    let kit =
        MultiNodeTestKit::new(["await-order-a", "await-order-b"]).expect("nodes should build");

    kit.enter_barrier("phase-one", "await-order-a")
        .expect("first node should enter phase one");

    let wrong = kit
        .await_barrier("phase-two", "await-order-b", Duration::from_millis(10))
        .expect_err("different barrier should be rejected while phase one is active");
    assert!(matches!(
        wrong,
        MultiNodeError::WrongBarrier {
            expected,
            actual,
            node,
        } if expected == "phase-one" && actual == "phase-two" && node == "await-order-b"
    ));

    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_await_barriers_runs_ordered_phases_under_one_timeout() {
    let kit =
        Arc::new(MultiNodeTestKit::new(["sequence-a", "sequence-b"]).expect("nodes should build"));
    let waiting_kit = Arc::clone(&kit);
    let waiter = thread::spawn(move || {
        waiting_kit.await_barriers(
            ["phase-one", "phase-two"],
            "sequence-a",
            Duration::from_secs(1),
        )
    });

    thread::sleep(Duration::from_millis(20));
    let main_statuses = kit
        .await_barriers(
            ["phase-one", "phase-two"],
            "sequence-b",
            Duration::from_secs(1),
        )
        .expect("second node should pass both barriers");
    assert_eq!(
        passed_barrier_names(&main_statuses),
        vec!["phase-one", "phase-two"]
    );

    let waiter_statuses = waiter
        .join()
        .expect("waiting thread should not panic")
        .expect("waiting node should pass both barriers");
    assert_eq!(
        passed_barrier_names(&waiter_statuses),
        vec!["phase-one", "phase-two"]
    );

    let kit = Arc::try_unwrap(kit).expect("test should release shared kit refs");
    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_await_barriers_times_out_later_phase_under_shared_budget() {
    let kit = Arc::new(
        MultiNodeTestKit::new(["sequence-timeout-a", "sequence-timeout-b"])
            .expect("nodes should build"),
    );
    let waiting_kit = Arc::clone(&kit);
    let waiter = thread::spawn(move || {
        waiting_kit.await_barriers(
            ["phase-one", "phase-two"],
            "sequence-timeout-a",
            Duration::from_millis(100),
        )
    });

    let main_statuses = kit
        .await_barriers(["phase-one"], "sequence-timeout-b", Duration::from_secs(1))
        .expect("second node should complete only the first barrier");
    assert_eq!(passed_barrier_names(&main_statuses), vec!["phase-one"]);

    let error = waiter
        .join()
        .expect("waiting thread should not panic")
        .expect_err("second phase should inherit the remaining shared timeout");
    assert!(matches!(
        error,
        MultiNodeError::BarrierTimeout {
            name,
            node,
            timeout,
            arrived,
            remaining,
        } if name == "phase-two"
            && node == "sequence-timeout-a"
            && timeout <= Duration::from_millis(100)
            && arrived == BTreeSet::from(["sequence-timeout-a".to_string()])
            && remaining == BTreeSet::from(["sequence-timeout-b".to_string()])
    ));

    let kit = Arc::try_unwrap(kit).expect("test should release shared kit refs");
    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

#[test]
fn multi_node_testkit_rejects_wrong_barrier_order_and_duplicate_arrivals() {
    let kit = MultiNodeTestKit::new(["order-a", "order-b"]).expect("nodes should build");

    kit.enter_barrier("phase-one", "order-a")
        .expect("first node should enter phase one");

    let wrong = kit
        .enter_barrier("phase-two", "order-b")
        .expect_err("different barrier should be rejected while phase one is active");
    assert!(matches!(
        wrong,
        MultiNodeError::WrongBarrier {
            expected,
            actual,
            node,
        } if expected == "phase-one" && actual == "phase-two" && node == "order-b"
    ));

    let duplicate = kit
        .enter_barrier("phase-one", "order-a")
        .expect_err("same node should not enter one barrier twice");
    assert!(matches!(
        duplicate,
        MultiNodeError::DuplicateBarrierArrival { name, node }
            if name == "phase-one" && node == "order-a"
    ));

    let unknown = kit
        .enter_barrier("phase-one", "missing")
        .expect_err("unknown node should be rejected before barrier state changes");
    assert!(matches!(
        unknown,
        MultiNodeError::UnknownNode(name) if name == "missing"
    ));

    kit.shutdown(Duration::from_secs(1))
        .expect("nodes should terminate");
}

fn passed_barrier_names(statuses: &[MultiNodeBarrierStatus]) -> Vec<&str> {
    statuses
        .iter()
        .map(|status| {
            assert!(status.passed());
            status.name()
        })
        .collect()
}
