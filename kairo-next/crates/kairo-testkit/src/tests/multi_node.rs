use super::*;

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
fn multi_node_testkit_rejects_empty_duplicate_and_unknown_nodes() {
    let empty =
        MultiNodeTestKit::new(Vec::<String>::new()).expect_err("empty node set should be rejected");
    assert!(matches!(empty, MultiNodeError::EmptyNodeSet));

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
