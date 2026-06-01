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
fn multi_node_testkit_reports_manual_time_disabled() {
    let kit = MultiNodeTestKit::new(["plain"]).expect("node should build");

    let error = kit
        .manual_time("plain")
        .expect_err("plain node should not expose manual time");
    assert!(matches!(
        error,
        MultiNodeError::ManualTimeDisabled(name) if name == "plain"
    ));
    kit.shutdown(Duration::from_secs(1))
        .expect("node should terminate");
}
