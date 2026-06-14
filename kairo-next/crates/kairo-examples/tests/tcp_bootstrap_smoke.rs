use std::collections::BTreeMap;
use std::time::Duration;

use kairo::cluster_tools::PubSubStatus;
use kairo::distributed_data::ReplicaId;
use kairo_examples::cluster_tcp;
use kairo_examples::cluster_tools_tcp;
use kairo_examples::ddata_tcp;

#[path = "tcp_bootstrap_smoke/support.rs"]
mod support;

use support::{
    TestResult, assert_replacement_peer_route, assert_three_node_full_mesh_then_shrink,
    assert_two_node_bidirectional_routes, assert_two_node_membership_shrink, lock_tcp_smoke,
};

#[test]
fn cluster_tcp_peer_bootstrap_establishes_bidirectional_routes() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = cluster_tcp::bind_two_nodes()?;
    let result = assert_two_node_bidirectional_routes(&node_a, &node_b);

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    Ok(())
}

#[test]
fn cluster_tcp_peer_bootstrap_delivers_remote_join() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = cluster_tcp::bind_two_nodes()?;
    let result = (|| -> TestResult {
        assert_two_node_bidirectional_routes(&node_a, &node_b)?;
        node_a.send_join_to(&node_b, ["backend"])?;
        let received = node_b.wait_for_join_count(1, Duration::from_secs(2));
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].node, node_a.self_node().clone());
        assert_eq!(received[0].roles, vec!["backend".to_string()]);
        Ok(())
    })();

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    Ok(())
}

#[test]
fn cluster_tcp_peer_bootstrap_removes_route_when_membership_shrinks() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = cluster_tcp::bind_two_nodes()?;
    let result = assert_two_node_membership_shrink(&node_a, &node_b);

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    Ok(())
}

#[test]
fn cluster_tcp_peer_bootstrap_establishes_three_node_full_mesh_and_shrinks() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = cluster_tcp::bind_three_nodes()?;
    let result = assert_three_node_full_mesh_then_shrink(&node_a, &node_b, &node_c);

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));
    let shutdown_c = node_c.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    shutdown_c?;
    Ok(())
}

#[test]
fn cluster_tcp_peer_bootstrap_keeps_remaining_join_route_after_peer_removed() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = cluster_tcp::bind_three_nodes()?;
    let result = (|| -> TestResult {
        node_a.publish_up_members(vec![
            node_a.self_node().clone(),
            node_b.self_node().clone(),
            node_c.self_node().clone(),
        ])?;
        let full_mesh = node_a.wait_for_route_count(2, Duration::from_secs(2))?;
        assert!(
            full_mesh
                .active_targets
                .iter()
                .any(|target| target.node() == node_b.self_node())
        );
        assert!(
            full_mesh
                .active_targets
                .iter()
                .any(|target| target.node() == node_c.self_node())
        );

        node_a.send_join_to(&node_b, ["before-removal"])?;
        let before = node_b.wait_for_join_count(1, Duration::from_secs(2));
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].roles, vec!["before-removal".to_string()]);

        node_a.publish_up_members(vec![node_a.self_node().clone(), node_b.self_node().clone()])?;
        let reduced = node_a.wait_for_route_count(1, Duration::from_secs(2))?;
        assert!(
            reduced
                .active_targets
                .iter()
                .any(|target| target.node() == node_b.self_node())
        );

        node_a.send_join_to(&node_b, ["after-removal"])?;
        let after = node_b.wait_for_join_count(2, Duration::from_secs(2));
        assert_eq!(after.len(), 2);
        assert_eq!(after[1].node, node_a.self_node().clone());
        assert_eq!(after[1].roles, vec!["after-removal".to_string()]);
        Ok(())
    })();

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));
    let shutdown_c = node_c.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    shutdown_c?;
    Ok(())
}

#[test]
fn cluster_tcp_peer_bootstrap_reinstalls_route_for_replacement_peer() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = cluster_tcp::bind_three_nodes()?;
    let result = assert_replacement_peer_route(&node_a, &node_b, &node_c);

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));
    let shutdown_c = node_c.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    shutdown_c?;
    Ok(())
}

#[test]
fn ddata_tcp_peer_bootstrap_establishes_bidirectional_routes() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = ddata_tcp::bind_two_nodes()?;
    let result = assert_two_node_bidirectional_routes(&node_a, &node_b);

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    Ok(())
}

#[test]
fn ddata_tcp_peer_bootstrap_delivers_remote_read_request() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = ddata_tcp::bind_two_nodes()?;
    let result = (|| -> TestResult {
        assert_two_node_bidirectional_routes(&node_a, &node_b)?;
        node_a.send_read_to(&node_b, "example-counter")?;
        let received = node_b.wait_for_request_count(1, Duration::from_secs(2));
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0, ReplicaId::new("ddata-node-a"));
        let read = node_b.decode_read(received[0].1.clone())?;
        assert_eq!(read.key, "example-counter");
        assert_eq!(read.from, Some(ReplicaId::from(node_a.self_node())));
        Ok(())
    })();

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    Ok(())
}

#[test]
fn ddata_tcp_peer_bootstrap_removes_route_when_membership_shrinks() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = ddata_tcp::bind_two_nodes()?;
    let result = assert_two_node_membership_shrink(&node_a, &node_b);

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    Ok(())
}

#[test]
fn ddata_tcp_peer_bootstrap_establishes_three_node_full_mesh_and_shrinks() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = ddata_tcp::bind_three_nodes()?;
    let result = assert_three_node_full_mesh_then_shrink(&node_a, &node_b, &node_c);

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));
    let shutdown_c = node_c.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    shutdown_c?;
    Ok(())
}

#[test]
fn ddata_tcp_peer_bootstrap_keeps_remaining_read_route_after_peer_removed() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = ddata_tcp::bind_three_nodes()?;
    let result = (|| -> TestResult {
        node_a.publish_up_members(vec![
            node_a.self_node().clone(),
            node_b.self_node().clone(),
            node_c.self_node().clone(),
        ])?;
        let full_mesh = node_a.wait_for_route_count(2, Duration::from_secs(2))?;
        assert!(
            full_mesh
                .active_targets
                .iter()
                .any(|target| target.node() == node_b.self_node())
        );
        assert!(
            full_mesh
                .active_targets
                .iter()
                .any(|target| target.node() == node_c.self_node())
        );

        node_a.send_read_to(&node_b, "counter-before-removal")?;
        let before = node_b.wait_for_request_count(1, Duration::from_secs(2));
        assert_eq!(before.len(), 1);
        let before_read = node_b.decode_read(before[0].1.clone())?;
        assert_eq!(before_read.key, "counter-before-removal");
        assert_eq!(before_read.from, Some(ReplicaId::from(node_a.self_node())));

        node_a.publish_up_members(vec![node_a.self_node().clone(), node_b.self_node().clone()])?;
        let reduced = node_a.wait_for_route_count(1, Duration::from_secs(2))?;
        assert!(
            reduced
                .active_targets
                .iter()
                .any(|target| target.node() == node_b.self_node())
        );

        node_a.send_read_to(&node_b, "counter-after-removal")?;
        let after = node_b.wait_for_request_count(2, Duration::from_secs(2));
        assert_eq!(after.len(), 2);
        let after_read = node_b.decode_read(after[1].1.clone())?;
        assert_eq!(after_read.key, "counter-after-removal");
        assert_eq!(after_read.from, Some(ReplicaId::from(node_a.self_node())));
        Ok(())
    })();

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));
    let shutdown_c = node_c.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    shutdown_c?;
    Ok(())
}

#[test]
fn ddata_tcp_peer_bootstrap_reinstalls_route_for_replacement_peer() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = ddata_tcp::bind_three_nodes()?;
    let result = assert_replacement_peer_route(&node_a, &node_b, &node_c);

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));
    let shutdown_c = node_c.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    shutdown_c?;
    Ok(())
}

#[test]
fn cluster_tools_tcp_peer_bootstrap_establishes_bidirectional_routes() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = cluster_tools_tcp::bind_two_nodes()?;
    let result = assert_two_node_bidirectional_routes(&node_a, &node_b);

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    Ok(())
}

#[test]
fn cluster_tools_tcp_peer_bootstrap_delivers_remote_pubsub_publish() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = cluster_tools_tcp::bind_two_nodes()?;
    let result = (|| -> TestResult {
        assert_two_node_bidirectional_routes(&node_a, &node_b)?;
        let message = PubSubStatus {
            from: node_a.self_node().clone(),
            versions: BTreeMap::from([(cluster_tools_tcp::EXAMPLE_PUBSUB_TOPIC.to_string(), 1)]),
            reply: false,
        };
        node_a.send_status_to(&node_b, message.clone())?;
        let received = node_b.wait_for_status_count(1, Duration::from_secs(2));
        assert_eq!(received, vec![message]);
        Ok(())
    })();

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    Ok(())
}

#[test]
fn cluster_tools_tcp_peer_bootstrap_removes_route_when_membership_shrinks() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = cluster_tools_tcp::bind_two_nodes()?;
    let result = assert_two_node_membership_shrink(&node_a, &node_b);

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    Ok(())
}

#[test]
fn cluster_tools_tcp_peer_bootstrap_establishes_three_node_full_mesh_and_shrinks() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = cluster_tools_tcp::bind_three_nodes()?;
    let result = assert_three_node_full_mesh_then_shrink(&node_a, &node_b, &node_c);

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));
    let shutdown_c = node_c.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    shutdown_c?;
    Ok(())
}

#[test]
fn cluster_tools_tcp_peer_bootstrap_reinstalls_route_for_replacement_peer() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = cluster_tools_tcp::bind_three_nodes()?;
    let result = assert_replacement_peer_route(&node_a, &node_b, &node_c);

    let shutdown_a = node_a.shutdown(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));
    let shutdown_c = node_c.shutdown(Duration::from_secs(1));

    result?;
    shutdown_a?;
    shutdown_b?;
    shutdown_c?;
    Ok(())
}
