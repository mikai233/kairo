use std::collections::BTreeMap;
use std::net::TcpListener;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use kairo::actor::Address;
use kairo::cluster::UniqueAddress;
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
fn cluster_tcp_peer_bootstrap_shutdown_stops_connector_after_live_route() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = cluster_tcp::bind_two_nodes()?;
    if let Err(error) = assert_two_node_bidirectional_routes(&node_a, &node_b) {
        let shutdown_a = node_a.shutdown(Duration::from_secs(1));
        let shutdown_b = node_b.shutdown(Duration::from_secs(1));
        shutdown_a?;
        shutdown_b?;
        return Err(error);
    }

    let observation_a = node_a.shutdown_with_observation(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));

    let observation = observation_a?;
    assert_eq!(observation.route_count_before_shutdown, 1);
    assert!(observation.connector_stopped);
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
fn cluster_tcp_peer_bootstrap_clears_pending_reconnect_when_peer_leaves() -> TestResult {
    let _lock = lock_tcp_smoke();
    let node = cluster_tcp::ClusterTcpExampleNode::bind(
        "cluster-pending-node-a",
        1,
        11,
        "cluster-pending-node-a-peers",
    )?;
    let result = (|| -> TestResult {
        let missing = missing_peer("cluster-pending-missing", 2);
        node.publish_up_members(vec![node.self_node().clone(), missing.clone()])?;
        wait_for_cluster_pending_reconnect(&node, &missing, Duration::from_secs(2))?;

        node.publish_up_members(vec![node.self_node().clone()])?;
        wait_for_cluster_no_routes_or_pending(&node, Duration::from_secs(2))?;
        Ok(())
    })();

    let shutdown = node.shutdown(Duration::from_secs(1));

    result?;
    shutdown?;
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

        let removed_error = node_a
            .send_join_to(&node_c, ["after-removal-removed"])
            .expect_err("removed cluster peer route should reject sends");
        assert!(
            removed_error
                .to_string()
                .contains("no remote association route"),
            "unexpected removed cluster peer send error: {removed_error:?}"
        );
        assert!(
            node_c
                .wait_for_join_count(1, Duration::from_millis(100))
                .is_empty()
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
fn cluster_tcp_peer_bootstrap_delivers_join_to_replacement_peer() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = cluster_tcp::bind_three_nodes()?;
    let result = (|| -> TestResult {
        assert_replacement_peer_route(&node_a, &node_b, &node_c)?;

        node_a.send_join_to(&node_c, ["replacement"])?;
        let received = node_c.wait_for_join_count(1, Duration::from_secs(2));
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].node, node_a.self_node().clone());
        assert_eq!(received[0].roles, vec!["replacement".to_string()]);

        assert!(
            node_b
                .wait_for_join_count(1, Duration::from_millis(100))
                .is_empty()
        );
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
fn ddata_tcp_peer_bootstrap_shutdown_stops_connector_after_live_route() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = ddata_tcp::bind_two_nodes()?;
    if let Err(error) = assert_two_node_bidirectional_routes(&node_a, &node_b) {
        let shutdown_a = node_a.shutdown(Duration::from_secs(1));
        let shutdown_b = node_b.shutdown(Duration::from_secs(1));
        shutdown_a?;
        shutdown_b?;
        return Err(error);
    }

    let observation_a = node_a.shutdown_with_observation(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));

    let observation = observation_a?;
    assert_eq!(observation.route_count_before_shutdown, 1);
    assert!(observation.connector_stopped);
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
        assert_eq!(received[0].0, ReplicaId::from(node_a.self_node()));
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
fn ddata_tcp_peer_bootstrap_clears_pending_reconnect_when_peer_leaves() -> TestResult {
    let _lock = lock_tcp_smoke();
    let node = ddata_tcp::DDataTcpExampleNode::bind(
        "ddata-pending-node-a",
        1,
        11,
        "ddata-pending-node-a-peers",
    )?;
    let result = (|| -> TestResult {
        let missing = missing_peer("ddata-pending-missing", 2);
        node.publish_up_members(vec![node.self_node().clone(), missing.clone()])?;
        wait_for_ddata_pending_reconnect(&node, &missing, Duration::from_secs(2))?;

        node.publish_up_members(vec![node.self_node().clone()])?;
        wait_for_ddata_no_routes_or_pending(&node, Duration::from_secs(2))?;
        Ok(())
    })();

    let shutdown = node.shutdown(Duration::from_secs(1));

    result?;
    shutdown?;
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

        let removed_error = node_a
            .send_read_to(&node_c, "counter-after-removal-removed")
            .expect_err("removed distributed-data peer route should reject sends");
        assert!(
            removed_error
                .to_string()
                .contains("no remote association route"),
            "unexpected removed distributed-data peer send error: {removed_error:?}"
        );
        assert!(
            node_c
                .wait_for_request_count(1, Duration::from_millis(100))
                .is_empty()
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
fn ddata_tcp_peer_bootstrap_delivers_read_to_replacement_peer() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = ddata_tcp::bind_three_nodes()?;
    let result = (|| -> TestResult {
        assert_replacement_peer_route(&node_a, &node_b, &node_c)?;

        node_a.send_read_to(&node_c, "counter-after-replacement")?;
        let received = node_c.wait_for_request_count(1, Duration::from_secs(2));
        assert_eq!(received.len(), 1);
        let read = node_c.decode_read(received[0].1.clone())?;
        assert_eq!(read.key, "counter-after-replacement");
        assert_eq!(read.from, Some(ReplicaId::from(node_a.self_node())));

        assert!(
            node_b
                .wait_for_request_count(1, Duration::from_millis(100))
                .is_empty()
        );
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
fn cluster_tools_tcp_peer_bootstrap_shutdown_stops_connector_after_live_route() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = cluster_tools_tcp::bind_two_nodes()?;
    if let Err(error) = assert_two_node_bidirectional_routes(&node_a, &node_b) {
        let shutdown_a = node_a.shutdown(Duration::from_secs(1));
        let shutdown_b = node_b.shutdown(Duration::from_secs(1));
        shutdown_a?;
        shutdown_b?;
        return Err(error);
    }

    let observation_a = node_a.shutdown_with_observation(Duration::from_secs(1));
    let shutdown_b = node_b.shutdown(Duration::from_secs(1));

    let observation = observation_a?;
    assert_eq!(observation.route_count_before_shutdown, 1);
    assert!(observation.connector_stopped);
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
fn cluster_tools_tcp_peer_bootstrap_delivers_remote_pubsub_path_messages() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = cluster_tools_tcp::bind_two_nodes()?;
    let result = (|| -> TestResult {
        assert_two_node_bidirectional_routes(&node_a, &node_b)?;
        let one = PubSubStatus {
            from: node_a.self_node().clone(),
            versions: BTreeMap::from([(cluster_tools_tcp::EXAMPLE_PUBSUB_TOPIC.to_string(), 101)]),
            reply: false,
        };
        node_a.send_status_path_to(&node_b, one.clone())?;
        let one_received = node_b.wait_for_path_status_count(1, Duration::from_secs(2));
        assert_eq!(one_received, vec![one.clone()]);
        assert!(
            node_b
                .wait_for_status_count(1, Duration::from_millis(100))
                .is_empty()
        );

        let all = PubSubStatus {
            from: node_a.self_node().clone(),
            versions: BTreeMap::from([(cluster_tools_tcp::EXAMPLE_PUBSUB_TOPIC.to_string(), 202)]),
            reply: false,
        };
        node_a.send_status_path_to_all(&node_b, all.clone())?;
        let all_received = node_b.wait_for_path_status_count(2, Duration::from_secs(2));
        assert_eq!(all_received, vec![one, all]);
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
fn cluster_tools_tcp_peer_bootstrap_clears_pending_reconnect_when_peer_leaves() -> TestResult {
    let _lock = lock_tcp_smoke();
    let node = cluster_tools_tcp::ClusterToolsTcpExampleNode::bind(
        "tools-pending-node-a",
        1,
        11,
        "tools-pending-node-a-peers",
    )?;
    let result = (|| -> TestResult {
        let missing = missing_peer("tools-pending-missing", 2);
        node.publish_up_members(vec![node.self_node().clone(), missing.clone()])?;
        wait_for_tools_pending_reconnect(&node, &missing, Duration::from_secs(2))?;

        node.publish_up_members(vec![node.self_node().clone()])?;
        wait_for_tools_no_routes_or_pending(&node, Duration::from_secs(2))?;
        Ok(())
    })();

    let shutdown = node.shutdown(Duration::from_secs(1));

    result?;
    shutdown?;
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
fn cluster_tools_tcp_peer_bootstrap_keeps_remaining_pubsub_route_after_peer_removed() -> TestResult
{
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = cluster_tools_tcp::bind_three_nodes()?;
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

        let before = PubSubStatus {
            from: node_a.self_node().clone(),
            versions: BTreeMap::from([(cluster_tools_tcp::EXAMPLE_PUBSUB_TOPIC.to_string(), 11)]),
            reply: false,
        };
        node_a.send_status_to(&node_b, before.clone())?;
        let before_received = node_b.wait_for_status_count(1, Duration::from_secs(2));
        assert_eq!(before_received, vec![before.clone()]);

        node_a.publish_up_members(vec![node_a.self_node().clone(), node_b.self_node().clone()])?;
        let reduced = node_a.wait_for_route_count(1, Duration::from_secs(2))?;
        assert!(
            reduced
                .active_targets
                .iter()
                .any(|target| target.node() == node_b.self_node())
        );

        let removed = PubSubStatus {
            from: node_a.self_node().clone(),
            versions: BTreeMap::from([(cluster_tools_tcp::EXAMPLE_PUBSUB_TOPIC.to_string(), 33)]),
            reply: false,
        };
        let removed_error = node_a
            .send_status_to(&node_c, removed)
            .expect_err("removed cluster-tools peer route should reject sends");
        assert!(
            removed_error
                .to_string()
                .contains("no remote association route"),
            "unexpected removed cluster-tools peer send error: {removed_error:?}"
        );
        assert!(
            node_c
                .wait_for_status_count(1, Duration::from_millis(100))
                .is_empty()
        );

        let after = PubSubStatus {
            from: node_a.self_node().clone(),
            versions: BTreeMap::from([(cluster_tools_tcp::EXAMPLE_PUBSUB_TOPIC.to_string(), 22)]),
            reply: false,
        };
        node_a.send_status_to(&node_b, after.clone())?;
        let after_received = node_b.wait_for_status_count(2, Duration::from_secs(2));
        assert_eq!(after_received, vec![before, after]);
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

#[test]
fn cluster_tools_tcp_peer_bootstrap_delivers_pubsub_to_replacement_peer() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = cluster_tools_tcp::bind_three_nodes()?;
    let result = (|| -> TestResult {
        assert_replacement_peer_route(&node_a, &node_b, &node_c)?;

        let status = PubSubStatus {
            from: node_a.self_node().clone(),
            versions: BTreeMap::from([(cluster_tools_tcp::EXAMPLE_PUBSUB_TOPIC.to_string(), 33)]),
            reply: false,
        };
        node_a.send_status_to(&node_c, status.clone())?;
        let received = node_c.wait_for_status_count(1, Duration::from_secs(2));
        assert_eq!(received, vec![status]);

        assert!(
            node_b
                .wait_for_status_count(1, Duration::from_millis(100))
                .is_empty()
        );
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

fn missing_peer(system_name: &str, uid: u64) -> UniqueAddress {
    let listener = TcpListener::bind("127.0.0.1:0").expect("unused port should bind");
    let port = listener
        .local_addr()
        .expect("unused port should resolve")
        .port();
    drop(listener);
    UniqueAddress::new(
        Address::new(
            "kairo",
            system_name,
            Some("127.0.0.1".to_string()),
            Some(port),
        ),
        uid,
    )
}

fn wait_for_cluster_pending_reconnect(
    node: &cluster_tcp::ClusterTcpExampleNode,
    expected: &UniqueAddress,
    timeout: Duration,
) -> TestResult {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = node.connector_snapshot(timeout)?;
        let has_pending = snapshot
            .pending_reconnects
            .iter()
            .any(|pending| pending.target.node() == expected);
        if snapshot.route_count == 0 && has_pending {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for pending cluster reconnect to {expected:?}: {snapshot:?}"
            )
            .into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_cluster_no_routes_or_pending(
    node: &cluster_tcp::ClusterTcpExampleNode,
    timeout: Duration,
) -> TestResult {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = node.connector_snapshot(timeout)?;
        if snapshot.route_count == 0 && snapshot.pending_reconnects.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for cluster routes and pending reconnects to clear: {snapshot:?}"
            )
            .into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_ddata_pending_reconnect(
    node: &ddata_tcp::DDataTcpExampleNode,
    expected: &UniqueAddress,
    timeout: Duration,
) -> TestResult {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = node.connector_snapshot(timeout)?;
        let has_pending = snapshot
            .pending_reconnects
            .iter()
            .any(|pending| pending.target.node() == expected);
        if snapshot.route_count == 0 && has_pending {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for pending distributed-data reconnect to {expected:?}: {snapshot:?}"
            )
            .into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_ddata_no_routes_or_pending(
    node: &ddata_tcp::DDataTcpExampleNode,
    timeout: Duration,
) -> TestResult {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = node.connector_snapshot(timeout)?;
        if snapshot.route_count == 0 && snapshot.pending_reconnects.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for distributed-data routes and pending reconnects to clear: {snapshot:?}"
            )
            .into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_tools_pending_reconnect(
    node: &cluster_tools_tcp::ClusterToolsTcpExampleNode,
    expected: &UniqueAddress,
    timeout: Duration,
) -> TestResult {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = node.connector_snapshot(timeout)?;
        let has_pending = snapshot
            .pending_reconnects
            .iter()
            .any(|pending| pending.target.node() == expected);
        if snapshot.route_count == 0 && has_pending {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for pending cluster-tools reconnect to {expected:?}: {snapshot:?}"
            )
            .into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_tools_no_routes_or_pending(
    node: &cluster_tools_tcp::ClusterToolsTcpExampleNode,
    timeout: Duration,
) -> TestResult {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = node.connector_snapshot(timeout)?;
        if snapshot.route_count == 0 && snapshot.pending_reconnects.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for cluster-tools routes and pending reconnects to clear: {snapshot:?}"
            )
            .into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}
