use std::time::Duration;

use kairo_examples::cluster_tcp;
use kairo_examples::cluster_tools_tcp;
use kairo_examples::ddata_tcp;

#[path = "tcp_bootstrap_smoke/support.rs"]
mod support;

use support::{
    TestResult, assert_three_node_full_mesh_then_shrink, assert_two_node_bidirectional_routes,
    assert_two_node_membership_shrink, lock_tcp_smoke,
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
