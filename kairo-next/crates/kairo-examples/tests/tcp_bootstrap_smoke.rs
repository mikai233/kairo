use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use kairo::cluster::UniqueAddress;
use kairo_examples::cluster_tcp;
use kairo_examples::cluster_tools_tcp;
use kairo_examples::ddata_tcp;

type TestResult = Result<(), Box<dyn std::error::Error>>;

static TCP_SMOKE_LOCK: Mutex<()> = Mutex::new(());

fn lock_tcp_smoke() -> MutexGuard<'static, ()> {
    TCP_SMOKE_LOCK.lock().expect("tcp smoke lock poisoned")
}

trait TcpSmokeNode {
    fn self_node(&self) -> &UniqueAddress;

    fn publish_up_members(&self, members: Vec<UniqueAddress>) -> TestResult;

    fn wait_for_route_count(&self, route_count: usize, timeout: Duration) -> TestResult;
}

impl TcpSmokeNode for cluster_tcp::ClusterTcpExampleNode {
    fn self_node(&self) -> &UniqueAddress {
        cluster_tcp::ClusterTcpExampleNode::self_node(self)
    }

    fn publish_up_members(&self, members: Vec<UniqueAddress>) -> TestResult {
        cluster_tcp::ClusterTcpExampleNode::publish_up_members(self, members)?;
        Ok(())
    }

    fn wait_for_route_count(&self, route_count: usize, timeout: Duration) -> TestResult {
        cluster_tcp::ClusterTcpExampleNode::wait_for_route_count(self, route_count, timeout)?;
        Ok(())
    }
}

impl TcpSmokeNode for ddata_tcp::DDataTcpExampleNode {
    fn self_node(&self) -> &UniqueAddress {
        ddata_tcp::DDataTcpExampleNode::self_node(self)
    }

    fn publish_up_members(&self, members: Vec<UniqueAddress>) -> TestResult {
        ddata_tcp::DDataTcpExampleNode::publish_up_members(self, members)?;
        Ok(())
    }

    fn wait_for_route_count(&self, route_count: usize, timeout: Duration) -> TestResult {
        ddata_tcp::DDataTcpExampleNode::wait_for_route_count(self, route_count, timeout)?;
        Ok(())
    }
}

impl TcpSmokeNode for cluster_tools_tcp::ClusterToolsTcpExampleNode {
    fn self_node(&self) -> &UniqueAddress {
        cluster_tools_tcp::ClusterToolsTcpExampleNode::self_node(self)
    }

    fn publish_up_members(&self, members: Vec<UniqueAddress>) -> TestResult {
        cluster_tools_tcp::ClusterToolsTcpExampleNode::publish_up_members(self, members)?;
        Ok(())
    }

    fn wait_for_route_count(&self, route_count: usize, timeout: Duration) -> TestResult {
        cluster_tools_tcp::ClusterToolsTcpExampleNode::wait_for_route_count(
            self,
            route_count,
            timeout,
        )?;
        Ok(())
    }
}

fn publish_current_membership<N: TcpSmokeNode>(nodes: &[&N]) -> TestResult {
    let members = nodes
        .iter()
        .map(|node| node.self_node().clone())
        .collect::<Vec<_>>();
    publish_membership(nodes, members)
}

fn publish_membership<N: TcpSmokeNode>(nodes: &[&N], members: Vec<UniqueAddress>) -> TestResult {
    for node in nodes {
        node.publish_up_members(members.clone())?;
    }
    Ok(())
}

fn wait_for_route_count<N: TcpSmokeNode>(
    nodes: &[&N],
    route_count: usize,
    timeout: Duration,
) -> TestResult {
    for node in nodes {
        node.wait_for_route_count(route_count, timeout)?;
    }
    Ok(())
}

#[test]
fn cluster_tcp_peer_bootstrap_establishes_bidirectional_routes() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b) = cluster_tcp::bind_two_nodes()?;
    let result: TestResult = (|| {
        publish_current_membership(&[&node_a, &node_b])?;
        let snapshot_a = node_a.wait_for_route_count(1, Duration::from_secs(2))?;
        let snapshot_b = node_b.wait_for_route_count(1, Duration::from_secs(2))?;

        assert_eq!(snapshot_a.route_count, 1);
        assert_eq!(snapshot_b.route_count, 1);
        assert!(node_a.local_address().contains("127.0.0.1"));
        assert!(node_b.local_address().contains("127.0.0.1"));
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
    let result: TestResult = (|| {
        publish_current_membership(&[&node_a, &node_b])?;
        wait_for_route_count(&[&node_a, &node_b], 1, Duration::from_secs(2))?;

        node_a.publish_up_members([node_a.self_node().clone()])?;
        node_a.wait_for_route_count(0, Duration::from_secs(2))?;
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
fn cluster_tcp_peer_bootstrap_establishes_three_node_full_mesh_and_shrinks() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = cluster_tcp::bind_three_nodes()?;
    let result: TestResult = (|| {
        publish_current_membership(&[&node_a, &node_b, &node_c])?;
        wait_for_route_count(&[&node_a, &node_b, &node_c], 2, Duration::from_secs(2))?;

        publish_membership(
            &[&node_a, &node_b],
            vec![node_a.self_node().clone(), node_b.self_node().clone()],
        )?;
        wait_for_route_count(&[&node_a, &node_b], 1, Duration::from_secs(2))?;
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
    let result: TestResult = (|| {
        publish_current_membership(&[&node_a, &node_b])?;
        let snapshot_a = node_a.wait_for_route_count(1, Duration::from_secs(2))?;
        let snapshot_b = node_b.wait_for_route_count(1, Duration::from_secs(2))?;

        assert_eq!(snapshot_a.route_count, 1);
        assert_eq!(snapshot_b.route_count, 1);
        assert!(node_a.local_address().contains("127.0.0.1"));
        assert!(node_b.local_address().contains("127.0.0.1"));
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
    let result: TestResult = (|| {
        publish_current_membership(&[&node_a, &node_b])?;
        wait_for_route_count(&[&node_a, &node_b], 1, Duration::from_secs(2))?;

        node_a.publish_up_members([node_a.self_node().clone()])?;
        node_a.wait_for_route_count(0, Duration::from_secs(2))?;
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
fn ddata_tcp_peer_bootstrap_establishes_three_node_full_mesh_and_shrinks() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = ddata_tcp::bind_three_nodes()?;
    let result: TestResult = (|| {
        publish_current_membership(&[&node_a, &node_b, &node_c])?;
        wait_for_route_count(&[&node_a, &node_b, &node_c], 2, Duration::from_secs(2))?;

        publish_membership(
            &[&node_a, &node_b],
            vec![node_a.self_node().clone(), node_b.self_node().clone()],
        )?;
        wait_for_route_count(&[&node_a, &node_b], 1, Duration::from_secs(2))?;
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
    let result: TestResult = (|| {
        publish_current_membership(&[&node_a, &node_b])?;
        let snapshot_a = node_a.wait_for_route_count(1, Duration::from_secs(2))?;
        let snapshot_b = node_b.wait_for_route_count(1, Duration::from_secs(2))?;

        assert_eq!(snapshot_a.route_count, 1);
        assert_eq!(snapshot_b.route_count, 1);
        assert!(node_a.local_address().contains("127.0.0.1"));
        assert!(node_b.local_address().contains("127.0.0.1"));
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
    let result: TestResult = (|| {
        publish_current_membership(&[&node_a, &node_b])?;
        wait_for_route_count(&[&node_a, &node_b], 1, Duration::from_secs(2))?;

        node_a.publish_up_members([node_a.self_node().clone()])?;
        node_a.wait_for_route_count(0, Duration::from_secs(2))?;
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
fn cluster_tools_tcp_peer_bootstrap_establishes_three_node_full_mesh_and_shrinks() -> TestResult {
    let _lock = lock_tcp_smoke();
    let (node_a, node_b, node_c) = cluster_tools_tcp::bind_three_nodes()?;
    let result: TestResult = (|| {
        publish_current_membership(&[&node_a, &node_b, &node_c])?;
        wait_for_route_count(&[&node_a, &node_b, &node_c], 2, Duration::from_secs(2))?;

        publish_membership(
            &[&node_a, &node_b],
            vec![node_a.self_node().clone(), node_b.self_node().clone()],
        )?;
        wait_for_route_count(&[&node_a, &node_b], 1, Duration::from_secs(2))?;
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
