use std::error::Error;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use kairo::cluster::UniqueAddress;
use kairo_examples::cluster_tcp;
use kairo_examples::cluster_tools_tcp;
use kairo_examples::ddata_tcp;

pub type TestResult = Result<(), Box<dyn Error>>;

static TCP_SMOKE_LOCK: Mutex<()> = Mutex::new(());

pub fn lock_tcp_smoke() -> MutexGuard<'static, ()> {
    TCP_SMOKE_LOCK.lock().expect("tcp smoke lock poisoned")
}

pub trait TcpSmokeNode {
    fn self_node(&self) -> &UniqueAddress;

    fn local_address(&self) -> String;

    fn publish_up_members(&self, members: Vec<UniqueAddress>) -> TestResult;

    fn wait_for_route_count(&self, route_count: usize, timeout: Duration) -> TestResult;

    fn wait_for_route_to(&self, peer: &UniqueAddress, timeout: Duration) -> TestResult;
}

impl TcpSmokeNode for cluster_tcp::ClusterTcpExampleNode {
    fn self_node(&self) -> &UniqueAddress {
        cluster_tcp::ClusterTcpExampleNode::self_node(self)
    }

    fn local_address(&self) -> String {
        cluster_tcp::ClusterTcpExampleNode::local_address(self)
    }

    fn publish_up_members(&self, members: Vec<UniqueAddress>) -> TestResult {
        cluster_tcp::ClusterTcpExampleNode::publish_up_members(self, members)?;
        Ok(())
    }

    fn wait_for_route_count(&self, route_count: usize, timeout: Duration) -> TestResult {
        cluster_tcp::ClusterTcpExampleNode::wait_for_route_count(self, route_count, timeout)?;
        Ok(())
    }

    fn wait_for_route_to(&self, peer: &UniqueAddress, timeout: Duration) -> TestResult {
        let snapshot = cluster_tcp::ClusterTcpExampleNode::wait_for_route_count(self, 1, timeout)?;
        if snapshot
            .active_targets
            .iter()
            .any(|target| target.node() == peer)
        {
            Ok(())
        } else {
            Err(format!("cluster peer route to {peer:?} was not installed: {snapshot:?}").into())
        }
    }
}

impl TcpSmokeNode for ddata_tcp::DDataTcpExampleNode {
    fn self_node(&self) -> &UniqueAddress {
        ddata_tcp::DDataTcpExampleNode::self_node(self)
    }

    fn local_address(&self) -> String {
        ddata_tcp::DDataTcpExampleNode::local_address(self)
    }

    fn publish_up_members(&self, members: Vec<UniqueAddress>) -> TestResult {
        ddata_tcp::DDataTcpExampleNode::publish_up_members(self, members)?;
        Ok(())
    }

    fn wait_for_route_count(&self, route_count: usize, timeout: Duration) -> TestResult {
        ddata_tcp::DDataTcpExampleNode::wait_for_route_count(self, route_count, timeout)?;
        Ok(())
    }

    fn wait_for_route_to(&self, peer: &UniqueAddress, timeout: Duration) -> TestResult {
        let snapshot = ddata_tcp::DDataTcpExampleNode::wait_for_route_count(self, 1, timeout)?;
        if snapshot
            .active_targets
            .iter()
            .any(|target| target.node() == peer)
        {
            Ok(())
        } else {
            Err(
                format!("distributed-data peer route to {peer:?} was not installed: {snapshot:?}")
                    .into(),
            )
        }
    }
}

impl TcpSmokeNode for cluster_tools_tcp::ClusterToolsTcpExampleNode {
    fn self_node(&self) -> &UniqueAddress {
        cluster_tools_tcp::ClusterToolsTcpExampleNode::self_node(self)
    }

    fn local_address(&self) -> String {
        cluster_tools_tcp::ClusterToolsTcpExampleNode::local_address(self)
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

    fn wait_for_route_to(&self, peer: &UniqueAddress, timeout: Duration) -> TestResult {
        let snapshot =
            cluster_tools_tcp::ClusterToolsTcpExampleNode::wait_for_route_count(self, 1, timeout)?;
        if snapshot
            .active_targets
            .iter()
            .any(|target| target.node() == peer)
        {
            Ok(())
        } else {
            Err(
                format!("cluster-tools peer route to {peer:?} was not installed: {snapshot:?}")
                    .into(),
            )
        }
    }
}

pub fn assert_two_node_bidirectional_routes<N: TcpSmokeNode>(node_a: &N, node_b: &N) -> TestResult {
    publish_current_membership(&[node_a, node_b])?;
    wait_for_route_count(&[node_a, node_b], 1, Duration::from_secs(2))?;
    assert!(node_a.local_address().contains("127.0.0.1"));
    assert!(node_b.local_address().contains("127.0.0.1"));
    Ok(())
}

pub fn assert_two_node_membership_shrink<N: TcpSmokeNode>(node_a: &N, node_b: &N) -> TestResult {
    publish_current_membership(&[node_a, node_b])?;
    wait_for_route_count(&[node_a, node_b], 1, Duration::from_secs(2))?;

    publish_membership(&[node_a, node_b], vec![node_a.self_node().clone()])?;
    wait_for_route_count(&[node_a, node_b], 0, Duration::from_secs(2))?;
    Ok(())
}

pub fn assert_three_node_full_mesh_then_shrink<N: TcpSmokeNode>(
    node_a: &N,
    node_b: &N,
    node_c: &N,
) -> TestResult {
    publish_current_membership(&[node_a, node_b, node_c])?;
    wait_for_route_count(&[node_a, node_b, node_c], 2, Duration::from_secs(2))?;

    publish_membership(
        &[node_a, node_b, node_c],
        vec![node_a.self_node().clone(), node_b.self_node().clone()],
    )?;
    wait_for_route_count(&[node_a, node_b], 1, Duration::from_secs(2))?;
    node_c.wait_for_route_count(0, Duration::from_secs(2))?;
    Ok(())
}

pub fn assert_replacement_peer_route<N: TcpSmokeNode>(
    sender: &N,
    old_receiver: &N,
    new_receiver: &N,
) -> TestResult {
    sender.publish_up_members(vec![
        sender.self_node().clone(),
        old_receiver.self_node().clone(),
    ])?;
    sender.wait_for_route_to(old_receiver.self_node(), Duration::from_secs(2))?;

    sender.publish_up_members(vec![sender.self_node().clone()])?;
    sender.wait_for_route_count(0, Duration::from_secs(2))?;

    sender.publish_up_members(vec![
        sender.self_node().clone(),
        new_receiver.self_node().clone(),
    ])?;
    sender.wait_for_route_to(new_receiver.self_node(), Duration::from_secs(2))?;
    Ok(())
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
