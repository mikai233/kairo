use std::time::Duration;

use kairo_examples::cluster_tools_tcp;
use kairo_examples::ddata_tcp;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn ddata_tcp_peer_bootstrap_establishes_bidirectional_routes() -> TestResult {
    let (node_a, node_b) = ddata_tcp::bind_two_nodes()?;
    let result: TestResult = (|| {
        let members = vec![node_a.self_node().clone(), node_b.self_node().clone()];

        node_a.publish_up_members(members.clone())?;
        node_b.publish_up_members(members)?;

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
fn cluster_tools_tcp_peer_bootstrap_establishes_bidirectional_routes() -> TestResult {
    let (node_a, node_b) = cluster_tools_tcp::bind_two_nodes()?;
    let result: TestResult = (|| {
        let members = vec![node_a.self_node().clone(), node_b.self_node().clone()];

        node_a.publish_up_members(members.clone())?;
        node_b.publish_up_members(members)?;

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
