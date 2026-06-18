use std::time::Duration;

use kairo_examples::cluster_tcp::bind_two_nodes;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (node_a, node_b) = bind_two_nodes()?;
    let members = vec![node_a.self_node().clone(), node_b.self_node().clone()];

    node_a.publish_up_members(members.clone())?;
    node_b.publish_up_members(members)?;

    let snapshot_a = node_a.wait_for_route_count(1, Duration::from_secs(2))?;
    let snapshot_b = node_b.wait_for_route_count(1, Duration::from_secs(2))?;

    println!(
        "{} has {} cluster TCP peer route(s) at {}",
        node_a.self_node().ordering_key(),
        snapshot_a.route_count,
        node_a.local_address()
    );
    println!(
        "{} has {} cluster TCP peer route(s) at {}",
        node_b.self_node().ordering_key(),
        snapshot_b.route_count,
        node_b.local_address()
    );

    let shutdown_a = node_a.shutdown_with_observation(Duration::from_secs(1))?;
    println!(
        "cluster TCP shutdown cleared routes: {} -> {}, connector stopped: {}",
        shutdown_a.route_count_before_shutdown,
        shutdown_a.route_count_after_shutdown,
        shutdown_a.connector_stopped
    );
    node_b.shutdown(Duration::from_secs(1))?;
    Ok(())
}
