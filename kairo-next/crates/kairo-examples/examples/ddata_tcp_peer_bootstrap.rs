use std::time::Duration;

use kairo_examples::ddata_tcp::bind_two_nodes;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (node_a, node_b) = bind_two_nodes()?;
    let members = vec![node_a.self_node().clone(), node_b.self_node().clone()];

    node_a.publish_up_members(members.clone())?;
    node_b.publish_up_members(members)?;

    let snapshot_a = node_a.wait_for_route_count(1, Duration::from_secs(2))?;
    let snapshot_b = node_b.wait_for_route_count(1, Duration::from_secs(2))?;

    println!(
        "{} has {} distributed-data TCP peer route(s) at {}",
        node_a.self_node().ordering_key(),
        snapshot_a.route_count,
        node_a.local_address()
    );
    println!(
        "{} has {} distributed-data TCP peer route(s) at {}",
        node_b.self_node().ordering_key(),
        snapshot_b.route_count,
        node_b.local_address()
    );

    node_a.shutdown(Duration::from_secs(1))?;
    node_b.shutdown(Duration::from_secs(1))?;
    Ok(())
}
