use kairo_examples::cluster_tools_distributed::run_cluster_tools_distributed;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let observation = run_cluster_tools_distributed("cluster-tools-distributed")?;

    println!(
        "broadcast_topic={} delivered={} targets={:?} group_topic={} group_messages=({},{}) group_targets={:?}",
        observation.broadcast_topic,
        observation.remote_broadcast_message,
        observation.broadcast_targets,
        observation.group_topic,
        observation.local_group_message,
        observation.remote_group_message,
        observation.group_targets
    );

    Ok(())
}
