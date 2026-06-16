use kairo_examples::cluster_tools_singleton::run_cluster_tools_singleton;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let observation = run_cluster_tools_singleton("cluster-tools-singleton")?;

    println!(
        "first={} second={} handover_requested={} handover_in_progress={} first_stopped={} second_started={} ordered={}",
        observation.first_node,
        observation.second_node,
        observation.handover_requested,
        observation.handover_in_progress,
        observation.first_stopped,
        observation.second_started,
        observation.second_started_after_first_stopped
    );

    Ok(())
}
