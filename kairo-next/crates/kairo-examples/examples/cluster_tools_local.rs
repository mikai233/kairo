use kairo_examples::cluster_tools_local::run_cluster_tools_local;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let observation = run_cluster_tools_local("cluster-tools-local")?;

    println!(
        "topic={} subscribed={} delivered={} topics={:?} singleton_started={} singleton_reply={} singleton_running={}",
        observation.topic,
        observation.subscribed,
        observation.delivered_count,
        observation.current_topics,
        observation.singleton_started,
        observation.singleton_reply,
        observation.singleton_running
    );

    Ok(())
}
