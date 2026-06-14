use kairo_examples::cluster_membership::run_cluster_membership;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let observation = run_cluster_membership("cluster-membership")?;

    println!(
        "initial={} up={} removed={} previous={:?} final={}",
        observation.initial_member_count,
        observation.up_member,
        observation.removed_member,
        observation.previous_status,
        observation.final_member_count
    );

    Ok(())
}
