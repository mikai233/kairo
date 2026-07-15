use kairo_examples::sharding_tcp::run_three_node_sharding_acceptance;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let observation = run_three_node_sharding_acceptance()?;
    println!(
        "three-node sharding rebalanced {}, recovered {} after oldest-node leave, and delivered afterward: {}",
        observation.rebalanced_entity,
        observation.recovered_after_leave,
        observation.delivered_after_recovery,
    );
    Ok(())
}
