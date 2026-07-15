use kairo_examples::sharding_tcp::run_three_node_sharding_acceptance;

#[test]
fn cluster_sharding_tcp_survives_join_rebalance_leave_and_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let observation = run_three_node_sharding_acceptance()?;

    assert_eq!(observation.initial_entities.len(), 6);
    assert!(
        observation
            .initial_entities
            .contains(&observation.rebalanced_entity)
    );
    assert!(
        observation
            .initial_entities
            .contains(&observation.recovered_after_leave)
    );
    assert_ne!(
        observation.rebalanced_entity,
        observation.recovered_after_leave
    );
    assert_eq!(observation.remaining_member_count, 2);
    assert!(observation.delivered_after_recovery);
    Ok(())
}
