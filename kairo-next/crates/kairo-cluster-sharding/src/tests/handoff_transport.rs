use super::*;

#[test]
fn handoff_transport_sends_begin_to_participants_then_handoff_to_owner() {
    let kit = kairo_testkit::ActorSystemTestKit::new("handoff-transport").unwrap();
    let region_a = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props("region-a", 10),
        )
        .unwrap();
    let region_b = kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props("region-b", 10),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let started = kit
        .create_probe::<ShardStartedPlan<String>>("started")
        .unwrap();
    region_a
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
    region_a
        .tell(ShardRegionMsg::MarkShardStarted {
            shard: "shard-1".to_string(),
            reply_to: started.actor_ref(),
        })
        .unwrap();
    started.expect_msg(Duration::from_millis(500)).unwrap();

    let mut transport = HandoffTransport::new();
    transport.set_targets([
        HandoffRegionTarget::new("region-a", region_a),
        HandoffRegionTarget::new("region-b", region_b),
    ]);
    let plan = ShardRebalancePlan {
        shard: "shard-1".to_string(),
        from_region: "region-a".to_string(),
        participants: BTreeSet::from(["region-a".to_string(), "region-b".to_string()]),
        begin_handoff: crate::BeginHandOff {
            shard_id: "shard-1".to_string(),
        },
    };
    let begin = kit.create_probe::<BeginHandOffPlan>("begin").unwrap();
    let handoff = kit.create_probe::<HandOffPlan>("handoff").unwrap();

    let begin_report = transport.send_begin_handoff(&plan, begin.actor_ref());

    assert!(begin_report.is_success());
    assert_eq!(
        begin_report.sent_to(),
        &[
            HandoffDeliveryTarget::BeginHandOff {
                region: "region-a".to_string(),
            },
            HandoffDeliveryTarget::BeginHandOff {
                region: "region-b".to_string(),
            },
        ]
    );
    for _ in 0..2 {
        assert_eq!(
            begin.expect_msg(Duration::from_millis(500)).unwrap(),
            BeginHandOffPlan::Ack {
                shard: "shard-1".to_string(),
                ack: crate::BeginHandOffAck {
                    shard_id: "shard-1".to_string(),
                },
            }
        );
    }

    let handoff_report = transport.send_handoff(&plan, handoff.actor_ref());

    assert!(handoff_report.is_success());
    assert_eq!(
        handoff_report.sent_to(),
        &[HandoffDeliveryTarget::HandOff {
            region: "region-a".to_string(),
        }]
    );
    assert_eq!(
        handoff.expect_msg(Duration::from_millis(500)).unwrap(),
        HandOffPlan::ForwardToLocalShard {
            shard: "shard-1".to_string(),
            command: HandOff {
                shard_id: "shard-1".to_string(),
            },
            dropped_buffered: 0,
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn handoff_transport_reports_missing_targets_without_stopping_other_sends() {
    let kit = kairo_testkit::ActorSystemTestKit::new("handoff-transport-missing").unwrap();
    let region_a = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props("region-a", 10),
        )
        .unwrap();
    let mut transport = HandoffTransport::new();
    transport.insert_target(HandoffRegionTarget::new("region-a", region_a));
    let plan = ShardRebalancePlan {
        shard: "shard-1".to_string(),
        from_region: "region-c".to_string(),
        participants: BTreeSet::from(["region-a".to_string(), "region-b".to_string()]),
        begin_handoff: crate::BeginHandOff {
            shard_id: "shard-1".to_string(),
        },
    };
    let begin = kit.create_probe::<BeginHandOffPlan>("begin").unwrap();
    let handoff = kit.create_probe::<HandOffPlan>("handoff").unwrap();

    let begin_report = transport.send_begin_handoff(&plan, begin.actor_ref());

    assert_eq!(
        begin_report.sent_to(),
        &[HandoffDeliveryTarget::BeginHandOff {
            region: "region-a".to_string(),
        }]
    );
    assert_eq!(
        begin_report.failures(),
        &[HandoffDeliveryFailure::MissingTarget {
            target: HandoffDeliveryTarget::BeginHandOff {
                region: "region-b".to_string(),
            },
        }]
    );
    begin.expect_msg(Duration::from_millis(500)).unwrap();

    let handoff_report = transport.send_handoff(&plan, handoff.actor_ref());

    assert_eq!(handoff_report.sent_to(), &[]);
    assert_eq!(
        handoff_report.failures(),
        &[HandoffDeliveryFailure::MissingTarget {
            target: HandoffDeliveryTarget::HandOff {
                region: "region-c".to_string(),
            },
        }]
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
