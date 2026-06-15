use super::*;

#[test]
fn region_runtime_buffers_unknown_shard_and_requests_home_once() {
    let mut runtime = ShardRegionRuntime::new("region-a", 10);

    let first = runtime.route("shard-1", ShardingEnvelope::new("entity-1", "first"));
    let second = runtime.route("shard-1", ShardingEnvelope::new("entity-1", "second"));

    assert_eq!(
        first,
        RegionRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: Some(GetShardHome {
                shard_id: "shard-1".to_string(),
            }),
        }
    );
    assert_eq!(
        second,
        RegionRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: None,
        }
    );
    assert_eq!(runtime.buffered_count(&"shard-1".to_string()), 2);
}

#[test]
fn region_runtime_records_remote_home_and_forwards_buffered_messages() {
    let mut runtime = ShardRegionRuntime::new("region-a", 10);
    assert!(matches!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-1", "first")),
        RegionRoutePlan::Buffered { .. }
    ));
    assert!(matches!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-2", "second")),
        RegionRoutePlan::Buffered { .. }
    ));

    let plan = runtime.record_shard_home("shard-1", "region-b").unwrap();

    assert_eq!(
        plan,
        ShardHomePlan::Forward {
            shard: "shard-1".to_string(),
            region: "region-b".to_string(),
            buffered: vec![
                ShardingEnvelope::new("entity-1", "first"),
                ShardingEnvelope::new("entity-2", "second"),
            ],
        }
    );
    assert_eq!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-3", "third")),
        RegionRoutePlan::Forward {
            shard: "shard-1".to_string(),
            region: "region-b".to_string(),
            message: ShardingEnvelope::new("entity-3", "third"),
        }
    );
}

#[test]
fn region_runtime_starts_local_shard_then_delivers_buffered_messages() {
    let mut runtime = ShardRegionRuntime::new("region-a", 10);
    assert!(matches!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-1", "first")),
        RegionRoutePlan::Buffered { .. }
    ));

    let home = runtime.record_shard_home("shard-1", "region-a").unwrap();
    assert_eq!(
        home,
        ShardHomePlan::StartLocalShard {
            shard: "shard-1".to_string(),
            command: HostShard {
                shard_id: "shard-1".to_string(),
            },
        }
    );
    assert!(runtime.starting_shards().contains("shard-1"));

    let started = runtime.mark_shard_started("shard-1");
    assert_eq!(
        started.started,
        ShardStarted {
            shard_id: "shard-1".to_string(),
        }
    );
    assert_eq!(
        started.buffered,
        vec![ShardingEnvelope::new("entity-1", "first")]
    );
    assert!(runtime.local_shards().contains("shard-1"));
    assert_eq!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-1", "second")),
        RegionRoutePlan::DeliverLocal {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "second"),
        }
    );
}

#[test]
fn region_runtime_host_shard_ignores_graceful_shutdown() {
    let mut runtime = ShardRegionRuntime::<&str>::new("region-a", 10);
    runtime.set_graceful_shutdown_in_progress(true);

    assert_eq!(
        runtime.host_shard("shard-1"),
        HostShardPlan::IgnoredGracefulShutdown {
            shard: "shard-1".to_string(),
        }
    );
    assert_eq!(runtime.region_for_shard(&"shard-1".to_string()), None);
}

#[test]
fn region_runtime_host_shard_marks_local_starting() {
    let mut runtime = ShardRegionRuntime::<&str>::new("region-a", 10);

    assert_eq!(
        runtime.host_shard("shard-1"),
        HostShardPlan::StartLocalShard {
            shard: "shard-1".to_string(),
            command: HostShard {
                shard_id: "shard-1".to_string(),
            },
        }
    );
    assert_eq!(
        runtime.region_for_shard(&"shard-1".to_string()),
        Some(&"region-a".to_string())
    );
    assert!(runtime.starting_shards().contains("shard-1"));
}

#[test]
fn region_runtime_rejects_inconsistent_remote_home_for_known_local_shard() {
    let mut runtime = ShardRegionRuntime::<&str>::new("region-a", 10);
    runtime.host_shard("shard-1");
    runtime.mark_shard_started("shard-1");

    assert_eq!(
        runtime
            .record_shard_home("shard-1", "region-b")
            .unwrap_err(),
        ShardingError::InconsistentShardHome {
            shard: "shard-1".to_string(),
            current_region: "region-a".to_string(),
            new_region: "region-b".to_string(),
        }
    );
    assert_eq!(
        runtime.region_for_shard(&"shard-1".to_string()),
        Some(&"region-a".to_string())
    );
    assert!(runtime.local_shards().contains("shard-1"));
}

#[test]
fn region_runtime_begin_handoff_removes_shard_home_and_acks() {
    let mut runtime = ShardRegionRuntime::<&str>::new("region-a", 10);
    runtime.host_shard("shard-1");
    runtime.mark_shard_started("shard-1");

    assert_eq!(
        runtime.begin_handoff("shard-1"),
        BeginHandOffPlan::Ack {
            shard: "shard-1".to_string(),
            ack: crate::BeginHandOffAck {
                shard_id: "shard-1".to_string(),
            },
        }
    );
    assert_eq!(runtime.region_for_shard(&"shard-1".to_string()), None);
    assert_eq!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-1", "after-begin")),
        RegionRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: Some(GetShardHome {
                shard_id: "shard-1".to_string(),
            }),
        }
    );
}

#[test]
fn region_runtime_begin_handoff_is_ignored_while_preparing_shutdown() {
    let mut runtime = ShardRegionRuntime::<&str>::new("region-a", 10);
    runtime.host_shard("shard-1");
    runtime.set_preparing_for_shutdown(true);

    assert_eq!(
        runtime.begin_handoff("shard-1"),
        BeginHandOffPlan::IgnoredPreparingForShutdown {
            shard: "shard-1".to_string(),
        }
    );
    assert_eq!(
        runtime.region_for_shard(&"shard-1".to_string()),
        Some(&"region-a".to_string())
    );
}

#[test]
fn region_runtime_handoff_drops_buffer_to_preserve_order_and_forwards_local_shard() {
    let mut runtime = ShardRegionRuntime::new("region-a", 10);
    runtime.host_shard("shard-1");
    runtime.mark_shard_started("shard-1");
    runtime.begin_handoff("shard-1");
    assert!(matches!(
        runtime.route(
            "shard-1",
            ShardingEnvelope::new("entity-1", "buffered-after-begin")
        ),
        RegionRoutePlan::Buffered { .. }
    ));

    assert_eq!(
        runtime.handoff("shard-1"),
        HandOffPlan::ForwardToLocalShard {
            shard: "shard-1".to_string(),
            command: HandOff {
                shard_id: "shard-1".to_string(),
            },
            dropped_buffered: 1,
        }
    );
    assert_eq!(runtime.buffered_count(&"shard-1".to_string()), 0);
    assert!(runtime.handing_off_shards().contains("shard-1"));
}

#[test]
fn region_runtime_handoff_without_begin_handoff_stops_routing_to_local_shard() {
    let mut runtime = ShardRegionRuntime::new("region-a", 10);
    runtime.host_shard("shard-1");
    runtime.mark_shard_started("shard-1");

    assert_eq!(
        runtime.handoff("shard-1"),
        HandOffPlan::ForwardToLocalShard {
            shard: "shard-1".to_string(),
            command: HandOff {
                shard_id: "shard-1".to_string(),
            },
            dropped_buffered: 0,
        }
    );
    assert_eq!(runtime.region_for_shard(&"shard-1".to_string()), None);
    assert_eq!(
        runtime.route(
            "shard-1",
            ShardingEnvelope::new("entity-1", "after-handoff")
        ),
        RegionRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: Some(GetShardHome {
                shard_id: "shard-1".to_string(),
            }),
        }
    );
}

#[test]
fn region_runtime_handoff_replies_stopped_when_local_shard_is_absent() {
    let mut runtime = ShardRegionRuntime::new("region-a", 10);
    assert!(matches!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-1", "buffered")),
        RegionRoutePlan::Buffered { .. }
    ));

    assert_eq!(
        runtime.handoff("shard-1"),
        HandOffPlan::ReplyShardStopped {
            shard: "shard-1".to_string(),
            stopped: ShardStopped {
                shard_id: "shard-1".to_string(),
            },
            dropped_buffered: 1,
        }
    );
}

#[test]
fn region_runtime_drops_empty_shard_and_full_buffer_messages() {
    let mut runtime = ShardRegionRuntime::new("region-a", 1);

    assert_eq!(
        runtime.route("", ShardingEnvelope::new("entity-1", "empty")),
        RegionRoutePlan::Dropped {
            shard: None,
            reason: RegionDropReason::EmptyShardId,
            message: ShardingEnvelope::new("entity-1", "empty"),
        }
    );
    assert!(matches!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-1", "first")),
        RegionRoutePlan::Buffered { .. }
    ));
    assert_eq!(
        runtime.route("shard-2", ShardingEnvelope::new("entity-2", "second")),
        RegionRoutePlan::Dropped {
            shard: Some("shard-2".to_string()),
            reason: RegionDropReason::BufferFull,
            message: ShardingEnvelope::new("entity-2", "second"),
        }
    );
}
