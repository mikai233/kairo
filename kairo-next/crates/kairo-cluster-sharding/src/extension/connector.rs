use super::*;

#[derive(Debug, Clone)]
pub(super) enum ClusterShardingConnectorMsg {
    Cluster(ClusterSubscriptionEvent),
}

pub(super) struct ClusterShardingConnectorConfig<M>
where
    M: Clone + Send + 'static,
{
    pub(super) cluster: Cluster,
    pub(super) self_node: UniqueAddress,
    pub(super) coordinator: ActorRef<ShardCoordinatorMsg<M>>,
    pub(super) region: ActorRef<ShardRegionMsg<M>>,
    pub(super) region_wire: ActorRefWireData,
    pub(super) coordinator_path: String,
    pub(super) region_path: String,
    pub(super) registry: Arc<Registry>,
    pub(super) outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
}

pub(super) struct ClusterShardingConnector<M>
where
    M: Clone + Send + 'static,
{
    config: ClusterShardingConnectorConfig<M>,
    members: BTreeMap<String, Member>,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoordinatorRegionLifecycle {
    Eligible,
    Pending,
    Terminating,
    Removed,
}

fn coordinator_region_lifecycle(status: &MemberStatus) -> CoordinatorRegionLifecycle {
    match status {
        MemberStatus::Up | MemberStatus::WeaklyUp => CoordinatorRegionLifecycle::Eligible,
        MemberStatus::Leaving | MemberStatus::Exiting => CoordinatorRegionLifecycle::Terminating,
        MemberStatus::Joining => CoordinatorRegionLifecycle::Pending,
        MemberStatus::Down => CoordinatorRegionLifecycle::Terminating,
        MemberStatus::Removed => CoordinatorRegionLifecycle::Removed,
    }
}

fn member_event_member(event: &MemberEvent) -> &Member {
    match event {
        MemberEvent::Joined(member)
        | MemberEvent::WeaklyUp(member)
        | MemberEvent::Up(member)
        | MemberEvent::Left(member)
        | MemberEvent::Exited(member)
        | MemberEvent::Downed(member)
        | MemberEvent::Removed { member, .. } => member,
    }
}

fn member_is_in_self_data_center(
    members: &BTreeMap<String, Member>,
    self_node: &UniqueAddress,
    member: &Member,
) -> bool {
    members
        .get(&self_node.ordering_key())
        .is_none_or(|self_member| self_member.data_center() == member.data_center())
}

impl<M> ClusterShardingConnector<M>
where
    M: Clone + Send + 'static,
{
    pub(super) fn props(config: ClusterShardingConnectorConfig<M>) -> Props<Self> {
        let config = Arc::new(config);
        Props::new(move || Self {
            config: ClusterShardingConnectorConfig {
                cluster: config.cluster.clone(),
                self_node: config.self_node.clone(),
                coordinator: config.coordinator.clone(),
                region: config.region.clone(),
                region_wire: config.region_wire.clone(),
                coordinator_path: config.coordinator_path.clone(),
                region_path: config.region_path.clone(),
                registry: config.registry.clone(),
                outbound: config.outbound.clone(),
            },
            members: BTreeMap::new(),
            subscription: None,
        })
    }

    fn update_targets(&self) -> Result<(), ActorError>
    where
        M: RemoteMessage,
    {
        let mut remote_coordinators = Vec::new();
        let mut remote_regions = Vec::new();
        for member in self.members.values() {
            let node = member.unique_address.clone();
            if node == self.config.self_node {
                continue;
            }
            let coordinator =
                ShardCoordinatorRemoteTarget::for_node(node.clone(), &self.config.coordinator_path)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            remote_coordinators.push(coordinator);
            let region_wire =
                ActorRefWireData::new(format!("{}{}", node.address, self.config.region_path))
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            let region_id = remote_region_id(&region_wire);
            let target = ShardRegionRemoteOutbound::from_arc(
                node,
                self.config.registry.clone(),
                self.config.outbound.clone(),
            )
            .with_recipient_path(self.config.region_path.clone())
            .with_sender(Some(self.config.region_wire.clone()))
            .into_region_route_target(region_id);
            remote_regions.push(target);
        }
        self.config
            .region
            .tell(ShardRegionMsg::SetClusterTargets {
                local_coordinator: (
                    self.config.self_node.clone(),
                    self.config.coordinator.clone(),
                ),
                remote_coordinators,
                remote_regions,
            })
            .map_err(|error| ActorError::Message(error.reason().to_string()))
    }

    fn sync_coordinator_region_lifecycle(&self, member: &Member) -> ActorResult {
        let region_wire = ActorRefWireData::new(format!(
            "{}{}",
            member.unique_address.address, self.config.region_path
        ))
        .map_err(|error| ActorError::Message(error.to_string()))?;
        let region = remote_region_id(&region_wire);
        match coordinator_region_lifecycle(&member.status) {
            CoordinatorRegionLifecycle::Eligible => {
                self.tell_coordinator(ShardCoordinatorMsg::UnmarkRegionTerminating { region })
            }
            CoordinatorRegionLifecycle::Pending => Ok(()),
            CoordinatorRegionLifecycle::Terminating => {
                self.tell_coordinator(ShardCoordinatorMsg::MarkRegionTerminating { region })
            }
            CoordinatorRegionLifecycle::Removed => {
                self.tell_coordinator(ShardCoordinatorMsg::RegionStopped { region })
            }
        }
    }

    fn sync_coordinator_region_reachability(
        &self,
        member: &Member,
        unavailable: bool,
    ) -> ActorResult {
        if !self.member_is_in_self_data_center(member) {
            return Ok(());
        }
        let region_wire = ActorRefWireData::new(format!(
            "{}{}",
            member.unique_address.address, self.config.region_path
        ))
        .map_err(|error| ActorError::Message(error.to_string()))?;
        let region = remote_region_id(&region_wire);
        if unavailable {
            self.tell_coordinator(ShardCoordinatorMsg::MarkRegionUnavailable { region })
        } else {
            self.tell_coordinator(ShardCoordinatorMsg::UnmarkRegionUnavailable { region })
        }
    }

    fn member_is_in_self_data_center(&self, member: &Member) -> bool {
        member_is_in_self_data_center(&self.members, &self.config.self_node, member)
    }

    fn tell_coordinator(&self, message: ShardCoordinatorMsg<M>) -> ActorResult {
        self.config
            .coordinator
            .tell(message)
            .map_err(|error| ActorError::Message(error.reason().to_string()))
    }

    fn apply_event(&mut self, event: &ClusterEvent) {
        let ClusterEvent::Member(event) = event else {
            return;
        };
        match event {
            MemberEvent::Removed { member, .. } => {
                self.members.remove(&member.unique_address.ordering_key());
            }
            MemberEvent::Joined(member)
            | MemberEvent::WeaklyUp(member)
            | MemberEvent::Up(member)
            | MemberEvent::Left(member)
            | MemberEvent::Exited(member)
            | MemberEvent::Downed(member) => {
                self.members
                    .insert(member.unique_address.ordering_key(), member.clone());
            }
        }
    }
}

impl<M> Actor for ClusterShardingConnector<M>
where
    M: Clone + RemoteMessage + Send + 'static,
{
    type Msg = ClusterShardingConnectorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription = ctx.message_adapter(ClusterShardingConnectorMsg::Cluster)?;
        self.config
            .cluster
            .subscribe_with_initial_state(
                subscription.clone(),
                ClusterSubscriptionInitialState::Snapshot,
            )
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.subscription = Some(subscription);
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        if let Some(subscription) = self.subscription.take() {
            let _ = self.config.cluster.unsubscribe(subscription);
        }
        Ok(())
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterShardingConnectorMsg::Cluster(ClusterSubscriptionEvent::CurrentState(state)) => {
                self.members = state
                    .members
                    .iter()
                    .cloned()
                    .map(|member| (member.unique_address.ordering_key(), member))
                    .collect();
                let unreachable = state
                    .unreachable
                    .iter()
                    .map(|member| member.unique_address.ordering_key())
                    .collect::<BTreeSet<_>>();
                for member in &state.members {
                    self.sync_coordinator_region_lifecycle(member)?;
                    self.sync_coordinator_region_reachability(
                        member,
                        unreachable.contains(&member.unique_address.ordering_key()),
                    )?;
                }
                self.update_targets()?;
                self.config
                    .region
                    .tell(ShardRegionMsg::CoordinatorDiscoverySnapshot { state })
                    .map_err(|error| ActorError::Message(error.reason().to_string()))
            }
            ClusterShardingConnectorMsg::Cluster(ClusterSubscriptionEvent::Event(event)) => {
                if let ClusterEvent::Member(member_event) = &event {
                    self.sync_coordinator_region_lifecycle(member_event_member(member_event))?;
                }
                if let ClusterEvent::Reachability(reachability_event) = &event {
                    match reachability_event {
                        ReachabilityEvent::Unreachable(member) => {
                            self.sync_coordinator_region_reachability(member, true)?;
                        }
                        ReachabilityEvent::Reachable(member) => {
                            self.sync_coordinator_region_reachability(member, false)?;
                        }
                    }
                }
                self.apply_event(&event);
                self.update_targets()?;
                self.config
                    .region
                    .tell(ShardRegionMsg::CoordinatorDiscoveryEvent { event })
                    .map_err(|error| ActorError::Message(error.reason().to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use kairo_actor::{Address, IgnoreRef};
    use kairo_cluster::{ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Reachability};
    use kairo_testkit::ActorSystemTestKit;

    #[derive(Clone)]
    struct ConnectorTestMessage;

    impl RemoteMessage for ConnectorTestMessage {
        const MANIFEST: &'static str = "kairo.sharding.test.ConnectorMessage";
        const VERSION: u16 = 1;
    }

    fn node(system: &str, port: u16, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
            uid,
        )
    }

    fn member(node: UniqueAddress, data_center: &str) -> Member {
        Member::new(node, vec![format!("dc-{data_center}")]).with_status(MemberStatus::Up)
    }

    #[test]
    fn coordinator_region_lifecycle_excludes_departing_members() {
        assert_eq!(
            coordinator_region_lifecycle(&MemberStatus::Leaving),
            CoordinatorRegionLifecycle::Terminating
        );
        assert_eq!(
            coordinator_region_lifecycle(&MemberStatus::Exiting),
            CoordinatorRegionLifecycle::Terminating
        );
    }

    #[test]
    fn coordinator_region_lifecycle_recovers_only_live_members() {
        assert_eq!(
            coordinator_region_lifecycle(&MemberStatus::Up),
            CoordinatorRegionLifecycle::Eligible
        );
        assert_eq!(
            coordinator_region_lifecycle(&MemberStatus::Down),
            CoordinatorRegionLifecycle::Terminating
        );
        assert_eq!(
            coordinator_region_lifecycle(&MemberStatus::Removed),
            CoordinatorRegionLifecycle::Removed
        );
    }

    #[test]
    fn connector_tracks_reachability_only_within_self_data_center() {
        let self_node = node("connector-dc", 2660, 1);
        let self_member = member(self_node.clone(), "east");
        let same_dc = member(node("connector-dc", 2661, 2), "east");
        let other_dc = member(node("connector-dc", 2662, 3), "west");
        let members = BTreeMap::from([(self_node.ordering_key(), self_member)]);

        assert!(member_is_in_self_data_center(
            &members, &self_node, &same_dc
        ));
        assert!(!member_is_in_self_data_center(
            &members, &self_node, &other_dc
        ));
    }

    #[test]
    fn connector_projects_snapshot_and_events_into_rebalance_availability() {
        let kit = ActorSystemTestKit::new("connector-reachability").unwrap();
        let self_node = node("connector-reachability", 2670, 1);
        let peer_node = node("connector-reachability", 2671, 2);
        let self_member = member(self_node.clone(), "east");
        let peer_member = member(peer_node.clone(), "east");
        let publisher = kit
            .system()
            .spawn(
                "cluster-events",
                Props::new({
                    let self_node = self_node.clone();
                    move || ClusterEventPublisher::new(self_node.clone())
                }),
            )
            .unwrap();
        let unreachable = Gossip::from_members([self_member.clone(), peer_member.clone()])
            .with_reachability(
                Reachability::new().unreachable(self_node.clone(), peer_node.clone()),
            );
        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(unreachable))
            .unwrap();
        let published = kit
            .create_probe::<kairo_cluster::CurrentClusterState>("published-state")
            .unwrap();
        publisher
            .tell(ClusterEventPublisherMsg::SendCurrentState {
                reply_to: published.actor_ref(),
            })
            .unwrap();
        let published = published.expect_msg(Duration::from_secs(1)).unwrap();
        assert_eq!(
            published.unreachable.as_slice(),
            std::slice::from_ref(&peer_member)
        );

        let coordinator = kit
            .create_probe::<ShardCoordinatorMsg<ConnectorTestMessage>>("coordinator")
            .unwrap();
        let region = kit
            .create_probe::<ShardRegionMsg<ConnectorTestMessage>>("region")
            .unwrap();
        let cluster = Cluster::new(publisher.clone());
        let connector = kit
            .system()
            .spawn_system(
                "connector",
                ClusterShardingConnector::props(ClusterShardingConnectorConfig {
                    cluster,
                    self_node: self_node.clone(),
                    coordinator: coordinator.actor_ref(),
                    region: region.actor_ref(),
                    region_wire: ActorRefWireData::new(format!(
                        "{}/system/region",
                        self_node.address
                    ))
                    .unwrap(),
                    coordinator_path: "/system/coordinator".to_string(),
                    region_path: "/system/region".to_string(),
                    registry: Arc::new(Registry::new()),
                    outbound: Arc::new(IgnoreRef::<RemoteEnvelope>::new()),
                }),
            )
            .unwrap();

        let initial = coordinator
            .receive_messages(4, Duration::from_secs(1))
            .unwrap();
        assert!(initial.iter().any(|message| matches!(
            message,
            ShardCoordinatorMsg::MarkRegionUnavailable { region }
                if region.starts_with(&peer_node.address.to_string())
        )));

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([self_member, peer_member]),
            ))
            .unwrap();
        assert!(matches!(
            coordinator.expect_msg(Duration::from_secs(1)).unwrap(),
            ShardCoordinatorMsg::UnmarkRegionUnavailable { region }
                if region.starts_with(&peer_node.address.to_string())
        ));

        kit.system().stop(&connector);
        kit.system().terminate(Duration::from_secs(1)).unwrap();
    }
}
