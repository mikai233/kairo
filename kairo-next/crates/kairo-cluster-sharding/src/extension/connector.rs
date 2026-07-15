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
                self.tell_coordinator(ShardCoordinatorMsg::UnmarkRegionTerminating {
                    region: region.clone(),
                })?;
                self.tell_coordinator(ShardCoordinatorMsg::UnmarkRegionUnavailable { region })
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
                for member in &state.members {
                    self.sync_coordinator_region_lifecycle(member)?;
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
}
