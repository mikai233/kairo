use super::*;

impl<D> ReplicatorActor<D>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    pub(super) fn run_gossip_tick(&mut self) -> Result<ReplicatorGossipTickReport, ActorError> {
        let (Some(transport), Some(codec)) =
            (self.gossip_transport.clone(), self.gossip_codec.clone())
        else {
            return Ok(ReplicatorGossipTickReport::skipped(
                ReplicatorGossipTickSkipReason::NotConfigured,
            ));
        };
        let Some(target) = self.select_gossip_target() else {
            return Ok(ReplicatorGossipTickReport::skipped(
                ReplicatorGossipTickSkipReason::NoReachableTargets,
            ));
        };

        let entry_count = self.state.entries().count();
        let total_chunks = gossip_total_chunks(entry_count, self.gossip_max_entries);
        let chunk = if total_chunks == 1 {
            0
        } else {
            let chunk = self.gossip_next_chunk % total_chunks;
            self.gossip_next_chunk = (chunk + 1) % total_chunks;
            chunk
        };
        let status = build_gossip_status(
            &self.state,
            codec.as_ref(),
            chunk,
            total_chunks,
            None,
            self.self_system_uid,
        )
        .map_err(|error| ActorError::Message(error.to_string()))?;
        let report = transport.send_status(target.clone(), status.clone());
        Ok(ReplicatorGossipTickReport::sent(target, status, report))
    }

    pub(super) fn receive_gossip_status(
        &mut self,
        from: ReplicaId,
        status: &ReplicatorGossipStatus,
        codec: &dyn CrdtDataCodec<D>,
    ) -> Result<ReplicatorGossipStatusReceiveReport, ActorError> {
        let plan = respond_to_gossip_status(&self.state, status, codec, self.gossip_max_entries)
            .map_err(|error| ActorError::Message(error.to_string()))?;
        let mut transport_report = ReplicatorGossipTransportReport::empty();
        if let Some(transport) = &self.gossip_transport {
            if let Some(gossip) = plan.gossip().cloned() {
                transport_report.extend(transport.send_gossip(from.clone(), gossip));
            }
            if let Some(missing_status) = plan.missing_status().cloned() {
                transport_report.extend(transport.send_status(from, missing_status));
            }
        }
        Ok(ReplicatorGossipStatusReceiveReport::new(
            plan,
            transport_report,
        ))
    }

    pub(super) fn receive_gossip(
        &mut self,
        from: ReplicaId,
        gossip: &ReplicatorGossip,
        codec: &dyn CrdtDataCodec<D>,
    ) -> Result<ReplicatorGossipReceiveReport, ActorError> {
        let apply = apply_gossip(&mut self.state, gossip, codec)
            .map_err(|error| ActorError::Message(error.to_string()))?;
        let mut transport_report = ReplicatorGossipTransportReport::empty();
        if let (Some(transport), Some(reply)) = (&self.gossip_transport, apply.reply().cloned()) {
            transport_report.extend(transport.send_gossip(from, reply));
        }
        Ok(ReplicatorGossipReceiveReport::new(apply, transport_report))
    }

    fn select_gossip_target(&mut self) -> Option<ReplicaId> {
        let reachable = self
            .remote_nodes
            .iter()
            .filter(|node| !self.unreachable_nodes.contains(*node))
            .cloned()
            .collect::<Vec<_>>();
        if reachable.is_empty() {
            return None;
        }
        let index = self.gossip_next_index % reachable.len();
        self.gossip_next_index = (index + 1) % reachable.len();
        Some(reachable[index].clone())
    }

    pub(super) fn schedule_gossip_tick(&self, ctx: &Context<ReplicatorActorMsg<D>>) {
        if let Some(interval) = self.gossip_tick_interval {
            ctx.schedule_once_self(interval, ReplicatorActorMsg::GossipTick);
        }
    }
}

fn gossip_total_chunks(entry_count: usize, max_entries: usize) -> u32 {
    if entry_count <= max_entries {
        1
    } else {
        let chunks = entry_count.div_ceil(max_entries);
        chunks.min(u32::MAX as usize) as u32
    }
}
