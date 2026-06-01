use super::*;

impl<D> ReplicatorActor<D>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    pub(super) fn run_delta_propagation_tick(&mut self) -> DeltaPropagationTickReport {
        match &self.delta_loop {
            Some(delta_loop) => delta_loop.run_tick(&mut self.delta_log),
            None => DeltaPropagationTickReport::skipped(self.delta_log.propagation_count()),
        }
    }

    pub(super) fn schedule_delta_propagation_tick(&self, ctx: &Context<ReplicatorActorMsg<D>>) {
        if let Some(interval) = self.delta_tick_interval {
            ctx.schedule_once_self(interval, ReplicatorActorMsg::DeltaPropagationTick);
        }
    }
}
