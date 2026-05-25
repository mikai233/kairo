mod coordinator;
mod service;

pub use coordinator::{
    PatternCoordinator, PatternCoordinatorMsg, PatternObservation, run_ask_pipe_to_self,
    spawn_pattern_coordinator,
};
pub use service::{
    CalculationReply, CalculationService, CalculationServiceMsg, spawn_calculation_service,
};

#[cfg(test)]
mod tests {
    use super::{PatternObservation, run_ask_pipe_to_self};

    #[test]
    fn ask_and_pipe_to_self_example_reports_both_results() {
        let observations = run_ask_pipe_to_self("ask-pipe-example-test", 21)
            .expect("ask and pipe-to-self example should complete");

        assert!(observations.contains(&PatternObservation::AskCompleted {
            input: 21,
            output: 42,
        }));
        assert!(observations.contains(&PatternObservation::PipeCompleted {
            input: 21,
            output: 24,
        }));
    }
}
