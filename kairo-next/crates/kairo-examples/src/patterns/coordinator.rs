use std::error::Error;
use std::sync::mpsc;
use std::time::Duration;

use kairo::actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, AskResult, Context, Props,
};

use super::service::{CalculationReply, CalculationServiceMsg, spawn_calculation_service};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatternObservation {
    AskCompleted { input: i64, output: i64 },
    AskFailed { reason: String },
    PipeCompleted { input: i64, output: i64 },
    PipeFailed { input: i64, reason: String },
}

pub enum PatternCoordinatorMsg {
    Run {
        value: i64,
        observe: mpsc::Sender<PatternObservation>,
    },
    Asked {
        result: AskResult<CalculationReply>,
        observe: mpsc::Sender<PatternObservation>,
    },
    Piped {
        input: i64,
        result: Result<i64, String>,
        observe: mpsc::Sender<PatternObservation>,
    },
    Stop,
}

pub struct PatternCoordinator {
    service: ActorRef<CalculationServiceMsg>,
}

impl PatternCoordinator {
    pub fn new(service: ActorRef<CalculationServiceMsg>) -> Self {
        Self { service }
    }
}

impl Actor for PatternCoordinator {
    type Msg = PatternCoordinatorMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            PatternCoordinatorMsg::Run { value, observe } => {
                let asked_observer = observe.clone();
                ctx.ask(
                    self.service.clone(),
                    Duration::from_secs(1),
                    move |reply_to| CalculationServiceMsg::Double { value, reply_to },
                    move |result| PatternCoordinatorMsg::Asked {
                        result,
                        observe: asked_observer,
                    },
                )?;

                ctx.pipe_to_self(
                    move || {
                        value
                            .checked_add(3)
                            .ok_or_else(|| format!("overflow while adjusting {value}"))
                    },
                    move |result| PatternCoordinatorMsg::Piped {
                        input: value,
                        result,
                        observe,
                    },
                )?;
            }
            PatternCoordinatorMsg::Asked { result, observe } => {
                let observation = match result {
                    Ok(reply) => PatternObservation::AskCompleted {
                        input: reply.input,
                        output: reply.output,
                    },
                    Err(error) => PatternObservation::AskFailed {
                        reason: error.to_string(),
                    },
                };
                observe
                    .send(observation)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            PatternCoordinatorMsg::Piped {
                input,
                result,
                observe,
            } => {
                let observation = match result {
                    Ok(output) => PatternObservation::PipeCompleted { input, output },
                    Err(reason) => PatternObservation::PipeFailed { input, reason },
                };
                observe
                    .send(observation)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            PatternCoordinatorMsg::Stop => ctx.stop(ctx.myself())?,
        }
        Ok(())
    }
}

pub fn spawn_pattern_coordinator(
    system: &ActorSystem,
    name: &str,
    service: ActorRef<CalculationServiceMsg>,
) -> Result<ActorRef<PatternCoordinatorMsg>, ActorError> {
    system.spawn(
        name,
        Props::new(move || PatternCoordinator::new(service.clone())),
    )
}

pub fn run_ask_pipe_to_self(
    system_name: &str,
    value: i64,
) -> Result<Vec<PatternObservation>, Box<dyn Error>> {
    let system = ActorSystem::builder(system_name).build()?;
    let service = spawn_calculation_service(&system, "calculation-service")?;
    let coordinator = spawn_pattern_coordinator(&system, "pattern-coordinator", service.clone())?;
    let (observed_tx, observed_rx) = mpsc::channel();

    coordinator.tell(PatternCoordinatorMsg::Run {
        value,
        observe: observed_tx,
    })?;

    let observations = vec![
        observed_rx.recv_timeout(Duration::from_secs(1))?,
        observed_rx.recv_timeout(Duration::from_secs(1))?,
    ];

    coordinator.tell(PatternCoordinatorMsg::Stop)?;
    service.tell(CalculationServiceMsg::Stop)?;
    if !coordinator.wait_for_stop(Duration::from_secs(1)) {
        return Err("pattern coordinator did not stop within one second".into());
    }
    if !service.wait_for_stop(Duration::from_secs(1)) {
        return Err("calculation service did not stop within one second".into());
    }
    system.terminate(Duration::from_secs(1))?;

    Ok(observations)
}
