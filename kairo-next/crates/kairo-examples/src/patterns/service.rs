use kairo::actor::{Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, Props};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalculationReply {
    pub input: i64,
    pub output: i64,
}

pub enum CalculationServiceMsg {
    Double {
        value: i64,
        reply_to: ActorRef<CalculationReply>,
    },
    Stop,
}

pub struct CalculationService;

impl Actor for CalculationService {
    type Msg = CalculationServiceMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            CalculationServiceMsg::Double { value, reply_to } => {
                reply_to
                    .tell(CalculationReply {
                        input: value,
                        output: value * 2,
                    })
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            CalculationServiceMsg::Stop => ctx.stop(ctx.myself())?,
        }
        Ok(())
    }
}

pub fn spawn_calculation_service(
    system: &ActorSystem,
    name: &str,
) -> Result<ActorRef<CalculationServiceMsg>, ActorError> {
    system.spawn(name, Props::new(|| CalculationService))
}
