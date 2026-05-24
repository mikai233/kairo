use std::sync::mpsc;

use kairo::prelude::*;

pub enum CounterCmd {
    Increment,
    Get { reply_to: mpsc::Sender<i64> },
    Stop,
}

pub struct Counter {
    value: i64,
}

impl Counter {
    pub fn new(value: i64) -> Self {
        Self { value }
    }
}

impl Actor for Counter {
    type Msg = CounterCmd;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            CounterCmd::Increment => {
                self.value += 1;
            }
            CounterCmd::Get { reply_to } => {
                reply_to
                    .send(self.value)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            CounterCmd::Stop => ctx.stop(ctx.myself())?,
        }
        Ok(())
    }
}

pub fn spawn_counter(
    system: &ActorSystem,
    name: &str,
    initial_value: i64,
) -> Result<ActorRef<CounterCmd>, ActorError> {
    system.spawn(name, Props::new(move || Counter::new(initial_value)))
}
