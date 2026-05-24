use std::sync::mpsc;
use std::time::Duration;

use kairo::prelude::*;

enum CounterCmd {
    Increment,
    Get { reply_to: mpsc::Sender<i64> },
    Stop,
}

struct Counter {
    value: i64,
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder("local-counter").build()?;
    let counter = system.spawn("counter", Props::new(|| Counter { value: 0 }))?;
    let (reply_to, replies) = mpsc::channel();

    counter.tell(CounterCmd::Increment)?;
    counter.tell(CounterCmd::Increment)?;
    counter.tell(CounterCmd::Get { reply_to })?;

    let value = replies.recv_timeout(Duration::from_secs(1))?;
    println!("counter value: {value}");

    counter.tell(CounterCmd::Stop)?;
    if !counter.wait_for_stop(Duration::from_secs(1)) {
        return Err("counter did not stop within one second".into());
    }
    system.terminate(Duration::from_secs(1))?;
    Ok(())
}
