use std::sync::mpsc;

use kairo::actor::{Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, Props};

pub struct OneShotReply<M> {
    reply_to: Option<mpsc::Sender<M>>,
}

impl<M> OneShotReply<M> {
    pub fn new(reply_to: mpsc::Sender<M>) -> Self {
        Self {
            reply_to: Some(reply_to),
        }
    }
}

impl<M> Actor for OneShotReply<M>
where
    M: Send + 'static,
{
    type Msg = M;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        let reply_to = self
            .reply_to
            .take()
            .ok_or_else(|| ActorError::Message("one-shot reply actor already used".to_string()))?;
        reply_to
            .send(msg)
            .map_err(|error| ActorError::Message(error.to_string()))?;
        ctx.stop(ctx.myself())
    }
}

pub fn spawn_one_shot_reply<M>(
    system: &ActorSystem,
    name: impl AsRef<str>,
) -> Result<(ActorRef<M>, mpsc::Receiver<M>), ActorError>
where
    M: Send + 'static,
{
    let (reply_to, replies) = mpsc::channel();
    let actor_ref = system.spawn(
        name.as_ref(),
        Props::new(move || OneShotReply::new(reply_to)),
    )?;
    Ok((actor_ref, replies))
}
