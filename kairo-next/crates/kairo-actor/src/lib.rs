//! Typed local actor API and runtime primitives.

use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;

pub type ActorResult = Result<(), ActorError>;

#[derive(Debug, thiserror::Error)]
pub enum ActorError {
    #[error("{0}")]
    Message(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Address {
    protocol: String,
    system: String,
    host: Option<String>,
    port: Option<u16>,
}

impl Address {
    pub fn local(system: impl Into<String>) -> Self {
        Self {
            protocol: "kairo".to_string(),
            system: system.into(),
            host: None,
            port: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorPath {
    value: String,
}

impl ActorPath {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl Display for ActorPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}

#[derive(Debug)]
pub struct SendError<M> {
    message: M,
    reason: &'static str,
}

impl<M> SendError<M> {
    pub fn into_message(self) -> M {
        self.message
    }

    pub fn reason(&self) -> &'static str {
        self.reason
    }
}

#[derive(Debug, Clone)]
pub struct ActorRef<M> {
    path: ActorPath,
    _message: PhantomData<fn(M)>,
}

impl<M> ActorRef<M> {
    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    pub fn tell(&self, message: M) -> Result<(), SendError<M>> {
        Err(SendError {
            message,
            reason: "actor runtime is not implemented yet",
        })
    }
}

#[derive(Debug, Clone)]
pub struct AnyActorRef {
    path: ActorPath,
}

impl AnyActorRef {
    pub fn path(&self) -> &ActorPath {
        &self.path
    }
}

pub trait Actor: Send + 'static {
    type Msg: Send + 'static;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult;
}

pub struct Props<A> {
    builder: Box<dyn FnOnce() -> A + Send>,
}

impl<A> Props<A> {
    pub fn new<F>(builder: F) -> Self
    where
        F: FnOnce() -> A + Send + 'static,
    {
        Self {
            builder: Box::new(builder),
        }
    }

    pub fn build(self) -> A {
        (self.builder)()
    }
}

#[derive(Debug)]
pub struct Context<M> {
    myself: ActorRef<M>,
}

impl<M> Context<M> {
    pub fn myself(&self) -> &ActorRef<M> {
        &self.myself
    }
}

#[derive(Debug)]
pub struct ActorSystem {
    name: String,
    address: Address,
}

impl ActorSystem {
    pub fn builder(name: impl Into<String>) -> ActorSystemBuilder {
        ActorSystemBuilder { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn address(&self) -> &Address {
        &self.address
    }

    pub fn spawn<A>(
        &self,
        name: impl AsRef<str>,
        _props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        Ok(ActorRef {
            path: ActorPath::new(format!("kairo://{}/user/{}", self.name, name.as_ref())),
            _message: PhantomData,
        })
    }
}

#[derive(Debug)]
pub struct ActorSystemBuilder {
    name: String,
}

impl ActorSystemBuilder {
    pub fn build(self) -> Result<ActorSystem, ActorError> {
        Ok(ActorSystem {
            address: Address::local(self.name.clone()),
            name: self.name,
        })
    }
}

pub mod prelude {
    pub use crate::{
        Actor, ActorError, ActorPath, ActorRef, ActorResult, ActorSystem, Context, Props,
    };
}
