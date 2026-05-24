use std::collections::VecDeque;
use std::fmt::{self, Display, Formatter};

use kairo_actor::{Actor, ActorPath, ActorRef, ActorResult, Context, Props};

const MAX_BUFFER_SIZE: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SingletonProxySettings {
    buffer_size: usize,
}

impl SingletonProxySettings {
    pub fn new(buffer_size: usize) -> Result<Self, SingletonProxySettingsError> {
        if buffer_size > MAX_BUFFER_SIZE {
            return Err(SingletonProxySettingsError::BufferTooLarge {
                buffer_size,
                max_buffer_size: MAX_BUFFER_SIZE,
            });
        }
        Ok(Self { buffer_size })
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

impl Default for SingletonProxySettings {
    fn default() -> Self {
        Self { buffer_size: 1000 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingletonProxySettingsError {
    BufferTooLarge {
        buffer_size: usize,
        max_buffer_size: usize,
    },
}

impl Display for SingletonProxySettingsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooLarge {
                buffer_size,
                max_buffer_size,
            } => write!(
                f,
                "singleton proxy buffer size {buffer_size} exceeds maximum {max_buffer_size}"
            ),
        }
    }
}

impl std::error::Error for SingletonProxySettingsError {}

pub struct SingletonProxyActor<M>
where
    M: Send + 'static,
{
    settings: SingletonProxySettings,
    singleton: Option<ActorRef<M>>,
    buffer: VecDeque<M>,
    dropped_messages: u64,
}

impl<M> SingletonProxyActor<M>
where
    M: Send + 'static,
{
    pub fn new(settings: SingletonProxySettings) -> Self {
        Self {
            settings,
            singleton: None,
            buffer: VecDeque::new(),
            dropped_messages: 0,
        }
    }

    pub fn props(settings: SingletonProxySettings) -> Props<Self> {
        Props::new(move || Self::new(settings))
    }

    fn set_singleton(
        &mut self,
        ctx: &mut Context<SingletonProxyMsg<M>>,
        singleton: ActorRef<M>,
    ) -> ActorResult {
        if let Some(current) = &self.singleton {
            if current.path() == singleton.path() {
                return Ok(());
            }
            ctx.unwatch(current);
        }

        let singleton_path = singleton.path().clone();
        ctx.watch_with(
            &singleton,
            SingletonProxyMsg::SingletonTerminated {
                path: singleton_path,
            },
        )?;
        self.singleton = Some(singleton);
        self.flush_buffer();
        Ok(())
    }

    fn clear_singleton(&mut self, path: &ActorPath) {
        if self
            .singleton
            .as_ref()
            .is_some_and(|singleton| singleton.path() == path)
        {
            self.singleton = None;
        }
    }

    fn route(&mut self, message: M) {
        if let Some(singleton) = &self.singleton {
            let _ = singleton.tell(message);
        } else {
            self.buffer(message);
        }
    }

    fn buffer(&mut self, message: M) {
        if self.settings.buffer_size == 0 {
            self.dropped_messages = self.dropped_messages.saturating_add(1);
            return;
        }

        if self.buffer.len() == self.settings.buffer_size {
            self.buffer.pop_front();
            self.dropped_messages = self.dropped_messages.saturating_add(1);
        }
        self.buffer.push_back(message);
    }

    fn flush_buffer(&mut self) {
        let Some(singleton) = &self.singleton else {
            return;
        };

        while let Some(message) = self.buffer.pop_front() {
            let _ = singleton.tell(message);
        }
    }
}

pub enum SingletonProxyMsg<M: Send + 'static> {
    Route(M),
    IdentifySingleton {
        singleton: ActorRef<M>,
    },
    SingletonTerminated {
        path: ActorPath,
    },
    GetState {
        reply_to: ActorRef<SingletonProxySnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonProxySnapshot {
    pub singleton_path: Option<ActorPath>,
    pub buffered_messages: usize,
    pub dropped_messages: u64,
}

impl<M> Actor for SingletonProxyActor<M>
where
    M: Send + 'static,
{
    type Msg = SingletonProxyMsg<M>;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            SingletonProxyMsg::Route(message) => self.route(message),
            SingletonProxyMsg::IdentifySingleton { singleton } => {
                self.set_singleton(ctx, singleton)?;
            }
            SingletonProxyMsg::SingletonTerminated { path } => {
                self.clear_singleton(&path);
            }
            SingletonProxyMsg::GetState { reply_to } => {
                let _ = reply_to.tell(SingletonProxySnapshot {
                    singleton_path: self
                        .singleton
                        .as_ref()
                        .map(|singleton| singleton.path().clone()),
                    buffered_messages: self.buffer.len(),
                    dropped_messages: self.dropped_messages,
                });
            }
        }
        Ok(())
    }
}
