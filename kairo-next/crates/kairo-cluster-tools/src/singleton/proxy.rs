#![deny(missing_docs)]

use std::collections::VecDeque;
use std::fmt::{self, Display, Formatter};

use kairo_actor::{Actor, ActorPath, ActorRef, ActorResult, Context, Props};
use kairo_cluster::UniqueAddress;

use super::{proxy_routes::SingletonProxyRoutes, proxy_target::SingletonProxyTarget};
use crate::singleton::{SingletonOldestChange, SingletonOldestObservation};

const MAX_BUFFER_SIZE: usize = 10_000;

/// Buffering policy for a [`SingletonProxyActor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SingletonProxySettings {
    buffer_size: usize,
}

impl SingletonProxySettings {
    /// Creates settings with space for at most `buffer_size` pending messages.
    ///
    /// A size of zero disables buffering. Values above 10,000 are rejected to
    /// preserve the bounded proxy contract inherited from Pekko.
    pub fn new(buffer_size: usize) -> Result<Self, SingletonProxySettingsError> {
        if buffer_size > MAX_BUFFER_SIZE {
            return Err(SingletonProxySettingsError::BufferTooLarge {
                buffer_size,
                max_buffer_size: MAX_BUFFER_SIZE,
            });
        }
        Ok(Self { buffer_size })
    }

    /// Returns the maximum number of messages buffered without a target.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

impl Default for SingletonProxySettings {
    fn default() -> Self {
        Self { buffer_size: 1000 }
    }
}

/// Invalid singleton-proxy settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingletonProxySettingsError {
    /// The requested pending-message buffer exceeds the supported maximum.
    BufferTooLarge {
        /// Requested buffer capacity.
        buffer_size: usize,
        /// Maximum supported buffer capacity.
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

/// Typed singleton proxy with bounded buffering and membership-driven routing.
///
/// Messages are forwarded to the target registered for the current oldest
/// eligible member. While that target is unknown, messages are buffered in
/// arrival order; a full buffer drops its oldest entry before accepting a new
/// one. Target sends are best effort and failed sends are not re-buffered.
/// Local targets are watched, while remote or custom targets must be refreshed
/// by their owning connector.
pub struct SingletonProxyActor<M>
where
    M: Send + 'static,
{
    settings: SingletonProxySettings,
    routes: SingletonProxyRoutes<M>,
    singleton: Option<SingletonProxyTarget<M>>,
    buffer: VecDeque<M>,
    dropped_messages: u64,
}

impl<M> SingletonProxyActor<M>
where
    M: Send + 'static,
{
    /// Creates a proxy with no routes, target, or buffered messages.
    pub fn new(settings: SingletonProxySettings) -> Self {
        Self {
            settings,
            routes: SingletonProxyRoutes::new(),
            singleton: None,
            buffer: VecDeque::new(),
            dropped_messages: 0,
        }
    }

    /// Creates actor properties for a new proxy with `settings`.
    pub fn props(settings: SingletonProxySettings) -> Props<Self> {
        Props::new(move || Self::new(settings))
    }

    fn set_singleton(
        &mut self,
        ctx: &mut Context<SingletonProxyMsg<M>>,
        singleton: SingletonProxyTarget<M>,
    ) -> ActorResult {
        if let Some(current) = &self.singleton {
            if current.path() == singleton.path() {
                return Ok(());
            }
            if let Some(current) = current.watchable() {
                ctx.unwatch(current);
            }
        }

        if let Some(watchable) = singleton.watchable() {
            let singleton_path = watchable.path().clone();
            ctx.watch_with(
                watchable,
                SingletonProxyMsg::SingletonTerminated {
                    path: singleton_path,
                },
            )?;
        }
        self.singleton = Some(singleton);
        self.flush_buffer();
        Ok(())
    }

    fn clear_identified_singleton(&mut self, ctx: &mut Context<SingletonProxyMsg<M>>) {
        if let Some(current) = self.singleton.take()
            && let Some(current) = current.watchable()
        {
            ctx.unwatch(current);
        }
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

    fn identify_current_oldest(&mut self, ctx: &mut Context<SingletonProxyMsg<M>>) -> ActorResult {
        if let Some(singleton) = self.routes.current_target() {
            self.set_singleton(ctx, singleton)?;
        }
        Ok(())
    }

    fn apply_oldest_change(
        &mut self,
        ctx: &mut Context<SingletonProxyMsg<M>>,
        changed: bool,
    ) -> ActorResult {
        if changed {
            self.clear_identified_singleton(ctx);
        }
        self.identify_current_oldest(ctx)
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

/// Commands accepted by [`SingletonProxyActor`].
pub enum SingletonProxyMsg<M: Send + 'static> {
    /// Routes one business message or buffers it while the target is unknown.
    Route(M),
    /// Registers a watchable local singleton route for a member incarnation.
    RegisterRoute {
        /// Exact member incarnation that owns the route.
        node: UniqueAddress,
        /// Typed local singleton reference.
        singleton: ActorRef<M>,
    },
    /// Registers a local, remote, or custom target for a member incarnation.
    RegisterTarget {
        /// Exact member incarnation that owns the route.
        node: UniqueAddress,
        /// Typed delivery target.
        singleton: SingletonProxyTarget<M>,
    },
    /// Removes the route owned by one exact member incarnation.
    RemoveRoute {
        /// Exact member incarnation whose route is obsolete.
        node: UniqueAddress,
    },
    /// Applies the initial role-scoped oldest-member observation.
    ApplyInitialObservation {
        /// Initial ordered ownership observation.
        observation: SingletonOldestObservation,
    },
    /// Applies a later role-scoped oldest-member change.
    ///
    /// Self-removal stops the proxy. Self-downing deliberately leaves it alive
    /// until removal, matching Pekko's membership-event lifecycle.
    ApplyOldestChange {
        /// Membership-derived ownership change.
        change: SingletonOldestChange,
    },
    /// Installs a directly identified watchable local singleton.
    IdentifySingleton {
        /// Typed local singleton reference.
        singleton: ActorRef<M>,
    },
    /// Installs a directly identified local, remote, or custom target.
    IdentifyTarget {
        /// Typed delivery target.
        singleton: SingletonProxyTarget<M>,
    },
    /// Reports termination of a previously watched local target.
    SingletonTerminated {
        /// Exact terminated actor path, including its incarnation.
        path: ActorPath,
    },
    /// Requests an immutable proxy state snapshot.
    GetState {
        /// Recipient for the snapshot.
        reply_to: ActorRef<SingletonProxySnapshot>,
    },
}

/// Observable routing and buffering state of a [`SingletonProxyActor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonProxySnapshot {
    /// Current oldest eligible member incarnation, if known.
    pub current_oldest: Option<UniqueAddress>,
    /// Number of member-incarnation routes currently registered.
    pub registered_routes: usize,
    /// Current delivery target path, if identified.
    pub singleton_path: Option<ActorPath>,
    /// Number of messages waiting for target identification.
    pub buffered_messages: usize,
    /// Cumulative messages dropped because buffering was disabled or full.
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
            SingletonProxyMsg::RegisterRoute { node, singleton } => {
                if self
                    .routes
                    .register_route(node, SingletonProxyTarget::local(singleton))
                {
                    self.identify_current_oldest(ctx)?;
                }
            }
            SingletonProxyMsg::RegisterTarget { node, singleton } => {
                if self.routes.register_route(node, singleton) {
                    self.identify_current_oldest(ctx)?;
                }
            }
            SingletonProxyMsg::RemoveRoute { node } => {
                if self.routes.remove_route(&node) {
                    self.clear_identified_singleton(ctx);
                }
            }
            SingletonProxyMsg::ApplyInitialObservation { observation } => {
                let changed = self.routes.apply_initial_observation(observation);
                self.apply_oldest_change(ctx, changed)?;
            }
            SingletonProxyMsg::ApplyOldestChange { change } => {
                if matches!(change, SingletonOldestChange::SelfRemoved) {
                    ctx.stop(ctx.myself())?;
                } else {
                    let changed = self.routes.apply_oldest_change(change);
                    self.apply_oldest_change(ctx, changed)?;
                }
            }
            SingletonProxyMsg::IdentifySingleton { singleton } => {
                self.set_singleton(ctx, SingletonProxyTarget::local(singleton))?;
            }
            SingletonProxyMsg::IdentifyTarget { singleton } => {
                self.set_singleton(ctx, singleton)?;
            }
            SingletonProxyMsg::SingletonTerminated { path } => {
                self.clear_singleton(&path);
            }
            SingletonProxyMsg::GetState { reply_to } => {
                let _ = reply_to.tell(SingletonProxySnapshot {
                    current_oldest: self.routes.current_oldest().cloned(),
                    registered_routes: self.routes.registered_routes(),
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
