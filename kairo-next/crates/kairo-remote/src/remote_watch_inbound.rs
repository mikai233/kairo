use kairo_actor::ActorRef;
use kairo_serialization::{ActorRefWireData, RemoteMessage};

use crate::{
    AddressTerminated, InboundMessage, RemoteDeathWatchCommand, RemoteError, RemoteHeartbeat,
    RemoteHeartbeatAck, RemoteInboundDelivery, RemoteTerminated, Result, UnwatchRemote,
    WatchRemote,
};

#[derive(Clone)]
pub struct RemoteDeathWatchProtocolDelivery {
    watcher: ActorRef<RemoteDeathWatchCommand>,
    local_uid: u64,
}

impl RemoteDeathWatchProtocolDelivery {
    pub fn new(watcher: ActorRef<RemoteDeathWatchCommand>, local_uid: u64) -> Self {
        Self { watcher, local_uid }
    }

    pub fn watcher(&self) -> &ActorRef<RemoteDeathWatchCommand> {
        &self.watcher
    }

    pub fn local_uid(&self) -> u64 {
        self.local_uid
    }

    fn tell(&self, command: RemoteDeathWatchCommand) -> Result<()> {
        self.watcher.tell(command).map_err(|error| {
            RemoteError::Inbound(format!(
                "failed to deliver remote death-watch protocol message: {}",
                error.reason()
            ))
        })
    }
}

impl RemoteInboundDelivery<WatchRemote> for RemoteDeathWatchProtocolDelivery {
    fn deliver(&self, inbound: InboundMessage<WatchRemote>) -> Result<()> {
        self.tell(RemoteDeathWatchCommand::InboundWatch(inbound.message))
    }
}

impl RemoteInboundDelivery<UnwatchRemote> for RemoteDeathWatchProtocolDelivery {
    fn deliver(&self, inbound: InboundMessage<UnwatchRemote>) -> Result<()> {
        self.tell(RemoteDeathWatchCommand::InboundUnwatch(inbound.message))
    }
}

impl RemoteInboundDelivery<RemoteHeartbeat> for RemoteDeathWatchProtocolDelivery {
    fn deliver(&self, inbound: InboundMessage<RemoteHeartbeat>) -> Result<()> {
        let address = sender_address(&inbound.sender, RemoteHeartbeat::MANIFEST)?;
        self.tell(RemoteDeathWatchCommand::Heartbeat {
            address,
            heartbeat: inbound.message,
            local_uid: self.local_uid,
        })
    }
}

impl RemoteInboundDelivery<RemoteHeartbeatAck> for RemoteDeathWatchProtocolDelivery {
    fn deliver(&self, inbound: InboundMessage<RemoteHeartbeatAck>) -> Result<()> {
        let address = sender_address(&inbound.sender, RemoteHeartbeatAck::MANIFEST)?;
        self.tell(RemoteDeathWatchCommand::HeartbeatAck {
            address,
            ack: inbound.message,
        })
    }
}

impl RemoteInboundDelivery<RemoteTerminated> for RemoteDeathWatchProtocolDelivery {
    fn deliver(&self, inbound: InboundMessage<RemoteTerminated>) -> Result<()> {
        self.tell(RemoteDeathWatchCommand::RemoteTerminated(inbound.message))
    }
}

impl RemoteInboundDelivery<AddressTerminated> for RemoteDeathWatchProtocolDelivery {
    fn deliver(&self, inbound: InboundMessage<AddressTerminated>) -> Result<()> {
        self.tell(RemoteDeathWatchCommand::AddressUnreachable {
            address: inbound.message.address,
            uid: inbound.message.uid,
        })
    }
}

fn sender_address(sender: &Option<ActorRefWireData>, manifest: &'static str) -> Result<String> {
    let Some(sender) = sender else {
        return Err(RemoteError::Inbound(format!(
            "remote death-watch `{manifest}` message is missing sender"
        )));
    };
    Ok(wire_address(sender))
}

fn wire_address(wire: &ActorRefWireData) -> String {
    let mut address = format!("{}://{}", wire.protocol(), wire.system());
    if let Some(host) = wire.host() {
        address.push('@');
        address.push_str(host);
        if let Some(port) = wire.port() {
            address.push(':');
            address.push_str(&port.to_string());
        }
    }
    address
}

#[cfg(test)]
mod tests;
