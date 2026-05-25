use std::sync::Arc;
use std::time::Duration;

use crate::{
    RemoteAssociationAddress, RemoteAssociationRouteInstaller, RemoteAssociationRouteRegistration,
    RemoteStreamId,
};

use super::{TcpAssociationIdentity, TcpRemoteByteSink};

#[derive(Clone)]
pub struct TcpAssociationDialer {
    installer: RemoteAssociationRouteInstaller,
    connect_timeout: Option<Duration>,
    local_identity: Option<TcpAssociationIdentity>,
}

impl TcpAssociationDialer {
    pub fn new(installer: RemoteAssociationRouteInstaller) -> Self {
        Self {
            installer,
            connect_timeout: None,
            local_identity: None,
        }
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    pub fn with_local_address(mut self, local_address: RemoteAssociationAddress) -> Self {
        self.local_identity = Some(TcpAssociationIdentity::new(local_address, 0));
        self
    }

    pub fn with_local_identity(
        mut self,
        local_address: RemoteAssociationAddress,
        local_uid: u64,
    ) -> Self {
        self.local_identity = Some(TcpAssociationIdentity::new(local_address, local_uid));
        self
    }

    pub fn installer(&self) -> &RemoteAssociationRouteInstaller {
        &self.installer
    }

    pub fn dial(
        &self,
        address: RemoteAssociationAddress,
    ) -> crate::Result<RemoteAssociationRouteRegistration> {
        let control = self.connect_lane(&address, RemoteStreamId::Control)?;
        let ordinary = self.connect_lane(&address, RemoteStreamId::Ordinary)?;
        let large = self.connect_lane(&address, RemoteStreamId::Large)?;

        Ok(self.installer.insert_stream_pipeline(
            address,
            Arc::new(control),
            Arc::new(ordinary),
            Arc::new(large),
        ))
    }

    fn connect_lane(
        &self,
        address: &RemoteAssociationAddress,
        stream_id: RemoteStreamId,
    ) -> crate::Result<TcpRemoteByteSink> {
        match &self.local_identity {
            Some(local_identity) => TcpRemoteByteSink::connect_handshaken(
                address,
                local_identity,
                stream_id,
                self.connect_timeout,
            ),
            None => TcpRemoteByteSink::connect(address, self.connect_timeout),
        }
    }
}
