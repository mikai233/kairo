use std::sync::Arc;
use std::time::Duration;

use crate::{
    RemoteAssociationAddress, RemoteAssociationRouteInstaller, RemoteAssociationRouteRegistration,
};

use super::TcpRemoteByteSink;

#[derive(Clone)]
pub struct TcpAssociationDialer {
    installer: RemoteAssociationRouteInstaller,
    connect_timeout: Option<Duration>,
}

impl TcpAssociationDialer {
    pub fn new(installer: RemoteAssociationRouteInstaller) -> Self {
        Self {
            installer,
            connect_timeout: None,
        }
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    pub fn installer(&self) -> &RemoteAssociationRouteInstaller {
        &self.installer
    }

    pub fn dial(
        &self,
        address: RemoteAssociationAddress,
    ) -> crate::Result<RemoteAssociationRouteRegistration> {
        let control = TcpRemoteByteSink::connect(&address, self.connect_timeout)?;
        let ordinary = TcpRemoteByteSink::connect(&address, self.connect_timeout)?;
        let large = TcpRemoteByteSink::connect(&address, self.connect_timeout)?;

        Ok(self.installer.insert_stream_pipeline(
            address,
            Arc::new(control),
            Arc::new(ordinary),
            Arc::new(large),
        ))
    }
}
