use std::sync::Arc;
use std::time::Duration;

use crate::{
    RemoteAssociationAddress, RemoteAssociationRouteInstaller, RemoteAssociationRouteRegistration,
    RemoteByteSink, RemoteStreamId,
};

use super::{
    TcpAssociationIdentity, TcpAssociationReaderHandle, TcpAssociationStreamReader,
    TcpRemoteByteSink,
};

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

        self.installer.insert_stream_pipeline(
            address,
            Arc::new(control),
            Arc::new(ordinary),
            Arc::new(large),
        )
    }

    pub fn dial_with_reader(
        &self,
        address: RemoteAssociationAddress,
        reader: TcpAssociationStreamReader,
    ) -> crate::Result<(
        RemoteAssociationRouteRegistration,
        TcpAssociationReaderHandle,
    )> {
        let control = self.connect_lane_stream(&address, RemoteStreamId::Control)?;
        let ordinary = self.connect_lane_stream(&address, RemoteStreamId::Ordinary)?;
        let large = self.connect_lane_stream(&address, RemoteStreamId::Large)?;

        let control_sink = clone_sink(&address, &control)?;
        let ordinary_sink = clone_sink(&address, &ordinary)?;
        let large_sink = clone_sink(&address, &large)?;
        let handle = TcpAssociationReaderHandle::spawn_streams(
            reader,
            vec![
                (address.to_string(), control),
                (address.to_string(), ordinary),
                (address.to_string(), large),
            ],
        );
        let registration = self.installer.insert_stream_pipeline(
            address,
            control_sink,
            ordinary_sink,
            large_sink,
        )?;

        Ok((registration, handle))
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

    fn connect_lane_stream(
        &self,
        address: &RemoteAssociationAddress,
        stream_id: RemoteStreamId,
    ) -> crate::Result<std::net::TcpStream> {
        match &self.local_identity {
            Some(local_identity) => TcpRemoteByteSink::connect_handshaken_stream(
                address,
                local_identity,
                stream_id,
                self.connect_timeout,
            ),
            None => TcpRemoteByteSink::connect_stream(address, self.connect_timeout),
        }
    }
}

fn clone_sink(
    address: &RemoteAssociationAddress,
    stream: &std::net::TcpStream,
) -> crate::Result<Arc<dyn RemoteByteSink>> {
    let stream = stream.try_clone().map_err(|error| {
        crate::RemoteError::Outbound(format!("tcp stream clone failed: {error}"))
    })?;
    Ok(Arc::new(TcpRemoteByteSink::from_stream(
        address.to_string(),
        stream,
    )))
}
