use std::sync::Arc;
use std::time::Duration;

use crate::{
    RemoteAssociationAddress, RemoteAssociationRouteInstaller, RemoteAssociationRouteRegistration,
    RemoteByteSink, RemoteStreamId, TcpHandshakeReadSettings,
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
    handshake_response_required: bool,
    handshake_read_settings: TcpHandshakeReadSettings,
}

impl TcpAssociationDialer {
    pub fn new(installer: RemoteAssociationRouteInstaller) -> Self {
        Self {
            installer,
            connect_timeout: None,
            local_identity: None,
            handshake_response_required: false,
            handshake_read_settings: TcpHandshakeReadSettings::default(),
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

    pub fn with_handshake_response_required(mut self) -> Self {
        self.handshake_response_required = true;
        self
    }

    pub fn with_handshake_read_settings(mut self, settings: TcpHandshakeReadSettings) -> Self {
        self.handshake_read_settings = settings;
        self
    }

    pub fn installer(&self) -> &RemoteAssociationRouteInstaller {
        &self.installer
    }

    pub fn dial(
        &self,
        address: RemoteAssociationAddress,
    ) -> crate::Result<RemoteAssociationRouteRegistration> {
        let mut control = self.connect_lane_stream(&address, RemoteStreamId::Control)?;
        let mut ordinary = self.connect_lane_stream(&address, RemoteStreamId::Ordinary)?;
        let mut large = self.connect_lane_stream(&address, RemoteStreamId::Large)?;
        self.complete_remote_handshake(&address, [&mut control, &mut ordinary, &mut large])?;
        let peer = address.to_string();

        self.installer.insert_stream_pipeline(
            address,
            Arc::new(TcpRemoteByteSink::from_stream(peer.clone(), control)),
            Arc::new(TcpRemoteByteSink::from_stream(peer.clone(), ordinary)),
            Arc::new(TcpRemoteByteSink::from_stream(peer, large)),
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
        let mut control = self.connect_lane_stream(&address, RemoteStreamId::Control)?;
        let mut ordinary = self.connect_lane_stream(&address, RemoteStreamId::Ordinary)?;
        let mut large = self.connect_lane_stream(&address, RemoteStreamId::Large)?;
        self.complete_remote_handshake(&address, [&mut control, &mut ordinary, &mut large])?;

        let control_sink = clone_sink(&address, &control)?;
        let ordinary_sink = clone_sink(&address, &ordinary)?;
        let large_sink = clone_sink(&address, &large)?;
        let registration = self.installer.insert_stream_pipeline(
            address.clone(),
            control_sink,
            ordinary_sink,
            large_sink,
        )?;
        let handle = TcpAssociationReaderHandle::spawn_streams(
            reader,
            vec![
                (address.to_string(), control),
                (address.to_string(), ordinary),
                (address.to_string(), large),
            ],
            registration.lifecycle(),
        );

        Ok((registration, handle))
    }

    fn complete_remote_handshake(
        &self,
        address: &RemoteAssociationAddress,
        streams: [&mut std::net::TcpStream; 3],
    ) -> crate::Result<()> {
        if !self.handshake_response_required {
            return Ok(());
        }
        let local_identity = self.local_identity.as_ref().ok_or_else(|| {
            crate::RemoteError::InvalidReliableSystemDelivery(
                "a handshake response requires a configured local identity".to_string(),
            )
        })?;
        let mut handshakes = Vec::with_capacity(streams.len());
        for stream in streams {
            stream
                .set_read_timeout(Some(self.handshake_read_settings.read_timeout()))
                .map_err(|error| {
                    crate::RemoteError::Inbound(format!(
                        "tcp handshake response timeout setup failed: {error}"
                    ))
                })?;
            handshakes.push(super::read_tcp_association_handshake_with_limit(
                stream,
                self.handshake_read_settings.max_payload_bytes(),
            )?);
            stream.set_read_timeout(None).map_err(|error| {
                crate::RemoteError::Inbound(format!(
                    "tcp handshake response timeout clear failed: {error}"
                ))
            })?;
        }
        let remote_identity = super::validate_tcp_association_handshakes(
            local_identity.address(),
            handshakes.len(),
            &handshakes,
        )?
        .ok_or_else(|| {
            crate::RemoteError::InvalidFrame(
                "tcp association handshake response omitted remote identity".to_string(),
            )
        })?;
        if remote_identity.address() != address {
            return Err(crate::RemoteError::InvalidFrame(format!(
                "tcp association handshake response came from {}, expected {}",
                remote_identity.address(),
                address
            )));
        }
        self.installer
            .complete_handshake(address.clone(), remote_identity.uid())
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
