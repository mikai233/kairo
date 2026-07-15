use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::tcp::{
    TcpAssociationHandshake, TcpAssociationIdentity, TcpAssociationReaderSupervisor,
    TcpHandshakeReadSettings, TcpRemoteByteSink, encode_tcp_association_handshake,
    read_tcp_association_handshake_with_limit, validate_tcp_association_handshakes,
};
use crate::{
    RemoteAssociationAddress, RemoteAssociationRegistry, RemoteAssociationRouteInstaller,
    RemoteAssociationRouteRegistration, RemoteByteSink, RemoteError, RemoteFrameHandler,
    RemoteStreamId, Result,
};

use super::DEFAULT_EXPECTED_LANE_STREAMS;
use super::accepted::{TcpAcceptedAssociation, TcpAcceptedStream};
use super::error::{missing_lane_error, tcp_inbound_failure};
use super::reports::{TcpAssociationListenerReport, TcpAssociationReadReport};
use super::stream_reader::TcpAssociationStreamReader;

const DEFAULT_ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const STOP_READER_JOIN_TIMEOUT: Duration = Duration::from_millis(50);

pub struct TcpAssociationListener {
    listener: TcpListener,
    reader: TcpAssociationStreamReader,
    handler_factory: Option<Arc<dyn TcpAssociationFrameHandlerFactory>>,
    expected_streams: usize,
    accept_poll_interval: Duration,
    local_address: Option<RemoteAssociationAddress>,
    local_identity: Option<TcpAssociationIdentity>,
    handshake_read_settings: TcpHandshakeReadSettings,
    association_registry: Option<RemoteAssociationRegistry>,
    route_installer: Option<RemoteAssociationRouteInstaller>,
}

pub trait TcpAssociationFrameHandlerFactory: Send + Sync + 'static {
    fn handler_for(
        &self,
        remote_identity: Option<&TcpAssociationIdentity>,
    ) -> Arc<dyn RemoteFrameHandler>;
}

impl<F> TcpAssociationFrameHandlerFactory for F
where
    F: Fn(Option<&TcpAssociationIdentity>) -> Arc<dyn RemoteFrameHandler> + Send + Sync + 'static,
{
    fn handler_for(
        &self,
        remote_identity: Option<&TcpAssociationIdentity>,
    ) -> Arc<dyn RemoteFrameHandler> {
        self(remote_identity)
    }
}

impl TcpAssociationListener {
    pub fn bind(address: impl ToSocketAddrs, handler: Arc<dyn RemoteFrameHandler>) -> Result<Self> {
        let listener = TcpListener::bind(address)
            .map_err(|error| RemoteError::Inbound(format!("tcp bind failed: {error}")))?;
        Ok(Self::from_listener(listener, handler))
    }

    pub fn from_listener(listener: TcpListener, handler: Arc<dyn RemoteFrameHandler>) -> Self {
        Self {
            listener,
            reader: TcpAssociationStreamReader::new(handler),
            handler_factory: None,
            expected_streams: DEFAULT_EXPECTED_LANE_STREAMS,
            accept_poll_interval: DEFAULT_ACCEPT_POLL_INTERVAL,
            local_address: None,
            local_identity: None,
            handshake_read_settings: TcpHandshakeReadSettings::default(),
            association_registry: None,
            route_installer: None,
        }
    }

    pub fn with_expected_streams(mut self, expected_streams: usize) -> Self {
        self.expected_streams = expected_streams.max(1);
        self
    }

    pub fn with_local_address(mut self, local_address: RemoteAssociationAddress) -> Self {
        self.local_address = Some(local_address);
        self
    }

    pub fn with_local_identity(
        mut self,
        local_address: RemoteAssociationAddress,
        uid: u64,
    ) -> Self {
        self.local_identity = Some(TcpAssociationIdentity::new(local_address.clone(), uid));
        self.local_address = Some(local_address);
        self
    }

    pub fn with_handshake_read_settings(mut self, settings: TcpHandshakeReadSettings) -> Self {
        self.handshake_read_settings = settings;
        self
    }

    pub fn with_association_registry(mut self, registry: RemoteAssociationRegistry) -> Self {
        self.association_registry = Some(registry);
        self
    }

    pub fn with_route_installer(mut self, installer: RemoteAssociationRouteInstaller) -> Self {
        self.route_installer = Some(installer);
        self
    }

    pub fn with_handler_factory(
        mut self,
        handler_factory: Arc<dyn TcpAssociationFrameHandlerFactory>,
    ) -> Self {
        self.handler_factory = Some(handler_factory);
        self
    }

    pub fn with_read_chunk_len(mut self, read_chunk_len: usize) -> Self {
        self.reader = self.reader.with_read_chunk_len(read_chunk_len);
        self
    }

    pub fn with_accept_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.accept_poll_interval = if poll_interval.is_zero() {
            DEFAULT_ACCEPT_POLL_INTERVAL
        } else {
            poll_interval
        };
        self
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|error| RemoteError::Inbound(format!("tcp local address failed: {error}")))
    }

    pub fn accept_association(&self) -> Result<TcpAcceptedAssociation> {
        let mut streams = Vec::with_capacity(self.expected_streams);
        let mut handshakes = Vec::with_capacity(self.expected_streams);
        for _ in 0..self.expected_streams {
            let (mut stream, peer) = self
                .listener
                .accept()
                .map_err(|error| RemoteError::Inbound(format!("tcp accept failed: {error}")))?;
            stream
                .set_nodelay(true)
                .map_err(|error| tcp_inbound_failure(&peer.to_string(), error))?;
            let stream_id = self.read_handshake(&mut stream, &mut handshakes)?;
            streams.push(TcpAcceptedStream {
                peer,
                stream_id,
                stream,
            });
        }
        let remote_identity = self.validate_handshakes(&handshakes)?;
        self.write_handshake_responses(&remote_identity, &mut streams)?;
        self.register_remote_identity(&remote_identity)?;
        let route_registration = self.install_reverse_route(&remote_identity, &streams)?;
        Ok(TcpAcceptedAssociation {
            reader: self.reader_for(&remote_identity),
            remote_identity,
            route_registration,
            streams,
        })
    }

    pub fn spawn_accept_loop(self) -> Result<TcpAssociationListenerHandle> {
        let stop = Arc::new(AtomicBool::new(false));
        self.listener
            .set_nonblocking(true)
            .map_err(|error| RemoteError::Inbound(format!("tcp nonblocking failed: {error}")))?;
        let thread_stop = Arc::clone(&stop);
        let join = thread::spawn(move || self.run_accept_loop(thread_stop));
        Ok(TcpAssociationListenerHandle { stop, join })
    }

    fn run_accept_loop(self, stop: Arc<AtomicBool>) -> Result<TcpAssociationListenerReport> {
        let mut accepted_associations = 0_usize;
        let mut remote_identities = Vec::new();
        let mut reader_handles = Vec::new();
        let mut reader_supervisor = TcpAssociationReaderSupervisor::default();
        let mut first_error = None;

        while !stop.load(Ordering::SeqCst) {
            match self.try_accept_association(&stop) {
                Ok(Some(accepted)) => {
                    accepted_associations += 1;
                    if let Some(identity) = accepted.remote_identity().cloned() {
                        remote_identities.push(identity);
                    }
                    reader_handles.push(accepted.spawn_lane_readers());
                }
                Ok(None) => thread::sleep(self.accept_poll_interval),
                Err(error) => {
                    first_error.get_or_insert(error);
                    break;
                }
            }
        }

        let stopped = stop.load(Ordering::SeqCst);
        if stopped {
            reader_supervisor.stop();
        }

        let mut read = TcpAssociationReadReport::default();
        let mut supervision = Vec::new();
        let stop_reader_deadline = Instant::now() + STOP_READER_JOIN_TIMEOUT;
        for handle in reader_handles {
            let report = if stopped {
                handle
                    .join_with_supervisor_until(&mut reader_supervisor, stop_reader_deadline)
                    .unwrap_or_default()
            } else {
                handle.join_with_supervisor(&mut reader_supervisor)
            };
            read.streams += report.read.streams;
            read.frames += report.read.frames;
            supervision.extend(report.supervision);
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(TcpAssociationListenerReport {
                accepted_associations,
                remote_identities,
                read,
                supervision,
            })
        }
    }

    fn try_accept_association(&self, stop: &AtomicBool) -> Result<Option<TcpAcceptedAssociation>> {
        let mut streams = Vec::with_capacity(self.expected_streams);
        let mut handshakes = Vec::with_capacity(self.expected_streams);
        while streams.len() < self.expected_streams {
            match self.listener.accept() {
                Ok((stream, peer)) => {
                    stream
                        .set_nonblocking(false)
                        .map_err(|error| tcp_inbound_failure(&peer.to_string(), error))?;
                    stream
                        .set_nodelay(true)
                        .map_err(|error| tcp_inbound_failure(&peer.to_string(), error))?;
                    let mut stream = stream;
                    let stream_id = self.read_handshake(&mut stream, &mut handshakes)?;
                    streams.push(TcpAcceptedStream {
                        peer,
                        stream_id,
                        stream,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if streams.is_empty() {
                        return Ok(None);
                    }
                    if stop.load(Ordering::SeqCst) {
                        return Err(RemoteError::Inbound(
                            "tcp accept stopped before all association lane streams arrived"
                                .to_string(),
                        ));
                    }
                    thread::sleep(self.accept_poll_interval);
                }
                Err(error) => {
                    return Err(RemoteError::Inbound(format!("tcp accept failed: {error}")));
                }
            }
        }
        let remote_identity = self.validate_handshakes(&handshakes)?;
        self.write_handshake_responses(&remote_identity, &mut streams)?;
        self.register_remote_identity(&remote_identity)?;
        let route_registration = self.install_reverse_route(&remote_identity, &streams)?;
        Ok(Some(TcpAcceptedAssociation {
            reader: self.reader_for(&remote_identity),
            remote_identity,
            route_registration,
            streams,
        }))
    }

    fn read_handshake(
        &self,
        stream: &mut TcpStream,
        handshakes: &mut Vec<TcpAssociationHandshake>,
    ) -> Result<Option<RemoteStreamId>> {
        if self.local_address.is_some() {
            stream
                .set_read_timeout(Some(self.handshake_read_settings.read_timeout()))
                .map_err(|error| {
                    RemoteError::Inbound(format!(
                        "tcp handshake read timeout setup failed: {error}"
                    ))
                })?;
            let handshake = read_tcp_association_handshake_with_limit(
                stream,
                self.handshake_read_settings.max_payload_bytes(),
            )?;
            stream.set_read_timeout(None).map_err(|error| {
                RemoteError::Inbound(format!("tcp handshake read timeout clear failed: {error}"))
            })?;
            let stream_id = handshake.stream_id();
            handshakes.push(handshake);
            return Ok(Some(stream_id));
        }
        Ok(None)
    }

    fn validate_handshakes(
        &self,
        handshakes: &[TcpAssociationHandshake],
    ) -> Result<Option<TcpAssociationIdentity>> {
        match &self.local_address {
            Some(local_address) => validate_tcp_association_handshakes(
                local_address,
                self.expected_streams,
                handshakes,
            ),
            None => Ok(None),
        }
    }

    fn register_remote_identity(&self, identity: &Option<TcpAssociationIdentity>) -> Result<()> {
        let Some(identity) = identity else {
            return Ok(());
        };
        if let Some(registry) = &self.association_registry {
            registry.complete_handshake(identity.address().clone(), identity.uid())?;
        }
        Ok(())
    }

    fn write_handshake_responses(
        &self,
        remote_identity: &Option<TcpAssociationIdentity>,
        streams: &mut [TcpAcceptedStream],
    ) -> Result<()> {
        let (Some(local_identity), Some(remote_identity)) = (&self.local_identity, remote_identity)
        else {
            return Ok(());
        };
        for stream in streams {
            let stream_id = stream
                .stream_id
                .ok_or_else(|| missing_lane_error(RemoteStreamId::Control))?;
            let response = TcpAssociationHandshake::new(
                stream_id,
                local_identity.clone(),
                remote_identity.address().clone(),
            );
            stream
                .stream
                .write_all(&encode_tcp_association_handshake(&response)?)
                .map_err(|error| tcp_inbound_failure(&stream.peer.to_string(), error))?;
        }
        Ok(())
    }

    fn install_reverse_route(
        &self,
        identity: &Option<TcpAssociationIdentity>,
        streams: &[TcpAcceptedStream],
    ) -> Result<Option<RemoteAssociationRouteRegistration>> {
        let (Some(identity), Some(installer)) = (identity, &self.route_installer) else {
            return Ok(None);
        };

        let control = clone_lane_sink(streams, RemoteStreamId::Control, identity.address())?;
        let ordinary = clone_lane_sink(streams, RemoteStreamId::Ordinary, identity.address())?;
        let large = clone_lane_sink(streams, RemoteStreamId::Large, identity.address())?;
        Ok(Some(installer.insert_stream_pipeline(
            identity.address().clone(),
            control,
            ordinary,
            large,
        )?))
    }

    fn reader_for(&self, identity: &Option<TcpAssociationIdentity>) -> TcpAssociationStreamReader {
        match &self.handler_factory {
            Some(factory) => self
                .reader
                .with_handler(factory.handler_for(identity.as_ref())),
            None => self.reader.clone(),
        }
    }
}

fn clone_lane_sink(
    streams: &[TcpAcceptedStream],
    stream_id: RemoteStreamId,
    address: &RemoteAssociationAddress,
) -> Result<Arc<dyn RemoteByteSink>> {
    let stream = streams
        .iter()
        .find(|stream| stream.stream_id == Some(stream_id))
        .ok_or_else(|| missing_lane_error(stream_id))?;
    let stream = stream
        .stream
        .try_clone()
        .map_err(|error| tcp_inbound_failure(&stream.peer.to_string(), error))?;
    Ok(Arc::new(TcpRemoteByteSink::from_stream(
        address.to_string(),
        stream,
    )))
}

pub struct TcpAssociationListenerHandle {
    stop: Arc<AtomicBool>,
    join: JoinHandle<Result<TcpAssociationListenerReport>>,
}

impl TcpAssociationListenerHandle {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    pub fn join(self) -> Result<TcpAssociationListenerReport> {
        self.join
            .join()
            .map_err(|_| RemoteError::Inbound("tcp association listener panicked".to_string()))?
    }
}
