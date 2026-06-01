use std::sync::{Mutex, mpsc};

use bytes::Bytes;
use kairo_serialization::{ActorRefWireData, Manifest, RemoteEnvelope, SerializedMessage};

use super::*;
use crate::{RemoteAssociationAddress, RemoteError, RemoteFrameHandler, RemoteStreamId};

mod association;
mod sink;
mod supervision;

#[derive(Default)]
struct CollectingFrameHandler {
    frames: Mutex<Vec<(RemoteStreamId, Bytes)>>,
}

impl CollectingFrameHandler {
    fn frames(&self) -> Vec<(RemoteStreamId, Bytes)> {
        self.frames.lock().expect("frame handler poisoned").clone()
    }
}

struct ChannelFrameHandler {
    tx: Mutex<mpsc::Sender<(RemoteStreamId, Bytes)>>,
}

impl ChannelFrameHandler {
    fn new(tx: mpsc::Sender<(RemoteStreamId, Bytes)>) -> Self {
        Self { tx: Mutex::new(tx) }
    }
}

impl RemoteFrameHandler for ChannelFrameHandler {
    fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> crate::Result<()> {
        self.tx
            .lock()
            .expect("channel frame handler poisoned")
            .send((stream_id, frame))
            .map_err(|error| RemoteError::Inbound(error.to_string()))
    }
}

impl RemoteFrameHandler for CollectingFrameHandler {
    fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> crate::Result<()> {
        self.frames
            .lock()
            .expect("frame handler poisoned")
            .push((stream_id, frame));
        Ok(())
    }
}

fn address(port: u16) -> RemoteAssociationAddress {
    RemoteAssociationAddress::new("kairo", "remote", "127.0.0.1", Some(port)).unwrap()
}

fn association_address(system: &str, port: u16) -> RemoteAssociationAddress {
    RemoteAssociationAddress::new("kairo", system, "127.0.0.1", Some(port)).unwrap()
}

fn envelope(port: u16, value: u8) -> RemoteEnvelope {
    envelope_to("remote", port, value)
}

fn envelope_to(system: &str, port: u16, value: u8) -> RemoteEnvelope {
    RemoteEnvelope::new(
        ActorRefWireData::new(format!("kairo://{system}@127.0.0.1:{port}/user/target")).unwrap(),
        None,
        SerializedMessage::new(
            777,
            Manifest::new("kairo.remote.test.TcpAssociation"),
            1,
            Bytes::from(vec![value]),
        ),
    )
}
