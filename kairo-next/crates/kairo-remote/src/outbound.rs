use kairo_serialization::RemoteEnvelope;

use crate::Result;

pub trait RemoteOutbound: Send + Sync + 'static {
    fn send(&self, envelope: RemoteEnvelope) -> Result<()>;
}
