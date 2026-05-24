use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::{
    RemoteError, RemoteLaneSink, RemoteStreamEncoder, RemoteStreamId, Result, lane_send_failure,
};

pub trait RemoteByteSink: Send + Sync + 'static {
    fn send_bytes(&self, bytes: Bytes) -> Result<()>;
}

impl<F> RemoteByteSink for F
where
    F: Fn(Bytes) -> Result<()> + Send + Sync + 'static,
{
    fn send_bytes(&self, bytes: Bytes) -> Result<()> {
        self(bytes)
    }
}

#[derive(Clone)]
pub struct RemoteStreamWriter {
    inner: Arc<RemoteStreamWriterInner>,
}

struct RemoteStreamWriterInner {
    encoder: Mutex<RemoteStreamEncoder>,
    sink: Arc<dyn RemoteByteSink>,
}

impl RemoteStreamWriter {
    pub fn new(stream_id: RemoteStreamId, sink: Arc<dyn RemoteByteSink>) -> Self {
        Self {
            inner: Arc::new(RemoteStreamWriterInner {
                encoder: Mutex::new(RemoteStreamEncoder::new(stream_id)),
                sink,
            }),
        }
    }

    pub fn stream_id(&self) -> RemoteStreamId {
        self.inner
            .encoder
            .lock()
            .expect("remote stream encoder poisoned")
            .stream_id()
    }

    pub fn send_frame_payload(&self, payload: Bytes) -> Result<()> {
        let encoded = self
            .inner
            .encoder
            .lock()
            .expect("remote stream encoder poisoned")
            .encode_frame(&payload)?;
        self.inner.sink.send_bytes(encoded)
    }
}

#[derive(Clone)]
pub struct StreamLaneSink {
    control: RemoteStreamWriter,
    ordinary: RemoteStreamWriter,
    large: RemoteStreamWriter,
}

impl StreamLaneSink {
    pub fn new(
        control: Arc<dyn RemoteByteSink>,
        ordinary: Arc<dyn RemoteByteSink>,
        large: Arc<dyn RemoteByteSink>,
    ) -> Self {
        Self {
            control: RemoteStreamWriter::new(RemoteStreamId::Control, control),
            ordinary: RemoteStreamWriter::new(RemoteStreamId::Ordinary, ordinary),
            large: RemoteStreamWriter::new(RemoteStreamId::Large, large),
        }
    }

    pub fn from_writers(
        control: RemoteStreamWriter,
        ordinary: RemoteStreamWriter,
        large: RemoteStreamWriter,
    ) -> Self {
        Self {
            control,
            ordinary,
            large,
        }
    }

    pub fn control(&self) -> &RemoteStreamWriter {
        &self.control
    }

    pub fn ordinary(&self) -> &RemoteStreamWriter {
        &self.ordinary
    }

    pub fn large(&self) -> &RemoteStreamWriter {
        &self.large
    }

    fn writer_for(&self, lane: RemoteStreamId) -> &RemoteStreamWriter {
        match lane {
            RemoteStreamId::Control => &self.control,
            RemoteStreamId::Ordinary => &self.ordinary,
            RemoteStreamId::Large => &self.large,
        }
    }
}

impl RemoteLaneSink for StreamLaneSink {
    fn send_lane_frame(&self, lane: RemoteStreamId, frame: Bytes) -> Result<()> {
        self.writer_for(lane)
            .send_frame_payload(frame)
            .map_err(|error| match error {
                RemoteError::Outbound(reason) => lane_send_failure(lane, reason),
                other => other,
            })
    }
}

pub fn stream_send_failure(stream_id: RemoteStreamId, reason: impl Into<String>) -> RemoteError {
    RemoteError::Outbound(format!(
        "remote {:?} stream write failed: {}",
        stream_id,
        reason.into()
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::{RemoteStreamDecoder, RemoteStreamFrame};

    #[derive(Default)]
    struct CollectingByteSink {
        writes: Mutex<Vec<Bytes>>,
        fail_with: Mutex<Option<String>>,
    }

    impl CollectingByteSink {
        fn writes(&self) -> Vec<Bytes> {
            self.writes.lock().expect("byte sink poisoned").clone()
        }

        fn fail(&self, reason: impl Into<String>) {
            *self.fail_with.lock().expect("byte sink poisoned") = Some(reason.into());
        }
    }

    impl RemoteByteSink for CollectingByteSink {
        fn send_bytes(&self, bytes: Bytes) -> Result<()> {
            if let Some(reason) = self.fail_with.lock().expect("byte sink poisoned").clone() {
                return Err(stream_send_failure(RemoteStreamId::Ordinary, reason));
            }
            self.writes.lock().expect("byte sink poisoned").push(bytes);
            Ok(())
        }
    }

    fn decode_stream(writes: Vec<Bytes>) -> Vec<RemoteStreamFrame> {
        let mut decoder = RemoteStreamDecoder::new();
        let mut frames = Vec::new();
        for write in writes {
            frames.extend(decoder.push(write).unwrap());
        }
        decoder.finish().unwrap();
        frames
    }

    #[test]
    fn stream_writer_writes_header_once_then_length_prefixed_frames() {
        let sink = Arc::new(CollectingByteSink::default());
        let writer = RemoteStreamWriter::new(
            RemoteStreamId::Control,
            sink.clone() as Arc<dyn RemoteByteSink>,
        );

        writer
            .send_frame_payload(Bytes::from_static(b"one"))
            .unwrap();
        writer
            .send_frame_payload(Bytes::from_static(b"two"))
            .unwrap();

        let writes = sink.writes();
        assert_eq!(writes.len(), 2);
        assert_eq!(&writes[0][..5], b"KAIR\x01");
        assert_eq!(&writes[1][..4], &[0, 0, 0, 3]);
        let frames = decode_stream(writes);
        assert_eq!(
            frames,
            vec![
                RemoteStreamFrame::new(RemoteStreamId::Control, Bytes::from_static(b"one")),
                RemoteStreamFrame::new(RemoteStreamId::Control, Bytes::from_static(b"two")),
            ]
        );
    }

    #[test]
    fn stream_lane_sink_writes_each_lane_to_its_own_stream() {
        let control = Arc::new(CollectingByteSink::default());
        let ordinary = Arc::new(CollectingByteSink::default());
        let large = Arc::new(CollectingByteSink::default());
        let sink = StreamLaneSink::new(
            control.clone() as Arc<dyn RemoteByteSink>,
            ordinary.clone() as Arc<dyn RemoteByteSink>,
            large.clone() as Arc<dyn RemoteByteSink>,
        );

        sink.send_lane_frame(RemoteStreamId::Control, Bytes::from_static(b"watch"))
            .unwrap();
        sink.send_lane_frame(RemoteStreamId::Ordinary, Bytes::from_static(b"user"))
            .unwrap();
        sink.send_lane_frame(RemoteStreamId::Large, Bytes::from_static(b"bulk"))
            .unwrap();
        sink.send_lane_frame(RemoteStreamId::Control, Bytes::from_static(b"heartbeat"))
            .unwrap();

        assert_eq!(&control.writes()[0][..5], b"KAIR\x01");
        assert_eq!(&ordinary.writes()[0][..5], b"KAIR\x02");
        assert_eq!(&large.writes()[0][..5], b"KAIR\x03");

        let control_frames = decode_stream(control.writes());
        assert_eq!(control_frames.len(), 2);
        assert_eq!(control_frames[0].payload(), &Bytes::from_static(b"watch"));
        assert_eq!(
            control_frames[1].payload(),
            &Bytes::from_static(b"heartbeat")
        );
        assert_eq!(
            decode_stream(ordinary.writes())[0].payload(),
            &Bytes::from_static(b"user")
        );
        assert_eq!(
            decode_stream(large.writes())[0].payload(),
            &Bytes::from_static(b"bulk")
        );
    }

    #[test]
    fn stream_lane_sink_propagates_failed_stream_writes_with_lane_context() {
        let control = Arc::new(CollectingByteSink::default());
        let ordinary = Arc::new(CollectingByteSink::default());
        let large = Arc::new(CollectingByteSink::default());
        ordinary.fail("socket closed");
        let sink = StreamLaneSink::new(
            control as Arc<dyn RemoteByteSink>,
            ordinary as Arc<dyn RemoteByteSink>,
            large as Arc<dyn RemoteByteSink>,
        );

        let error = sink
            .send_lane_frame(RemoteStreamId::Ordinary, Bytes::from_static(b"user"))
            .expect_err("failed stream write should propagate");

        assert!(matches!(error, RemoteError::Outbound(_)));
        assert!(error.to_string().contains("Ordinary"));
        assert!(error.to_string().contains("socket closed"));
    }
}
