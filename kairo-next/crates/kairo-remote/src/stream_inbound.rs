use std::sync::Arc;

use bytes::Bytes;

use crate::{RemoteStreamDecoder, RemoteStreamId, Result};

pub trait RemoteFrameHandler: Send + Sync + 'static {
    fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> Result<()>;
}

impl<F> RemoteFrameHandler for F
where
    F: Fn(RemoteStreamId, Bytes) -> Result<()> + Send + Sync + 'static,
{
    fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> Result<()> {
        self(stream_id, frame)
    }
}

pub struct StreamFrameInbound {
    decoder: RemoteStreamDecoder,
    handler: Arc<dyn RemoteFrameHandler>,
}

impl StreamFrameInbound {
    pub fn new(handler: Arc<dyn RemoteFrameHandler>) -> Self {
        Self {
            decoder: RemoteStreamDecoder::new(),
            handler,
        }
    }

    pub fn with_max_frame_len(max_frame_len: usize, handler: Arc<dyn RemoteFrameHandler>) -> Self {
        Self {
            decoder: RemoteStreamDecoder::with_max_frame_len(max_frame_len),
            handler,
        }
    }

    pub fn stream_id(&self) -> Option<RemoteStreamId> {
        self.decoder.stream_id()
    }

    pub fn push_bytes(&mut self, chunk: Bytes) -> Result<usize> {
        let frames = self.decoder.push(chunk)?;
        let delivered = frames.len();
        for frame in frames {
            self.handler
                .handle_frame(frame.stream_id(), frame.into_payload())?;
        }
        Ok(delivered)
    }

    pub fn finish(self) -> Result<()> {
        self.decoder.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::{RemoteError, RemoteStreamEncoder};

    #[derive(Default)]
    struct CollectingFrameHandler {
        frames: Mutex<Vec<(RemoteStreamId, Bytes)>>,
        fail_after: Mutex<Option<usize>>,
    }

    impl CollectingFrameHandler {
        fn frames(&self) -> Vec<(RemoteStreamId, Bytes)> {
            self.frames.lock().expect("frame handler poisoned").clone()
        }

        fn fail_after(&self, delivered: usize) {
            *self.fail_after.lock().expect("frame handler poisoned") = Some(delivered);
        }
    }

    impl RemoteFrameHandler for CollectingFrameHandler {
        fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> Result<()> {
            let mut frames = self.frames.lock().expect("frame handler poisoned");
            if self
                .fail_after
                .lock()
                .expect("frame handler poisoned")
                .is_some_and(|limit| frames.len() >= limit)
            {
                return Err(RemoteError::Inbound("target unavailable".to_string()));
            }
            frames.push((stream_id, frame));
            Ok(())
        }
    }

    fn encoded_stream(stream_id: RemoteStreamId, payloads: &[&'static [u8]]) -> Bytes {
        let mut encoder = RemoteStreamEncoder::new(stream_id);
        let mut bytes = Vec::new();
        for payload in payloads {
            bytes.extend_from_slice(&encoder.encode_frame(&Bytes::from_static(payload)).unwrap());
        }
        Bytes::from(bytes)
    }

    #[test]
    fn stream_frame_inbound_buffers_chunks_and_dispatches_complete_frames() {
        let handler = Arc::new(CollectingFrameHandler::default());
        let mut inbound = StreamFrameInbound::new(handler.clone() as Arc<dyn RemoteFrameHandler>);
        let bytes = encoded_stream(RemoteStreamId::Control, &[b"watch", b"heartbeat"]);

        assert_eq!(inbound.push_bytes(bytes.slice(..3)).unwrap(), 0);
        assert_eq!(inbound.push_bytes(bytes.slice(3..14)).unwrap(), 1);
        assert_eq!(inbound.stream_id(), Some(RemoteStreamId::Control));
        assert_eq!(inbound.push_bytes(bytes.slice(14..)).unwrap(), 1);
        inbound.finish().unwrap();

        let frames = handler.frames();
        assert_eq!(
            frames,
            vec![
                (RemoteStreamId::Control, Bytes::from_static(b"watch")),
                (RemoteStreamId::Control, Bytes::from_static(b"heartbeat")),
            ]
        );
    }

    #[test]
    fn stream_frame_inbound_dispatches_multiple_frames_from_one_chunk() {
        let handler = Arc::new(CollectingFrameHandler::default());
        let mut inbound = StreamFrameInbound::new(handler.clone() as Arc<dyn RemoteFrameHandler>);
        let bytes = encoded_stream(RemoteStreamId::Ordinary, &[b"user-1", b"user-2"]);

        assert_eq!(inbound.push_bytes(bytes).unwrap(), 2);

        let frames = handler.frames();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].0, RemoteStreamId::Ordinary);
        assert_eq!(frames[0].1, Bytes::from_static(b"user-1"));
        assert_eq!(frames[1].1, Bytes::from_static(b"user-2"));
    }

    #[test]
    fn stream_frame_inbound_propagates_handler_failure() {
        let handler = Arc::new(CollectingFrameHandler::default());
        handler.fail_after(1);
        let mut inbound = StreamFrameInbound::new(handler.clone() as Arc<dyn RemoteFrameHandler>);
        let bytes = encoded_stream(RemoteStreamId::Large, &[b"first", b"second"]);

        let error = inbound
            .push_bytes(bytes)
            .expect_err("handler failure should propagate");

        assert!(matches!(error, RemoteError::Inbound(_)));
        assert_eq!(handler.frames().len(), 1);
    }

    #[test]
    fn stream_frame_inbound_propagates_decoder_failure_before_dispatch() {
        let handler = Arc::new(CollectingFrameHandler::default());
        let mut inbound = StreamFrameInbound::new(handler.clone() as Arc<dyn RemoteFrameHandler>);

        let error = inbound
            .push_bytes(Bytes::from_static(b"NOPE\x01"))
            .expect_err("invalid stream should fail");

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(handler.frames().is_empty());
    }

    #[test]
    fn stream_frame_inbound_finish_detects_truncated_frame() {
        let handler = Arc::new(CollectingFrameHandler::default());
        let mut inbound =
            StreamFrameInbound::with_max_frame_len(1024, handler as Arc<dyn RemoteFrameHandler>);
        let bytes = encoded_stream(RemoteStreamId::Ordinary, &[b"payload"]);

        assert_eq!(inbound.push_bytes(bytes.slice(..8)).unwrap(), 0);
        let error = inbound.finish().expect_err("truncated stream should fail");

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(error.to_string().contains("truncated"));
    }
}
