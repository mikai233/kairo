#![deny(missing_docs)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{SyncSender, TryRecvError, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use bytes::Bytes;

use crate::{
    RemoteError, RemoteLaneSink, RemoteStreamEncoder, RemoteStreamId, Result, lane_send_failure,
};

/// Sink for encoded bytes written to one remote transport stream.
pub trait RemoteByteSink: Send + Sync + 'static {
    /// Sends one encoded byte chunk.
    fn send_bytes(&self, bytes: Bytes) -> Result<()>;

    /// Sends an ordered batch of encoded byte chunks.
    ///
    /// The default implementation preserves compatibility for sinks that only
    /// support individual writes. Transport sinks may override this method to
    /// amortize synchronization and system calls across a queued burst.
    fn send_byte_batch(&self, batch: &[Bytes]) -> Result<()> {
        for bytes in batch {
            self.send_bytes(bytes.clone())?;
        }
        Ok(())
    }

    /// Closes the byte sink.
    ///
    /// Stateless implementations may keep the default no-op behavior.
    fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Bounded queue capacities for the three outbound transport lanes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteOutboundQueueSettings {
    control_capacity: usize,
    ordinary_capacity: usize,
    large_capacity: usize,
}

impl RemoteOutboundQueueSettings {
    /// Creates queue settings, rejecting a zero capacity for any lane.
    pub fn new(
        control_capacity: usize,
        ordinary_capacity: usize,
        large_capacity: usize,
    ) -> Result<Self> {
        if control_capacity == 0 || ordinary_capacity == 0 || large_capacity == 0 {
            return Err(RemoteError::Outbound(
                "remote outbound lane queue capacities must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            control_capacity,
            ordinary_capacity,
            large_capacity,
        })
    }

    /// Returns the control-lane queue capacity.
    pub fn control_capacity(&self) -> usize {
        self.control_capacity
    }

    /// Returns the ordinary-lane queue capacity.
    pub fn ordinary_capacity(&self) -> usize {
        self.ordinary_capacity
    }

    /// Returns the large-lane queue capacity.
    pub fn large_capacity(&self) -> usize {
        self.large_capacity
    }

    pub(crate) fn capacity_for(&self, lane: RemoteStreamId) -> usize {
        match lane {
            RemoteStreamId::Control => self.control_capacity,
            RemoteStreamId::Ordinary => self.ordinary_capacity,
            RemoteStreamId::Large => self.large_capacity,
        }
    }
}

impl Default for RemoteOutboundQueueSettings {
    fn default() -> Self {
        Self {
            control_capacity: 256,
            ordinary_capacity: 1_024,
            large_capacity: 32,
        }
    }
}

/// A bounded, non-blocking byte sink backed by a dedicated writer thread.
///
/// Sends fail immediately when the lane queue is full, closed, or its writer
/// has observed an underlying sink failure.
pub struct QueuedRemoteByteSink {
    lane: RemoteStreamId,
    capacity: usize,
    sender: Mutex<Option<SyncSender<Bytes>>>,
    sink: Arc<dyn RemoteByteSink>,
    worker: Mutex<Option<JoinHandle<()>>>,
    failure: Arc<Mutex<Option<String>>>,
    closed: Arc<AtomicBool>,
}

type RemoteLaneWriterFailureHandler = Arc<dyn Fn(RemoteStreamId, String) + Send + Sync + 'static>;

const MAX_QUEUED_WRITE_BATCH_FRAMES: usize = 64;

impl QueuedRemoteByteSink {
    /// Creates a queued sink for `lane` with a positive bounded `capacity`.
    pub fn new(
        lane: RemoteStreamId,
        capacity: usize,
        sink: Arc<dyn RemoteByteSink>,
    ) -> Result<Self> {
        Self::new_with_failure_handler(lane, capacity, sink, None)
    }

    pub(crate) fn new_with_failure_handler(
        lane: RemoteStreamId,
        capacity: usize,
        sink: Arc<dyn RemoteByteSink>,
        failure_handler: Option<RemoteLaneWriterFailureHandler>,
    ) -> Result<Self> {
        if capacity == 0 {
            return Err(RemoteError::Outbound(format!(
                "remote {lane:?} lane queue capacity must be greater than zero"
            )));
        }
        let (sender, receiver) = sync_channel::<Bytes>(capacity);
        let worker_sink = sink.clone();
        let failure = Arc::new(Mutex::new(None));
        let worker_failure = failure.clone();
        let closed = Arc::new(AtomicBool::new(false));
        let worker_closed = closed.clone();
        let worker = thread::Builder::new()
            .name(format!("kairo-remote-{}-writer", lane_name(lane)))
            .spawn(move || {
                let mut batch = Vec::with_capacity(capacity.min(MAX_QUEUED_WRITE_BATCH_FRAMES));
                while let Ok(bytes) = receiver.recv() {
                    batch.push(bytes);
                    while batch.len() < MAX_QUEUED_WRITE_BATCH_FRAMES {
                        match receiver.try_recv() {
                            Ok(bytes) => batch.push(bytes),
                            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                        }
                    }
                    let write_result = worker_sink.send_byte_batch(&batch);
                    batch.clear();
                    if let Err(error) = write_result {
                        if !worker_closed.load(Ordering::Acquire) {
                            let reason = error.to_string();
                            *worker_failure
                                .lock()
                                .expect("remote lane writer failure lock poisoned") =
                                Some(reason.clone());
                            if let Some(handler) = &failure_handler {
                                handler(lane, reason);
                            }
                        }
                        break;
                    }
                }
            })
            .map_err(|error| {
                RemoteError::Outbound(format!(
                    "failed to spawn remote {lane:?} lane writer: {error}"
                ))
            })?;

        Ok(Self {
            lane,
            capacity,
            sender: Mutex::new(Some(sender)),
            sink,
            worker: Mutex::new(Some(worker)),
            failure,
            closed,
        })
    }

    fn failure(&self) -> Option<String> {
        self.failure
            .lock()
            .expect("remote lane writer failure lock poisoned")
            .clone()
    }
}

impl RemoteByteSink for QueuedRemoteByteSink {
    fn send_bytes(&self, bytes: Bytes) -> Result<()> {
        if let Some(reason) = self.failure() {
            return Err(RemoteError::OutboundLaneClosed {
                lane: lane_name(self.lane),
                reason,
            });
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(RemoteError::OutboundLaneClosed {
                lane: lane_name(self.lane),
                reason: "writer closed".to_string(),
            });
        }

        let sender = self
            .sender
            .lock()
            .expect("remote lane writer sender lock poisoned");
        let Some(sender) = sender.as_ref() else {
            return Err(RemoteError::OutboundLaneClosed {
                lane: lane_name(self.lane),
                reason: "writer closed".to_string(),
            });
        };
        match sender.try_send(bytes) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(RemoteError::OutboundLaneQueueFull {
                lane: lane_name(self.lane),
                capacity: self.capacity,
            }),
            Err(TrySendError::Disconnected(_)) => Err(RemoteError::OutboundLaneClosed {
                lane: lane_name(self.lane),
                reason: self
                    .failure()
                    .unwrap_or_else(|| "writer stopped".to_string()),
            }),
        }
    }

    fn close(&self) -> Result<()> {
        let first_close = !self.closed.swap(true, Ordering::AcqRel);
        if first_close {
            self.sender
                .lock()
                .expect("remote lane writer sender lock poisoned")
                .take();
        }
        let close_result = if first_close {
            self.sink.close()
        } else {
            Ok(())
        };
        let worker = self
            .worker
            .lock()
            .expect("remote lane writer worker lock poisoned")
            .take();
        if let Some(worker) = worker {
            worker.join().map_err(|_| RemoteError::OutboundLaneClosed {
                lane: lane_name(self.lane),
                reason: "writer thread panicked".to_string(),
            })?;
        }
        close_result
    }
}

impl Drop for QueuedRemoteByteSink {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn lane_name(lane: RemoteStreamId) -> String {
    format!("{lane:?}").to_ascii_lowercase()
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
/// Serializes frame payloads onto one stream and writes the encoded bytes.
///
/// Clones share encoder state so the stream header is emitted exactly once.
pub struct RemoteStreamWriter {
    inner: Arc<RemoteStreamWriterInner>,
}

struct RemoteStreamWriterInner {
    encoder: Mutex<RemoteStreamEncoder>,
    sink: Arc<dyn RemoteByteSink>,
}

impl RemoteStreamWriter {
    /// Creates a writer for `stream_id` backed by `sink`.
    pub fn new(stream_id: RemoteStreamId, sink: Arc<dyn RemoteByteSink>) -> Self {
        Self {
            inner: Arc::new(RemoteStreamWriterInner {
                encoder: Mutex::new(RemoteStreamEncoder::new(stream_id)),
                sink,
            }),
        }
    }

    /// Returns the writer's stream identifier.
    pub fn stream_id(&self) -> RemoteStreamId {
        self.inner
            .encoder
            .lock()
            .expect("remote stream encoder poisoned")
            .stream_id()
    }

    /// Encodes and sends one frame payload.
    pub fn send_frame_payload(&self, payload: Bytes) -> Result<()> {
        let encoded = self
            .inner
            .encoder
            .lock()
            .expect("remote stream encoder poisoned")
            .encode_frame(&payload)?;
        self.inner.sink.send_bytes(encoded)
    }

    /// Closes the underlying byte sink.
    pub fn close(&self) -> Result<()> {
        self.inner.sink.close()
    }
}

#[derive(Clone)]
/// Routes classified frames to independent control, ordinary, and large
/// stream writers.
pub struct StreamLaneSink {
    control: RemoteStreamWriter,
    ordinary: RemoteStreamWriter,
    large: RemoteStreamWriter,
}

impl StreamLaneSink {
    /// Creates one stream writer for each supplied lane byte sink.
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

    /// Creates a lane sink from existing stream writers.
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

    /// Returns the control stream writer.
    pub fn control(&self) -> &RemoteStreamWriter {
        &self.control
    }

    /// Returns the ordinary stream writer.
    pub fn ordinary(&self) -> &RemoteStreamWriter {
        &self.ordinary
    }

    /// Returns the large stream writer.
    pub fn large(&self) -> &RemoteStreamWriter {
        &self.large
    }

    /// Closes all three writers and returns the first close failure.
    pub fn close(&self) -> Result<()> {
        let mut first_error = None;
        for writer in [&self.control, &self.ordinary, &self.large] {
            if let Err(error) = writer.close() {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
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

/// Creates an outbound error annotated with the failed stream.
pub fn stream_send_failure(stream_id: RemoteStreamId, reason: impl Into<String>) -> RemoteError {
    RemoteError::Outbound(format!(
        "remote {:?} stream write failed: {}",
        stream_id,
        reason.into()
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::{Condvar, Mutex};
    use std::thread::ThreadId;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::{RemoteStreamDecoder, RemoteStreamFrame};

    #[derive(Default)]
    struct CollectingByteSink {
        writes: Mutex<Vec<Bytes>>,
        fail_with: Mutex<Option<String>>,
    }

    #[derive(Default)]
    struct BlockingByteSink {
        writes: Mutex<Vec<(Bytes, ThreadId)>>,
        batches: Mutex<Vec<Vec<Bytes>>>,
        entered: Condvar,
        released: Mutex<bool>,
        release: Condvar,
    }

    impl BlockingByteSink {
        fn wait_until_entered(&self, timeout: Duration) {
            let deadline = Instant::now() + timeout;
            let mut writes = self.writes.lock().expect("blocking sink poisoned");
            while writes.is_empty() {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    panic!("queued writer did not enter blocking sink");
                };
                let (next, wait) = self
                    .entered
                    .wait_timeout(writes, remaining)
                    .expect("blocking sink poisoned");
                writes = next;
                assert!(
                    !wait.timed_out(),
                    "queued writer did not enter blocking sink"
                );
            }
        }

        fn release(&self) {
            *self.released.lock().expect("blocking sink poisoned") = true;
            self.release.notify_all();
        }

        fn writes(&self) -> Vec<(Bytes, ThreadId)> {
            self.writes.lock().expect("blocking sink poisoned").clone()
        }

        fn batches(&self) -> Vec<Vec<Bytes>> {
            self.batches.lock().expect("blocking sink poisoned").clone()
        }
    }

    impl RemoteByteSink for BlockingByteSink {
        fn send_bytes(&self, bytes: Bytes) -> Result<()> {
            self.writes
                .lock()
                .expect("blocking sink poisoned")
                .push((bytes, std::thread::current().id()));
            self.entered.notify_all();
            let mut released = self.released.lock().expect("blocking sink poisoned");
            while !*released {
                released = self.release.wait(released).expect("blocking sink poisoned");
            }
            Ok(())
        }

        fn send_byte_batch(&self, batch: &[Bytes]) -> Result<()> {
            self.batches
                .lock()
                .expect("blocking sink poisoned")
                .push(batch.to_vec());
            let writer = std::thread::current().id();
            self.writes
                .lock()
                .expect("blocking sink poisoned")
                .extend(batch.iter().cloned().map(|bytes| (bytes, writer)));
            self.entered.notify_all();
            let mut released = self.released.lock().expect("blocking sink poisoned");
            while !*released {
                released = self.release.wait(released).expect("blocking sink poisoned");
            }
            Ok(())
        }

        fn close(&self) -> Result<()> {
            self.release();
            Ok(())
        }
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

    #[test]
    fn queued_sink_rejects_overflow_without_blocking_sender() {
        let inner = Arc::new(BlockingByteSink::default());
        let queued = QueuedRemoteByteSink::new(
            RemoteStreamId::Ordinary,
            2,
            inner.clone() as Arc<dyn RemoteByteSink>,
        )
        .unwrap();
        let caller = std::thread::current().id();

        queued.send_bytes(Bytes::from_static(b"one")).unwrap();
        inner.wait_until_entered(Duration::from_secs(1));
        queued.send_bytes(Bytes::from_static(b"two")).unwrap();
        queued.send_bytes(Bytes::from_static(b"three")).unwrap();
        let started = Instant::now();
        let error = queued
            .send_bytes(Bytes::from_static(b"four"))
            .expect_err("bounded lane should reject overflow");

        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(matches!(
            error,
            RemoteError::OutboundLaneQueueFull { lane, capacity: 2 }
                if lane == "ordinary"
        ));
        inner.release();
        queued.close().unwrap();
        let writes = inner.writes();
        assert_eq!(
            writes
                .iter()
                .map(|(bytes, _)| bytes.clone())
                .collect::<Vec<_>>(),
            vec![
                Bytes::from_static(b"one"),
                Bytes::from_static(b"two"),
                Bytes::from_static(b"three"),
            ]
        );
        assert!(writes.iter().all(|(_, writer)| *writer != caller));
        assert!(writes.iter().all(|(_, writer)| *writer == writes[0].1));
    }

    #[test]
    fn queued_sink_drains_two_thousand_frames_on_one_writer() {
        let inner = Arc::new(BlockingByteSink::default());
        inner.release();
        let queued = QueuedRemoteByteSink::new(
            RemoteStreamId::Control,
            2_000,
            inner.clone() as Arc<dyn RemoteByteSink>,
        )
        .unwrap();

        for value in 0_u16..2_000 {
            queued
                .send_bytes(Bytes::copy_from_slice(&value.to_be_bytes()))
                .unwrap();
        }
        queued.close().unwrap();

        let writes = inner.writes();
        assert_eq!(writes.len(), 2_000);
        assert!(writes.iter().all(|(_, writer)| *writer == writes[0].1));
        for (expected, (bytes, _)) in (0_u16..2_000).zip(writes) {
            assert_eq!(bytes.as_ref(), expected.to_be_bytes());
        }
    }

    #[test]
    fn queued_sink_batches_an_already_queued_burst_in_fifo_order() {
        let inner = Arc::new(BlockingByteSink::default());
        let queued = QueuedRemoteByteSink::new(
            RemoteStreamId::Ordinary,
            4,
            inner.clone() as Arc<dyn RemoteByteSink>,
        )
        .unwrap();

        queued.send_bytes(Bytes::from_static(b"one")).unwrap();
        inner.wait_until_entered(Duration::from_secs(1));
        queued.send_bytes(Bytes::from_static(b"two")).unwrap();
        queued.send_bytes(Bytes::from_static(b"three")).unwrap();
        queued.send_bytes(Bytes::from_static(b"four")).unwrap();
        inner.release();
        queued.close().unwrap();

        assert_eq!(
            inner.batches(),
            vec![
                vec![Bytes::from_static(b"one")],
                vec![
                    Bytes::from_static(b"two"),
                    Bytes::from_static(b"three"),
                    Bytes::from_static(b"four"),
                ],
            ]
        );
    }

    #[test]
    fn queued_sink_caps_each_burst_batch() {
        let inner = Arc::new(BlockingByteSink::default());
        let queued = QueuedRemoteByteSink::new(
            RemoteStreamId::Ordinary,
            70,
            inner.clone() as Arc<dyn RemoteByteSink>,
        )
        .unwrap();

        queued.send_bytes(Bytes::from_static(b"first")).unwrap();
        inner.wait_until_entered(Duration::from_secs(1));
        for value in 0_u8..70 {
            queued.send_bytes(Bytes::copy_from_slice(&[value])).unwrap();
        }
        inner.release();
        queued.close().unwrap();

        let batches = inner.batches();
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![1, MAX_QUEUED_WRITE_BATCH_FRAMES, 6]
        );
        assert_eq!(
            batches
                .into_iter()
                .skip(1)
                .flatten()
                .map(|bytes| bytes[0])
                .collect::<Vec<_>>(),
            (0_u8..70).collect::<Vec<_>>()
        );
    }

    #[test]
    fn outbound_queue_settings_reject_zero_capacity() {
        let error =
            RemoteOutboundQueueSettings::new(1, 0, 1).expect_err("zero lane capacity should fail");

        assert!(matches!(error, RemoteError::Outbound(_)));
    }
}
