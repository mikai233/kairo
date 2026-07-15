#![deny(missing_docs)]

use std::collections::VecDeque;

use bytes::Bytes;
use kairo_serialization::{
    MessageCodec, Registry, RemoteEnvelope, RemoteMessage, SerializationRegistry, WireReader,
    WireWriter,
};

use crate::{RemoteError, Result};

/// Stable serializer identifier for [`ReliableSystemEnvelope`].
pub const RELIABLE_SYSTEM_ENVELOPE_SERIALIZER_ID: u32 = 1_010;
/// Stable serializer identifier for [`ReliableSystemAck`].
pub const RELIABLE_SYSTEM_ACK_SERIALIZER_ID: u32 = 1_011;
/// Stable serializer identifier for [`ReliableSystemNack`].
pub const RELIABLE_SYSTEM_NACK_SERIALIZER_ID: u32 = 1_012;

/// A retained system envelope tagged with association incarnations and an
/// ordered delivery sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReliableSystemEnvelope {
    /// Sending actor-system incarnation.
    pub from_uid: u64,
    /// Intended receiving actor-system incarnation.
    pub to_uid: u64,
    /// One-based sequence number within this association incarnation.
    pub sequence_nr: u64,
    /// Original serialized system envelope.
    pub envelope: RemoteEnvelope,
}

impl RemoteMessage for ReliableSystemEnvelope {
    const MANIFEST: &'static str = "kairo.remote.system.reliable-envelope";
    const VERSION: u16 = 1;
}

/// Cumulative acknowledgement for reliable system delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReliableSystemAck {
    /// Actor-system incarnation sending the acknowledgement.
    pub from_uid: u64,
    /// Original sender incarnation receiving the acknowledgement.
    pub to_uid: u64,
    /// Highest contiguously delivered sequence number.
    pub sequence_nr: u64,
}

impl RemoteMessage for ReliableSystemAck {
    const MANIFEST: &'static str = "kairo.remote.system.reliable-ack";
    const VERSION: u16 = 1;
}

/// Negative acknowledgement reporting a gap in reliable system delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReliableSystemNack {
    /// Actor-system incarnation reporting the gap.
    pub from_uid: u64,
    /// Original sender incarnation receiving the negative acknowledgement.
    pub to_uid: u64,
    /// Highest sequence number received contiguously before the gap.
    pub highest_contiguous_sequence_nr: u64,
}

impl RemoteMessage for ReliableSystemNack {
    const MANIFEST: &'static str = "kairo.remote.system.reliable-nack";
    const VERSION: u16 = 1;
}

/// Codec for [`ReliableSystemEnvelope`] protocol messages.
pub struct ReliableSystemEnvelopeCodec;
/// Codec for [`ReliableSystemAck`] protocol messages.
pub struct ReliableSystemAckCodec;
/// Codec for [`ReliableSystemNack`] protocol messages.
pub struct ReliableSystemNackCodec;

impl MessageCodec<ReliableSystemEnvelope> for ReliableSystemEnvelopeCodec {
    fn serializer_id(&self) -> u32 {
        RELIABLE_SYSTEM_ENVELOPE_SERIALIZER_ID
    }

    fn encode(&self, message: &ReliableSystemEnvelope) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_u64(message.from_uid);
        writer.write_u64(message.to_uid);
        writer.write_u64(message.sequence_nr);
        writer.write_bytes(&message.envelope.encode_wire()?)?;
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReliableSystemEnvelope> {
        ensure_version::<ReliableSystemEnvelope>(version)?;
        let mut reader = WireReader::new(&payload);
        let from_uid = reader.read_u64()?;
        let to_uid = reader.read_u64()?;
        let sequence_nr = reader.read_u64()?;
        let envelope = RemoteEnvelope::decode_wire(&reader.read_bytes()?)?;
        reader.ensure_finished()?;
        Ok(ReliableSystemEnvelope {
            from_uid,
            to_uid,
            sequence_nr,
            envelope,
        })
    }
}

impl MessageCodec<ReliableSystemAck> for ReliableSystemAckCodec {
    fn serializer_id(&self) -> u32 {
        RELIABLE_SYSTEM_ACK_SERIALIZER_ID
    }

    fn encode(&self, message: &ReliableSystemAck) -> kairo_serialization::Result<Bytes> {
        Ok(encode_reply(
            message.from_uid,
            message.to_uid,
            message.sequence_nr,
        ))
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReliableSystemAck> {
        ensure_version::<ReliableSystemAck>(version)?;
        let (from_uid, to_uid, sequence_nr) = decode_reply(payload)?;
        Ok(ReliableSystemAck {
            from_uid,
            to_uid,
            sequence_nr,
        })
    }
}

impl MessageCodec<ReliableSystemNack> for ReliableSystemNackCodec {
    fn serializer_id(&self) -> u32 {
        RELIABLE_SYSTEM_NACK_SERIALIZER_ID
    }

    fn encode(&self, message: &ReliableSystemNack) -> kairo_serialization::Result<Bytes> {
        Ok(encode_reply(
            message.from_uid,
            message.to_uid,
            message.highest_contiguous_sequence_nr,
        ))
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReliableSystemNack> {
        ensure_version::<ReliableSystemNack>(version)?;
        let (from_uid, to_uid, highest_contiguous_sequence_nr) = decode_reply(payload)?;
        Ok(ReliableSystemNack {
            from_uid,
            to_uid,
            highest_contiguous_sequence_nr,
        })
    }
}

/// Registers the reliable system envelope, acknowledgement, and negative
/// acknowledgement codecs.
pub fn register_reliable_system_codecs(registry: &mut Registry) -> kairo_serialization::Result<()> {
    registry.register::<ReliableSystemEnvelope, _>(ReliableSystemEnvelopeCodec)?;
    registry.register::<ReliableSystemAck, _>(ReliableSystemAckCodec)?;
    registry.register::<ReliableSystemNack, _>(ReliableSystemNackCodec)?;
    Ok(())
}

/// Sender-side retention and cumulative acknowledgement state for one remote
/// actor-system incarnation.
///
/// Retained envelopes remain in sequence order until acknowledged, reset for a
/// new remote incarnation, or failed by the owning runtime.
#[derive(Debug, Clone)]
pub struct ReliableSystemSender {
    local_uid: u64,
    remote_uid: u64,
    next_sequence_nr: u64,
    capacity: usize,
    unacknowledged: VecDeque<ReliableSystemEnvelope>,
}

impl ReliableSystemSender {
    /// Creates sender state with a positive bounded retention capacity.
    pub fn new(local_uid: u64, remote_uid: u64, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(RemoteError::InvalidReliableSystemDelivery(
                "sender buffer capacity must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            local_uid,
            remote_uid,
            next_sequence_nr: 1,
            capacity,
            unacknowledged: VecDeque::new(),
        })
    }

    /// Returns the sending actor-system incarnation.
    pub fn local_uid(&self) -> u64 {
        self.local_uid
    }

    /// Returns the intended remote actor-system incarnation.
    pub fn remote_uid(&self) -> u64 {
        self.remote_uid
    }

    /// Returns the number of unacknowledged envelopes retained for retry.
    pub fn pending_len(&self) -> usize {
        self.unacknowledged.len()
    }

    /// Assigns the next one-based sequence number and retains `envelope`.
    ///
    /// Returns [`RemoteError::ReliableSystemBufferFull`] without modifying
    /// state when the bounded retention buffer is full.
    pub fn retain(&mut self, envelope: RemoteEnvelope) -> Result<ReliableSystemEnvelope> {
        if self.unacknowledged.len() >= self.capacity {
            return Err(RemoteError::ReliableSystemBufferFull {
                capacity: self.capacity,
            });
        }
        let sequence_nr = self.next_sequence_nr;
        self.next_sequence_nr = self.next_sequence_nr.checked_add(1).ok_or_else(|| {
            RemoteError::InvalidReliableSystemDelivery(
                "sender sequence number overflow".to_string(),
            )
        })?;
        let reliable = ReliableSystemEnvelope {
            from_uid: self.local_uid,
            to_uid: self.remote_uid,
            sequence_nr,
            envelope,
        };
        self.unacknowledged.push_back(reliable.clone());
        Ok(reliable)
    }

    /// Applies a cumulative acknowledgement and returns the number of retained
    /// envelopes removed.
    pub fn acknowledge(&mut self, ack: &ReliableSystemAck) -> Result<usize> {
        self.validate_reply(ack.from_uid, ack.to_uid, ack.sequence_nr)?;
        Ok(self.clear_through(ack.sequence_nr))
    }

    /// Applies the cumulative progress carried by a negative acknowledgement.
    ///
    /// Envelopes after the reported contiguous sequence remain retained for
    /// retry. The return value is the number of earlier envelopes removed.
    pub fn negative_acknowledge(&mut self, nack: &ReliableSystemNack) -> Result<usize> {
        self.validate_reply(
            nack.from_uid,
            nack.to_uid,
            nack.highest_contiguous_sequence_nr,
        )?;
        Ok(self.clear_through(nack.highest_contiguous_sequence_nr))
    }

    /// Returns all unacknowledged envelopes in original sequence order.
    pub fn retry_batch(&self) -> Vec<ReliableSystemEnvelope> {
        self.unacknowledged.iter().cloned().collect()
    }

    /// Starts a fresh sequence for a new remote incarnation.
    ///
    /// All envelopes retained for the previous incarnation are removed and
    /// returned without their reliable-delivery wrappers for failure reporting.
    pub fn reset_remote_uid(&mut self, remote_uid: u64) -> Vec<RemoteEnvelope> {
        let failed = self
            .unacknowledged
            .drain(..)
            .map(|reliable| reliable.envelope)
            .collect();
        self.remote_uid = remote_uid;
        self.next_sequence_nr = 1;
        failed
    }

    fn validate_reply(&self, from_uid: u64, to_uid: u64, sequence_nr: u64) -> Result<()> {
        if from_uid != self.remote_uid || to_uid != self.local_uid {
            return Err(RemoteError::InvalidReliableSystemDelivery(format!(
                "reply association {from_uid}->{to_uid} does not match {}->{}",
                self.remote_uid, self.local_uid
            )));
        }
        let highest_sent = self.next_sequence_nr - 1;
        if sequence_nr > highest_sent {
            return Err(RemoteError::InvalidReliableSystemDelivery(format!(
                "reply sequence {sequence_nr} exceeds highest sent {highest_sent}"
            )));
        }
        Ok(())
    }

    fn clear_through(&mut self, sequence_nr: u64) -> usize {
        let mut removed = 0;
        while self
            .unacknowledged
            .front()
            .is_some_and(|envelope| envelope.sequence_nr <= sequence_nr)
        {
            self.unacknowledged.pop_front();
            removed += 1;
        }
        removed
    }
}

/// Receiver decision for one reliable system envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReliableSystemReceiveOutcome {
    /// The next expected envelope should be delivered exactly once.
    Deliver {
        /// Original serialized system envelope to deliver.
        envelope: Box<RemoteEnvelope>,
        /// Cumulative acknowledgement to return after delivery succeeds.
        ack: ReliableSystemAck,
    },
    /// The envelope was already observed and must not be delivered again.
    Duplicate {
        /// Cumulative acknowledgement of the highest contiguous sequence.
        ack: ReliableSystemAck,
    },
    /// The envelope arrived ahead of the next expected sequence.
    Gap {
        /// Negative acknowledgement identifying the highest contiguous sequence.
        nack: ReliableSystemNack,
    },
}

/// Receiver-side ordering and deduplication state for one remote actor-system
/// incarnation.
#[derive(Debug, Clone)]
pub struct ReliableSystemReceiver {
    local_uid: u64,
    remote_uid: u64,
    next_expected_sequence_nr: u64,
}

impl ReliableSystemReceiver {
    /// Creates receiver state expecting sequence number one from `remote_uid`.
    pub fn new(local_uid: u64, remote_uid: u64) -> Self {
        Self {
            local_uid,
            remote_uid,
            next_expected_sequence_nr: 1,
        }
    }

    /// Returns the next sequence number eligible for delivery.
    pub fn next_expected_sequence_nr(&self) -> u64 {
        self.next_expected_sequence_nr
    }

    /// Returns the sending remote actor-system incarnation.
    pub fn remote_uid(&self) -> u64 {
        self.remote_uid
    }

    /// Validates association incarnations and classifies one envelope as the
    /// next delivery, a duplicate, or a gap.
    ///
    /// Only an in-order envelope advances receiver state.
    pub fn receive(
        &mut self,
        reliable: ReliableSystemEnvelope,
    ) -> Result<ReliableSystemReceiveOutcome> {
        if reliable.from_uid != self.remote_uid || reliable.to_uid != self.local_uid {
            return Err(RemoteError::InvalidReliableSystemDelivery(format!(
                "envelope association {}->{} does not match {}->{}",
                reliable.from_uid, reliable.to_uid, self.remote_uid, self.local_uid
            )));
        }
        if reliable.sequence_nr == self.next_expected_sequence_nr {
            self.next_expected_sequence_nr = self
                .next_expected_sequence_nr
                .checked_add(1)
                .ok_or_else(|| {
                    RemoteError::InvalidReliableSystemDelivery(
                        "receiver sequence number overflow".to_string(),
                    )
                })?;
            return Ok(ReliableSystemReceiveOutcome::Deliver {
                envelope: Box::new(reliable.envelope),
                ack: self.ack(),
            });
        }
        if reliable.sequence_nr < self.next_expected_sequence_nr {
            return Ok(ReliableSystemReceiveOutcome::Duplicate { ack: self.ack() });
        }
        Ok(ReliableSystemReceiveOutcome::Gap {
            nack: ReliableSystemNack {
                from_uid: self.local_uid,
                to_uid: self.remote_uid,
                highest_contiguous_sequence_nr: self.next_expected_sequence_nr - 1,
            },
        })
    }

    fn ack(&self) -> ReliableSystemAck {
        ReliableSystemAck {
            from_uid: self.local_uid,
            to_uid: self.remote_uid,
            sequence_nr: self.next_expected_sequence_nr - 1,
        }
    }
}

fn encode_reply(from_uid: u64, to_uid: u64, sequence_nr: u64) -> Bytes {
    let mut writer = WireWriter::new();
    writer.write_u64(from_uid);
    writer.write_u64(to_uid);
    writer.write_u64(sequence_nr);
    writer.finish()
}

fn decode_reply(payload: Bytes) -> kairo_serialization::Result<(u64, u64, u64)> {
    let mut reader = WireReader::new(&payload);
    let reply = (reader.read_u64()?, reader.read_u64()?, reader.read_u64()?);
    reader.ensure_finished()?;
    Ok(reply)
}

fn ensure_version<M: RemoteMessage>(version: u16) -> kairo_serialization::Result<()> {
    if version == M::VERSION {
        Ok(())
    } else {
        Err(kairo_serialization::SerializationError::Message(format!(
            "unsupported {} version {version}",
            M::MANIFEST
        )))
    }
}

#[cfg(test)]
mod tests {
    use kairo_serialization::{ActorRefWireData, Manifest, SerializedMessage};

    use super::*;

    fn envelope(value: u8) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new("kairo://receiver@127.0.0.1:25520/system/target").unwrap(),
            Some(ActorRefWireData::new("kairo://sender@127.0.0.1:25521/system/source").unwrap()),
            SerializedMessage::new(
                9_999,
                Manifest::new("kairo.remote.test.lifecycle"),
                1,
                Bytes::from(vec![value]),
            ),
        )
    }

    #[test]
    fn reliable_protocol_codecs_round_trip_nested_envelope_and_replies() {
        let mut registry = Registry::new();
        register_reliable_system_codecs(&mut registry).unwrap();
        let reliable = ReliableSystemEnvelope {
            from_uid: 11,
            to_uid: 22,
            sequence_nr: 7,
            envelope: envelope(3),
        };
        let ack = ReliableSystemAck {
            from_uid: 22,
            to_uid: 11,
            sequence_nr: 7,
        };
        let nack = ReliableSystemNack {
            from_uid: 22,
            to_uid: 11,
            highest_contiguous_sequence_nr: 6,
        };

        assert_eq!(
            registry
                .deserialize::<ReliableSystemEnvelope>(registry.serialize(&reliable).unwrap())
                .unwrap(),
            reliable
        );
        assert_eq!(
            registry
                .deserialize::<ReliableSystemAck>(registry.serialize(&ack).unwrap())
                .unwrap(),
            ack
        );
        assert_eq!(
            registry
                .deserialize::<ReliableSystemNack>(registry.serialize(&nack).unwrap())
                .unwrap(),
            nack
        );
    }

    #[test]
    fn reliable_protocol_codecs_reject_unknown_versions_and_trailing_bytes() {
        let codec = ReliableSystemEnvelopeCodec;
        let reliable = ReliableSystemEnvelope {
            from_uid: 11,
            to_uid: 22,
            sequence_nr: 1,
            envelope: envelope(1),
        };
        let payload = codec.encode(&reliable).unwrap();
        assert!(codec.decode(payload.clone(), 2).is_err());
        let mut trailing = payload.to_vec();
        trailing.push(0);
        assert!(codec.decode(Bytes::from(trailing), 1).is_err());
    }

    #[test]
    fn sender_retains_in_order_and_cumulatively_acknowledges() {
        let mut sender = ReliableSystemSender::new(11, 22, 3).unwrap();
        assert_eq!(sender.retain(envelope(1)).unwrap().sequence_nr, 1);
        assert_eq!(sender.retain(envelope(2)).unwrap().sequence_nr, 2);
        assert_eq!(sender.retain(envelope(3)).unwrap().sequence_nr, 3);
        assert!(matches!(
            sender.retain(envelope(4)).unwrap_err(),
            RemoteError::ReliableSystemBufferFull { capacity: 3 }
        ));

        assert_eq!(
            sender
                .acknowledge(&ReliableSystemAck {
                    from_uid: 22,
                    to_uid: 11,
                    sequence_nr: 2,
                })
                .unwrap(),
            2
        );
        assert_eq!(sender.pending_len(), 1);
        assert_eq!(sender.retry_batch()[0].sequence_nr, 3);
        assert_eq!(
            sender
                .negative_acknowledge(&ReliableSystemNack {
                    from_uid: 22,
                    to_uid: 11,
                    highest_contiguous_sequence_nr: 2,
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn receiver_delivers_once_and_reports_duplicates_or_gaps() {
        let mut receiver = ReliableSystemReceiver::new(22, 11);
        let first = ReliableSystemEnvelope {
            from_uid: 11,
            to_uid: 22,
            sequence_nr: 1,
            envelope: envelope(1),
        };

        assert!(matches!(
            receiver.receive(first.clone()).unwrap(),
            ReliableSystemReceiveOutcome::Deliver { ack, .. } if ack.sequence_nr == 1
        ));
        assert!(matches!(
            receiver.receive(first).unwrap(),
            ReliableSystemReceiveOutcome::Duplicate { ack } if ack.sequence_nr == 1
        ));
        assert!(matches!(
            receiver
                .receive(ReliableSystemEnvelope {
                    from_uid: 11,
                    to_uid: 22,
                    sequence_nr: 3,
                    envelope: envelope(3),
                })
                .unwrap(),
            ReliableSystemReceiveOutcome::Gap { nack }
                if nack.highest_contiguous_sequence_nr == 1
        ));
        assert_eq!(receiver.next_expected_sequence_nr(), 2);
    }

    #[test]
    fn stale_uid_replies_are_rejected_and_new_uid_resets_sequence_state() {
        let mut sender = ReliableSystemSender::new(11, 22, 2).unwrap();
        sender.retain(envelope(1)).unwrap();
        assert!(matches!(
            sender
                .acknowledge(&ReliableSystemAck {
                    from_uid: 23,
                    to_uid: 11,
                    sequence_nr: 1,
                })
                .unwrap_err(),
            RemoteError::InvalidReliableSystemDelivery(_)
        ));

        let failed = sender.reset_remote_uid(23);
        assert_eq!(failed, vec![envelope(1)]);
        assert_eq!(sender.retain(envelope(2)).unwrap().sequence_nr, 1);
        assert_eq!(sender.remote_uid(), 23);
    }
}
