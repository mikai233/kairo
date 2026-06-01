use kairo_actor::{ActorError, ActorRef, ActorResult};

use crate::{
    DeltaReplicatedData, GetResponse, ReadAggregationOutcome, ReplicatedDelta, ReplicatorKey,
    UpdateOutcome, UpdateResponse, WriteAggregationOutcome,
};

pub(crate) fn write_aggregation_response<Delta>(
    key: &ReplicatorKey,
    outcome: &mut Option<UpdateOutcome<Delta>>,
    aggregation: WriteAggregationOutcome,
) -> UpdateResponse<Delta>
where
    Delta: ReplicatedDelta + Send + 'static,
{
    match aggregation {
        WriteAggregationOutcome::InProgress => UpdateResponse::Failure {
            key: key.clone(),
            reason: "write aggregation completed with non-terminal state".to_string(),
        },
        WriteAggregationOutcome::Success => {
            if let Some(outcome) = outcome.take() {
                UpdateResponse::Success(outcome)
            } else {
                UpdateResponse::Failure {
                    key: key.clone(),
                    reason: "write aggregation success was reported more than once".to_string(),
                }
            }
        }
        WriteAggregationOutcome::Failed {
            required,
            available,
        } => UpdateResponse::Failure {
            key: key.clone(),
            reason: format!(
                "write quorum failed: required {required} remote acknowledgements, \
                 only {available} replicas remain available"
            ),
        },
        WriteAggregationOutcome::Timeout { .. } => UpdateResponse::Timeout { key: key.clone() },
    }
}

pub(crate) fn read_aggregation_response<D>(
    key: &ReplicatorKey,
    outcome: ReadAggregationOutcome<D>,
) -> GetResponse<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    match outcome {
        ReadAggregationOutcome::InProgress => GetResponse::Failure {
            key: key.clone(),
            reason: "read aggregation completed with non-terminal state".to_string(),
        },
        ReadAggregationOutcome::Success { envelope } => GetResponse::Success {
            key: key.clone(),
            data: envelope.into_data(),
        },
        ReadAggregationOutcome::NotFound => GetResponse::NotFound { key: key.clone() },
        ReadAggregationOutcome::Failure { required, received } => GetResponse::Failure {
            key: key.clone(),
            reason: format!(
                "read quorum failed: required {required} remote replies, \
                 received {received}"
            ),
        },
    }
}

pub(super) fn tell_or_actor_error<M>(target: &ActorRef<M>, message: M) -> ActorResult
where
    M: Send + 'static,
{
    target
        .tell(message)
        .map_err(|error| ActorError::Message(error.reason().to_string()))
}
