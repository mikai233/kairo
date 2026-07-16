#![deny(missing_docs)]
//! Delivery targets, fan-out, and diagnostics for quorum aggregation requests.

mod report;
mod target;
mod transport;

pub use report::{
    AggregationTransportFailure, AggregationTransportOperation, AggregationTransportReport,
};
pub use target::{AggregationTarget, AggregationTargetRegistry, SenderAwareRecipient};
pub use transport::AggregationTransport;
