#![deny(missing_docs)]
//! Low-level actors that map aggregation events into client responses.
//!
//! These adapters are useful when an application composes aggregation actors
//! directly. The higher-level aggregation sessions perform the same terminal
//! response mapping while also owning transport publication and read repair.

mod read;
mod response;
mod write;

#[cfg(test)]
mod tests;

pub use read::{
    ReadAggregationOperation, ReadAggregationOperationEvent, ReadAggregationOperationMsg,
};
pub use write::{
    WriteAggregationOperation, WriteAggregationOperationEvent, WriteAggregationOperationMsg,
};

pub(crate) use response::{read_aggregation_response, write_aggregation_response};
