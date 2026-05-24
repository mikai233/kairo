use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrdtError {
    CounterOverflow,
    CounterValueOutOfRange,
}

impl Display for CrdtError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::CounterOverflow => f.write_str("counter value overflowed"),
            Self::CounterValueOutOfRange => {
                f.write_str("counter value is outside the supported signed range")
            }
        }
    }
}

impl std::error::Error for CrdtError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsistencyError {
    ReplicaCountTooSmall { requested: usize },
}

impl Display for ConsistencyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplicaCountTooSmall { requested } => write!(
                f,
                "replica count {requested} is too small; use local consistency for one replica"
            ),
        }
    }
}

impl std::error::Error for ConsistencyError {}
