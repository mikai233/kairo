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
