#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    PreRestart,
    PostStop,
}
