#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SupervisorStrategy {
    #[default]
    Stop,
    Resume,
    Restart,
}
