use crate::task::Task;

pub struct Lease {
    pub token: String,
    pub expiry: Instant,
    pub task: Task,
}
