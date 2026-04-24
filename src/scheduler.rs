use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Interval;

#[derive(Debug)]
pub struct Scheduler {
    intervals: HashMap<Duration, Interval>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self { intervals: HashMap::new() }
    }

    pub fn periodic(&mut self, every: Duration) -> &mut Interval {
        self.intervals.entry(every).or_insert_with(|| tokio::time::interval(every))
    }
}

impl Default for Scheduler {
    fn default() -> Self { Self::new() }
}
