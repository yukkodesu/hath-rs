use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicI64, Ordering};
use std::sync::RwLock;
use std::time::Instant;

#[derive(Debug)]
pub struct Stats {
    pub client_running: AtomicBool,
    pub client_suspended: AtomicBool,
    pub program_start_time: RwLock<Option<Instant>>,
    pub last_server_contact: AtomicI64,
    pub files_sent: AtomicU64,
    pub files_rcvd: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_rcvd: AtomicU64,
    pub cache_count: AtomicU32,
    pub cache_size: AtomicU64,
    pub open_connections: AtomicU32,
    pub bytes_sent_history: RwLock<Vec<u32>>,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            client_running: AtomicBool::new(false),
            client_suspended: AtomicBool::new(false),
            program_start_time: RwLock::new(None),
            last_server_contact: AtomicI64::new(0),
            files_sent: AtomicU64::new(0),
            files_rcvd: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_rcvd: AtomicU64::new(0),
            cache_count: AtomicU32::new(0),
            cache_size: AtomicU64::new(0),
            open_connections: AtomicU32::new(0),
            bytes_sent_history: RwLock::new(vec![0u32; 361]),
        }
    }

    pub fn program_started(&self) {
        if let Ok(mut t) = self.program_start_time.write() {
            *t = Some(Instant::now());
        }
        self.client_running.store(true, Ordering::SeqCst);
    }

    pub fn record_server_contact(&self) {
        self.last_server_contact
            .store(chrono::Utc::now().timestamp(), Ordering::SeqCst);
    }

    pub fn record_file_sent(&self)       { self.files_sent.fetch_add(1, Ordering::Relaxed); }
    pub fn record_file_rcvd(&self)       { self.files_rcvd.fetch_add(1, Ordering::Relaxed); }

    pub fn record_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        if self.client_running.load(Ordering::Relaxed)
            && let Ok(mut hist) = self.bytes_sent_history.write() {
                hist[0] = hist[0].wrapping_add(bytes as u32);
            }
    }

    pub fn record_bytes_rcvd(&self, bytes: u64) {
        self.bytes_rcvd.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn set_open_connections(&self, count: u32) { self.open_connections.store(count, Ordering::Relaxed); }
    pub fn set_cache_count(&self, count: u32)      { self.cache_count.store(count, Ordering::Relaxed); }
    pub fn set_cache_size(&self, size: u64)         { self.cache_size.store(size, Ordering::Relaxed); }

    pub fn shift_bytes_sent_history(&self) {
        if let Ok(mut hist) = self.bytes_sent_history.write() {
            for i in (1..361).rev() {
                hist[i] = hist[i - 1];
            }
            hist[0] = 0;
        }
    }

    pub fn get_uptime_secs(&self) -> f64 {
        self.program_start_time
            .read()
            .ok()
            .and_then(|t| t.as_ref().map(|inst| inst.elapsed().as_secs_f64()))
            .unwrap_or(0.0)
    }

    pub fn get_avg_bytes_sent_per_sec(&self) -> u64 {
        let uptime = self.get_uptime_secs();
        if uptime > 0.0 {
            (self.bytes_sent.load(Ordering::Relaxed) as f64 / uptime) as u64
        } else {
            0
        }
    }
}

impl Default for Stats {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_stats_are_zeroed() {
        let s = Stats::new();
        assert!(!s.client_running.load(Ordering::Relaxed));
        assert_eq!(s.files_sent.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_record_file_sent_increments() {
        let s = Stats::new();
        s.record_file_sent();
        s.record_file_sent();
        assert_eq!(s.files_sent.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_shift_bytes_sent_history() {
        let s = Stats::new();
        s.bytes_sent_history.write().unwrap()[0] = 42;
        s.shift_bytes_sent_history();
        let hist = s.bytes_sent_history.read().unwrap();
        assert_eq!(hist[0], 0);
        assert_eq!(hist[1], 42);
    }

    #[test]
    fn test_program_started_sets_instant() {
        let s = Stats::new();
        s.program_started();
        assert!(s.client_running.load(Ordering::Relaxed));
        assert!(s.program_start_time.read().unwrap().is_some());
    }
}
