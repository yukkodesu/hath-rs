use std::sync::Mutex;
use std::time::Duration;

const TIME_RESOLUTION: usize = 50;
const WINDOW_LENGTH: usize = 5;
const MILLIS_PER_TICK: u64 = 20;

#[derive(Debug)]
pub struct BandwidthMonitor {
    bytes_per_tick: u32,
    millis_per_tick: u64,
    inner: Mutex<BwmInner>,
}

#[derive(Debug)]
struct BwmInner {
    tick_bytes: [u32; TIME_RESOLUTION],
    tick_seconds: [u64; TIME_RESOLUTION],
}

impl BandwidthMonitor {
    pub fn new(throttle_bytes_per_sec: u32) -> Self {
        let bytes_per_tick = (throttle_bytes_per_sec as f64 / TIME_RESOLUTION as f64).ceil() as u32;
        Self {
            bytes_per_tick,
            millis_per_tick: MILLIS_PER_TICK,
            inner: Mutex::new(BwmInner {
                tick_bytes: [0u32; TIME_RESOLUTION],
                tick_seconds: [0u64; TIME_RESOLUTION],
            }),
        }
    }

    /// Wait until there is enough quota for `byte_count` bytes.
    pub async fn wait_for_quota(&self, byte_count: usize) {
        let byte_count = byte_count as u32;
        loop {
            let release = {
                let mut inner = self.inner.lock().unwrap();
                let now_millis = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let epoch_seconds = now_millis / 1000;
                let current_tick = ((now_millis - epoch_seconds * 1000) / self.millis_per_tick) as usize;

                let mut bytes_this_tick = 0u32;
                let mut bytes_last_window = 0u32;
                let mut bytes_last_second = 0u32;

                for offset in 0..TIME_RESOLUTION {
                    let tick_counter = current_tick as isize - TIME_RESOLUTION as isize + 1 + offset as isize;
                    let tick_index = if tick_counter < 0 { (TIME_RESOLUTION as isize + tick_counter) as usize } else { tick_counter as usize };
                    let valid_second = if tick_counter < 0 { epoch_seconds.wrapping_sub(1) } else { epoch_seconds };

                    if inner.tick_seconds[tick_index] == valid_second {
                        if tick_counter == current_tick as isize {
                            bytes_this_tick += inner.tick_bytes[tick_index];
                        } else {
                            if tick_counter >= current_tick as isize - WINDOW_LENGTH as isize {
                                bytes_last_window += inner.tick_bytes[tick_index];
                            }
                            bytes_last_second += inner.tick_bytes[tick_index];
                        }
                    }
                }

                let exceeded = bytes_this_tick as f64 > self.bytes_per_tick as f64 * 1.1
                    || bytes_last_window as f64 > self.bytes_per_tick as f64 * WINDOW_LENGTH as f64 * 1.05
                    || bytes_last_second > self.bytes_per_tick * TIME_RESOLUTION as u32;

                if !exceeded {
                    if inner.tick_seconds[current_tick] != epoch_seconds {
                        inner.tick_seconds[current_tick] = epoch_seconds;
                        inner.tick_bytes[current_tick] = 0;
                    }
                    inner.tick_bytes[current_tick] = inner.tick_bytes[current_tick].wrapping_add(byte_count);
                    true
                } else {
                    false
                }
            };

            if release { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_under_limit_grants() {
        let bwm = BandwidthMonitor::new(10_000_000);
        for _ in 0..50 {
            bwm.wait_for_quota(1460).await;
        }
    }
}
