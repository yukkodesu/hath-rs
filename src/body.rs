//! Streaming HTTP body with optional per-chunk bandwidth throttling.
//!
//! Java equivalent: HTTPSession.write body loop + HTTPBandwidthMonitor.waitForQuota
//! Chunk size: 1460 bytes (Java Settings.TCP_PACKET_SIZE)

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::bandwidth::BandwidthMonitor;

/// Java Settings.TCP_PACKET_SIZE = 1460
const TCP_PACKET_SIZE: usize = 1460;

/// A Hyper [`Body`] that streams data in chunks, with optional per-chunk
/// bandwidth throttling via [`BandwidthMonitor`].
///
/// When `bwm` is [`None`], the entire data is returned in a single frame
/// (equivalent to [`http_body_util::Full`]).
///
/// When `bwm` is [`Some`], data is split into 1460-byte chunks and each chunk
/// is throttled via [`BandwidthMonitor::wait_for_quota`] before being yielded.
pub struct StreamingBody {
    data: Bytes,
    offset: usize,
    total_size: usize,
    bwm: Option<Arc<BandwidthMonitor>>,
    /// Pending throttle future. `None` means no throttle is in flight.
    throttle_fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl StreamingBody {
    /// Create a new streaming body.
    ///
    /// * `data` - The full response body bytes.
    /// * `bwm`  - Optional bandwidth monitor for per-chunk throttling.
    ///   When `None`, the entire body is yielded in one frame.
    pub fn new(data: Vec<u8>, bwm: Option<Arc<BandwidthMonitor>>) -> Self {
        let total_size = data.len();
        Self {
            data: Bytes::from(data),
            offset: 0,
            total_size,
            bwm,
            throttle_fut: None,
        }
    }
}

impl Body for StreamingBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        loop {
            // Step 1: Poll pending throttle future (if any).
            // If it just completed, record that so we produce the chunk below
            // instead of starting another throttle for the same data.
            let mut throttled = false;
            if let Some(ref mut fut) = self.throttle_fut {
                match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        self.throttle_fut = None;
                        throttled = true;
                    }
                }
            }

            // Step 2: Check if done
            if self.offset >= self.total_size {
                return Poll::Ready(None);
            }

            // Step 3: If throttling is active and we haven't just completed a
            // throttle cycle, start a new one for the next chunk.
            if !throttled && self.bwm.is_some() && self.throttle_fut.is_none() {
                let chunk_size = TCP_PACKET_SIZE.min(self.total_size - self.offset);
                let bwm = self.bwm.clone().unwrap();
                self.throttle_fut = Some(Box::pin(async move {
                    bwm.wait_for_quota(chunk_size).await;
                }));
                continue; // Loop back to poll the new throttle future
            }

            // Step 4: Produce the chunk whose throttle just completed
            // (or all remaining data if throttling is disabled).
            let chunk_size = if self.bwm.is_some() {
                TCP_PACKET_SIZE
            } else {
                self.total_size - self.offset
            };
            let end = (self.offset + chunk_size).min(self.total_size);
            let chunk = self.data.slice(self.offset..end);
            self.offset = end;

            return Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
    }

    fn size_hint(&self) -> SizeHint {
        let mut hint = SizeHint::new();
        hint.set_exact(self.total_size as u64);
        hint
    }
}
