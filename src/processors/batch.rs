use std::time::{Duration, Instant};

use crate::signal::Signal;

/// Time- and size-based batcher.
///
/// Call `push(signal)` — returns `Some(batch)` when the batch should be
/// flushed (size limit hit or time limit exceeded).
/// Call `drain()` at shutdown to get any remaining signals.
pub struct BatchProcessor {
    max_size: usize,
    max_delay: Duration,
    buf: Vec<Signal>,
    last_flush: Instant,
}

impl BatchProcessor {
    pub fn new(max_size: usize, max_delay_secs: u64) -> Self {
        Self {
            max_size,
            max_delay: Duration::from_secs(max_delay_secs),
            buf: Vec::with_capacity(max_size),
            last_flush: Instant::now(),
        }
    }

    /// Push a signal. Returns `Some(batch)` if the batch should be flushed.
    pub fn push(&mut self, signal: Signal) -> Option<Vec<Signal>> {
        self.buf.push(signal);

        let size_trigger = self.buf.len() >= self.max_size;
        let time_trigger = self.last_flush.elapsed() >= self.max_delay;

        if size_trigger || time_trigger {
            Some(self.flush())
        } else {
            None
        }
    }

    /// Drain any remaining signals (call at shutdown).
    pub fn drain(&mut self) -> Option<Vec<Signal>> {
        if self.buf.is_empty() {
            None
        } else {
            Some(self.flush())
        }
    }

    fn flush(&mut self) -> Vec<Signal> {
        self.last_flush = Instant::now();
        std::mem::replace(&mut self.buf, Vec::with_capacity(self.max_size))
    }
}
