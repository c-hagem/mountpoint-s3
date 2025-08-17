//! Simple latency tracking using HDR Histogram
//!
//! This module provides low-overhead latency tracking for filesystem read operations.
//! When disabled, it has zero performance impact. When enabled, it uses HDR Histogram
//! to accurately capture latency distributions and periodically writes statistics to a file.
//!
//! # Usage
//!
//! Enable latency tracking by setting the `MOUNTPOINT_LATENCY_FILE` environment variable:
//! ```bash
//! export MOUNTPOINT_LATENCY_FILE=/tmp/read_latencies.csv
//! ```
//!
//! The output file will contain periodic statistics with columns:
//! - timestamp: Unix timestamp when stats were written
//! - samples: Number of samples in this period
//! - min_ns, mean_ns, p50_ns, p95_ns, p99_ns, p999_ns, max_ns: Latency percentiles in nanoseconds
//!
//! # Example Integration
//!
//! ```rust
//! use crate::latency::{LatencyConfig, LatencyTracker};
//! use std::time::Duration;
//!
//! let config = LatencyConfig::default(); // Reads from env vars
//! let tracker = LatencyTracker::new(config);
//!
//! // In your read path:
//! if tracker.is_enabled() {
//!     let start = std::time::Instant::now();
//!     let result = perform_read().await;
//!     tracker.record(read_size, start.elapsed());
//!     result
//! } else {
//!     perform_read().await
//! }
//! ```
//!
//! Or use the convenience macro:
//! ```rust
//! let result = track_read_latency!(tracker, read_size, {
//!     perform_read().await
//! });
//! ```

use hdrhistogram::Histogram;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;

/// Simple latency configuration
#[derive(Debug, Clone)]
pub struct LatencyConfig {
    pub enabled: bool,
    pub file_path: Option<String>,
}

impl Default for LatencyConfig {
    fn default() -> Self {
        Self {
            enabled: std::env::var("MOUNTPOINT_LATENCY_FILE").is_ok(),
            file_path: std::env::var("MOUNTPOINT_LATENCY_FILE").ok(),
        }
    }
}

/// Simple latency tracker using HDR Histogram
pub struct LatencyTracker {
    enabled: AtomicBool,
    histogram: Arc<Mutex<Histogram<u64>>>,
    writer: Arc<Mutex<Option<BufWriter<std::fs::File>>>>,
    sample_count: AtomicUsize,
    file_path: Option<String>,
}

impl LatencyTracker {
    /// Create a new latency tracker
    pub fn new(config: LatencyConfig) -> Self {
        // Create HDR histogram with range from 1μs to 60s, 3 significant digits
        let histogram = Histogram::new_with_bounds(1_000, 60_000_000_000, 3)
            .unwrap_or_else(|_| Histogram::new(3).expect("Failed to create histogram"));

        let writer = if config.enabled && config.file_path.is_some() {
            config.file_path.as_ref().and_then(|path| {
                match std::fs::File::create(path) {
                    Ok(file) => {
                        let mut buf_writer = BufWriter::new(file);
                        // Write header for statistics
                        if let Err(e) = writeln!(
                            buf_writer,
                            "timestamp,samples,min_ns,mean_ns,p50_ns,p95_ns,p99_ns,p999_ns,max_ns"
                        ) {
                            warn!("Failed to write latency file header: {}", e);
                            None
                        } else {
                            Some(buf_writer)
                        }
                    }
                    Err(e) => {
                        warn!("Failed to create latency file {}: {}", path, e);
                        None
                    }
                }
            })
        } else {
            None
        };

        Self {
            enabled: AtomicBool::new(config.enabled),
            histogram: Arc::new(Mutex::new(histogram)),
            writer: Arc::new(Mutex::new(writer)),
            sample_count: AtomicUsize::new(0),
            file_path: config.file_path,
        }
    }

    /// Record a latency measurement
    #[inline]
    pub fn record(&self, size: u32, latency: Duration) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        let latency_ns = latency.as_nanos() as u64;
        if latency_ns == 0 {
            return;
        }

        // Record to histogram (non-blocking)
        if let Ok(mut hist) = self.histogram.try_lock() {
            if let Err(e) = hist.record(latency_ns) {
                warn!("Failed to record latency in histogram: {}", e);
            }
        }

        // Every 1000 samples, write statistics to file
        let count = self.sample_count.fetch_add(1, Ordering::Relaxed);
        if count % 1000 == 0 && count > 0 {
            self.write_stats();
        }
    }

    /// Write current statistics to file
    fn write_stats(&self) {
        if let (Ok(hist), Ok(mut writer_guard)) = (self.histogram.try_lock(), self.writer.try_lock()) {
            if let Some(ref mut writer) = writer_guard.as_mut() {
                if hist.len() > 0 {
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();

                    if let Err(e) = writeln!(
                        writer,
                        "{},{},{},{:.0},{},{},{},{},{}",
                        timestamp,
                        hist.len(),
                        hist.min(),
                        hist.mean(),
                        hist.value_at_quantile(0.5),
                        hist.value_at_quantile(0.95),
                        hist.value_at_quantile(0.99),
                        hist.value_at_quantile(0.999),
                        hist.max()
                    ) {
                        warn!("Failed to write latency statistics: {}", e);
                    } else {
                        let _ = writer.flush();
                    }
                }
            }
        }
    }

    /// Flush any buffered data and write final statistics
    pub fn flush(&self) {
        // Write final statistics
        self.write_stats();

        if let Ok(mut writer_guard) = self.writer.lock() {
            if let Some(ref mut writer) = writer_guard.as_mut() {
                let _ = writer.flush();
            }
        }
    }

    /// Get current statistics
    pub fn get_stats(&self) -> Option<LatencyStats> {
        if let Ok(hist) = self.histogram.lock() {
            if hist.len() > 0 {
                return Some(LatencyStats {
                    samples: hist.len(),
                    min_ns: hist.min(),
                    mean_ns: hist.mean(),
                    p50_ns: hist.value_at_quantile(0.5),
                    p95_ns: hist.value_at_quantile(0.95),
                    p99_ns: hist.value_at_quantile(0.99),
                    p999_ns: hist.value_at_quantile(0.999),
                    max_ns: hist.max(),
                });
            }
        }
        None
    }

    /// Check if tracking is enabled
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Get the configured file path
    pub fn file_path(&self) -> Option<&str> {
        self.file_path.as_deref()
    }

    /// Get current sample count
    pub fn sample_count(&self) -> usize {
        self.sample_count.load(Ordering::Relaxed)
    }

    /// Reset all statistics (useful for testing)
    pub fn reset(&self) {
        if let Ok(mut hist) = self.histogram.lock() {
            hist.reset();
        }
        self.sample_count.store(0, Ordering::Relaxed);
    }
}

/// Latency statistics
#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub samples: u64,
    pub min_ns: u64,
    pub mean_ns: f64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

impl LatencyStats {
    /// Convert nanoseconds to milliseconds for easier reading
    pub fn min_ms(&self) -> f64 {
        self.min_ns as f64 / 1_000_000.0
    }

    pub fn mean_ms(&self) -> f64 {
        self.mean_ns / 1_000_000.0
    }

    pub fn p50_ms(&self) -> f64 {
        self.p50_ns as f64 / 1_000_000.0
    }

    pub fn p95_ms(&self) -> f64 {
        self.p95_ns as f64 / 1_000_000.0
    }

    pub fn p99_ms(&self) -> f64 {
        self.p99_ns as f64 / 1_000_000.0
    }

    pub fn p999_ms(&self) -> f64 {
        self.p999_ns as f64 / 1_000_000.0
    }

    pub fn max_ms(&self) -> f64 {
        self.max_ns as f64 / 1_000_000.0
    }

    /// Print a human-readable summary
    pub fn summary(&self) -> String {
        format!(
            "Samples: {}, Min: {:.2}ms, Mean: {:.2}ms, P50: {:.2}ms, P95: {:.2}ms, P99: {:.2}ms, Max: {:.2}ms",
            self.samples,
            self.min_ms(),
            self.mean_ms(),
            self.p50_ms(),
            self.p95_ms(),
            self.p99_ms(),
            self.max_ms()
        )
    }
}

impl Drop for LatencyTracker {
    fn drop(&mut self) {
        self.flush();
    }
}

/// Zero-overhead recording macro
#[macro_export]
macro_rules! track_read_latency {
    ($tracker:expr, $size:expr, $body:expr) => {{
        if $tracker.is_enabled() {
            let start = std::time::Instant::now();
            let result = $body;
            $tracker.record($size, start.elapsed());
            result
        } else {
            $body
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_disabled_tracker() {
        let config = LatencyConfig {
            enabled: false,
            file_path: None,
        };
        let tracker = LatencyTracker::new(config);
        assert!(!tracker.is_enabled());

        // Should be no-op
        tracker.record(1024, Duration::from_millis(10));
    }

    #[test]
    fn test_macro() {
        let config = LatencyConfig {
            enabled: false,
            file_path: None,
        };
        let tracker = LatencyTracker::new(config);

        let result = track_read_latency!(tracker, 1024, {
            // Simulate some work
            42
        });

        assert_eq!(result, 42);
    }

    #[test]
    fn test_enabled_tracker() {
        let config = LatencyConfig {
            enabled: true,
            file_path: None, // No file output for test
        };
        let tracker = LatencyTracker::new(config);
        assert!(tracker.is_enabled());

        // Record some latencies
        tracker.record(1024, Duration::from_millis(10));
        tracker.record(2048, Duration::from_millis(20));
        tracker.record(4096, Duration::from_millis(5));

        let stats = tracker.get_stats().unwrap();
        assert_eq!(stats.samples, 3);
        assert!(stats.min_ns > 0);
        assert!(stats.max_ns > stats.min_ns);
    }
}
