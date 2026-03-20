use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, TrySendError},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub struct JsonlJournal {
    path: PathBuf,
    run_id: String,
    next_seq: AtomicU64,
    sender: mpsc::SyncSender<WriterCommand>,
    worker: Mutex<Option<JoinHandle<Result<()>>>>,
    sync_lock: Mutex<()>,
    metrics: Arc<JournalRuntimeMetrics>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JournalRecord {
    #[serde(default)]
    pub seq: u64,
    #[serde(default)]
    pub run_id: String,
    pub ts_ms: i64,
    pub kind: String,
    pub payload: serde_json::Value,
}

enum WriterCommand {
    Append(Vec<u8>),
    AppendAndFlush(Vec<u8>, mpsc::Sender<Result<()>>),
    Flush(mpsc::Sender<Result<()>>),
    Shutdown(mpsc::Sender<Result<()>>),
}

#[derive(Debug, Default)]
struct JournalRuntimeMetrics {
    async_appends: AtomicU64,
    critical_appends: AtomicU64,
    sync_fallback_appends: AtomicU64,
    dropped_async_appends: AtomicU64,
    flush_requests: AtomicU64,
    writer_flushes: AtomicU64,
    writer_failures: AtomicU64,
    queue_disconnects: AtomicU64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct JournalRuntimeMetricsSnapshot {
    pub async_appends: u64,
    pub critical_appends: u64,
    pub sync_fallback_appends: u64,
    pub dropped_async_appends: u64,
    pub flush_requests: u64,
    pub writer_flushes: u64,
    pub writer_failures: u64,
    pub queue_disconnects: u64,
}

impl JsonlJournal {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self::with_capacity(path, 4_096)
    }

    pub fn with_capacity(path: impl AsRef<Path>, async_queue_capacity: usize) -> Self {
        let path = path.as_ref().to_path_buf();
        let (sender, receiver) = mpsc::sync_channel(async_queue_capacity.max(1));
        let worker_path = path.clone();
        let metrics = Arc::new(JournalRuntimeMetrics::default());
        let worker_metrics = Arc::clone(&metrics);
        let worker = thread::spawn(move || writer_loop(worker_path, receiver, worker_metrics));

        Self {
            path,
            run_id: format!(
                "lightfee-{}-{}",
                chrono::Utc::now().timestamp_millis(),
                std::process::id()
            ),
            next_seq: AtomicU64::new(1),
            sender,
            worker: Mutex::new(Some(worker)),
            sync_lock: Mutex::new(()),
            metrics,
        }
    }

    pub fn append<T: Serialize>(&self, ts_ms: i64, kind: &str, payload: &T) -> Result<()> {
        self.write_record(ts_ms, kind, payload, false)
    }

    pub fn append_critical<T: Serialize>(&self, ts_ms: i64, kind: &str, payload: &T) -> Result<()> {
        self.write_record(ts_ms, kind, payload, true)
    }

    pub fn flush(&self) -> Result<()> {
        self.metrics.flush_requests.fetch_add(1, Ordering::Relaxed);
        let (ack_tx, ack_rx) = mpsc::channel();
        if self.sender.send(WriterCommand::Flush(ack_tx)).is_err() {
            self.metrics
                .queue_disconnects
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        ack_rx
            .recv()
            .context("journal flush acknowledgement dropped")?
    }

    pub fn read_records(&self) -> Result<Vec<JournalRecord>> {
        let _ = self.flush();
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let raw = fs::read_to_string(&self.path)
            .with_context(|| format!("failed to read journal {}", self.path.display()))?;
        raw.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<JournalRecord>(line).with_context(|| {
                    format!("failed to parse journal record in {}", self.path.display())
                })
            })
            .collect()
    }

    pub fn shutdown(&self) -> Result<()> {
        let (ack_tx, ack_rx) = mpsc::channel();
        if self.sender.send(WriterCommand::Shutdown(ack_tx)).is_err() {
            self.metrics
                .queue_disconnects
                .fetch_add(1, Ordering::Relaxed);
        }
        let _ = ack_rx.recv();

        if let Some(handle) = self.worker.lock().expect("lock").take() {
            match handle.join() {
                Ok(result) => result?,
                Err(_) => anyhow::bail!("journal worker thread panicked"),
            }
        }
        Ok(())
    }

    pub fn metrics_snapshot(&self) -> JournalRuntimeMetricsSnapshot {
        JournalRuntimeMetricsSnapshot {
            async_appends: self.metrics.async_appends.load(Ordering::Relaxed),
            critical_appends: self.metrics.critical_appends.load(Ordering::Relaxed),
            sync_fallback_appends: self.metrics.sync_fallback_appends.load(Ordering::Relaxed),
            dropped_async_appends: self.metrics.dropped_async_appends.load(Ordering::Relaxed),
            flush_requests: self.metrics.flush_requests.load(Ordering::Relaxed),
            writer_flushes: self.metrics.writer_flushes.load(Ordering::Relaxed),
            writer_failures: self.metrics.writer_failures.load(Ordering::Relaxed),
            queue_disconnects: self.metrics.queue_disconnects.load(Ordering::Relaxed),
        }
    }

    fn write_record<T: Serialize>(
        &self,
        ts_ms: i64,
        kind: &str,
        payload: &T,
        critical: bool,
    ) -> Result<()> {
        let record = JournalRecord {
            seq: self.next_seq.fetch_add(1, Ordering::Relaxed),
            run_id: self.run_id.clone(),
            ts_ms,
            kind: kind.to_string(),
            payload: serde_json::to_value(payload)?,
        };
        let mut line = serde_json::to_vec(&record)?;
        line.push(b'\n');

        if critical {
            self.metrics
                .critical_appends
                .fetch_add(1, Ordering::Relaxed);
            let (ack_tx, ack_rx) = mpsc::channel();
            if self
                .sender
                .send(WriterCommand::AppendAndFlush(line.clone(), ack_tx))
                .is_ok()
            {
                return ack_rx
                    .recv()
                    .context("journal critical append acknowledgement dropped")?;
            }
            self.metrics
                .queue_disconnects
                .fetch_add(1, Ordering::Relaxed);
            return self.append_sync_line(&line, true, true);
        }

        self.metrics.async_appends.fetch_add(1, Ordering::Relaxed);
        match self.sender.try_send(WriterCommand::Append(line)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.metrics
                    .dropped_async_appends
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Disconnected(command)) => {
                self.metrics
                    .queue_disconnects
                    .fetch_add(1, Ordering::Relaxed);
                let line = match command {
                    WriterCommand::Append(line) => line,
                    _ => Vec::new(),
                };
                self.append_sync_line(&line, false, true)
            }
        }
    }

    fn append_sync_line(&self, line: &[u8], flush: bool, fallback: bool) -> Result<()> {
        let _guard = self.sync_lock.lock().expect("lock");
        if fallback {
            self.metrics
                .sync_fallback_appends
                .fetch_add(1, Ordering::Relaxed);
        }
        ensure_parent(&self.path)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("failed to open journal {}", self.path.display()))?;
        if let Err(error) = file.write_all(line) {
            self.metrics.writer_failures.fetch_add(1, Ordering::Relaxed);
            return Err(error.into());
        }
        if flush {
            if let Err(error) = file.flush() {
                self.metrics.writer_failures.fetch_add(1, Ordering::Relaxed);
                return Err(error.into());
            }
            self.metrics.writer_flushes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

impl Drop for JsonlJournal {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn writer_loop(
    path: PathBuf,
    receiver: mpsc::Receiver<WriterCommand>,
    metrics: Arc<JournalRuntimeMetrics>,
) -> Result<()> {
    let result = writer_loop_impl(path, receiver, &metrics);
    if result.is_err() {
        metrics.writer_failures.fetch_add(1, Ordering::Relaxed);
    }
    result
}

fn writer_loop_impl(
    path: PathBuf,
    receiver: mpsc::Receiver<WriterCommand>,
    metrics: &JournalRuntimeMetrics,
) -> Result<()> {
    ensure_parent(&path)?;
    let mut writer = open_writer(&path)?;
    let flush_interval = Duration::from_millis(200);
    let mut dirty = false;

    loop {
        match receiver.recv_timeout(flush_interval) {
            Ok(command) => match command {
                WriterCommand::Append(line) => {
                    writer.write_all(&line)?;
                    dirty = true;
                }
                WriterCommand::AppendAndFlush(line, ack) => {
                    let result = (|| -> Result<()> {
                        writer.write_all(&line)?;
                        writer.flush()?;
                        metrics.writer_flushes.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    })();
                    let _ = ack.send(
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| anyhow::anyhow!(error.to_string())),
                    );
                    result?;
                    dirty = false;
                }
                WriterCommand::Flush(ack) => {
                    let result = writer.flush().map_err(anyhow::Error::from);
                    if result.is_ok() {
                        metrics.writer_flushes.fetch_add(1, Ordering::Relaxed);
                    }
                    let _ = ack.send(
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| anyhow::anyhow!(error.to_string())),
                    );
                    result?;
                    dirty = false;
                }
                WriterCommand::Shutdown(ack) => {
                    let result = writer.flush().map_err(anyhow::Error::from);
                    if result.is_ok() {
                        metrics.writer_flushes.fetch_add(1, Ordering::Relaxed);
                    }
                    let _ = ack.send(
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| anyhow::anyhow!(error.to_string())),
                    );
                    result?;
                    return Ok(());
                }
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if dirty {
                    writer.flush()?;
                    metrics.writer_flushes.fetch_add(1, Ordering::Relaxed);
                    dirty = false;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if dirty {
                    writer.flush()?;
                    metrics.writer_flushes.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(());
            }
        }
    }
}

fn open_writer(path: &Path) -> Result<BufWriter<File>> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open journal {}", path.display()))?;
    Ok(BufWriter::new(file))
}

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    Ok(())
}
