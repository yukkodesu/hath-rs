use crate::config::Config;
use arc_swap::ArcSwap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing_subscriber::{EnvFilter, Registry, filter, fmt, prelude::*};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug = 1,
    Info = 2,
    Warning = 4,
    Error = 8,
}

pub fn init_logging(log_dir: &Path, config: Arc<ArcSwap<Config>>) -> std::io::Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    subscriber(log_dir, config, std::io::stdout, env_filter)?
        .try_init()
        .map_err(std::io::Error::other)?;
    tracing::info!("Logging started");
    Ok(())
}

fn subscriber<W>(
    log_dir: &Path,
    config: Arc<ArcSwap<Config>>,
    console: W,
    env_filter: EnvFilter,
) -> std::io::Result<impl tracing::Subscriber + Send + Sync>
where
    W: for<'a> fmt::MakeWriter<'a> + Send + Sync + 'static,
{
    let mut file_appender = LineWriter::new(log_dir.join("log_out"), 100_000);
    if !config.load().disable_logging {
        file_appender.open()?;
    }
    let mut err_appender = LineWriter::new(log_dir.join("log_err"), 10_000);
    err_appender.open()?;

    // Config is replaced by every RPC settings update. Do not cache either the
    // snapshot or the callsite decision: the same callsite can be toggled live.
    let file_filter = filter::dynamic_filter_fn(move |_, _| !config.load().disable_logging);
    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_target(false)
        .with_writer(Mutex::new(file_appender))
        .with_filter(file_filter);

    let err_layer = fmt::layer()
        .with_ansi(false)
        .with_target(false)
        .with_writer(Mutex::new(err_appender))
        .with_filter(tracing::level_filters::LevelFilter::WARN);

    let stdout_layer = fmt::layer().with_target(false).with_writer(console);

    Ok(Registry::default()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer)
        .with(err_layer))
}

/// Java Out.log rotates after 100,001 output / 10,001 error lines, retaining
/// only `.old`. The enclosing Mutex serializes an entire tracing event.
struct LineWriter {
    path: PathBuf,
    file: Option<File>,
    lines: usize,
    limit: usize,
    rotate_on_open: bool,
}

impl LineWriter {
    fn new(path: PathBuf, limit: usize) -> Self {
        Self {
            path,
            file: None,
            lines: 0,
            limit,
            rotate_on_open: true,
        }
    }

    fn open(&mut self) -> io::Result<()> {
        if self.file.is_none() {
            if self.rotate_on_open {
                rotate_log(&self.path)?;
                // If opening the new file fails, retry without deleting the backup.
                self.rotate_on_open = false;
            }
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?;
            writeln!(
                file,
                "\n{} Logging started",
                chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ")
            )?;
            self.file = Some(file);
            self.lines = 0;
        }
        Ok(())
    }
}

impl Write for LineWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_all(buf)?;
        Ok(buf.len())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.open()?;
        for line in buf.split_inclusive(|byte| *byte == b'\n') {
            self.file.as_mut().unwrap().write_all(line)?;
            if line.last() == Some(&b'\n') {
                self.lines += 1;
                if self.lines > self.limit {
                    self.flush()?;
                    self.file.take();
                    self.rotate_on_open = true;
                    self.open()?;
                }
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(file) = &mut self.file {
            file.flush()?;
        }
        Ok(())
    }
}

fn rotate_log(path: &Path) -> io::Result<()> {
    let old = path.with_extension("old");
    match std::fs::remove_file(&old) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    match std::fs::rename(path, old) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::parse_server_response;
    use crate::test_support::FixtureDirs;

    fn set_disabled(config: &ArcSwap<Config>, disabled: bool) {
        let response = parse_server_response(
            &format!("OK\ndisable_logging={disabled}\n"),
            "example.invalid",
        );
        Config::apply_server_response(config, &response);
    }

    #[test]
    fn remote_setting_controls_output_but_not_errors() {
        for initially_disabled in [false, true] {
            let dirs = FixtureDirs::new();
            let mut initial = dirs.config();
            initial.disable_logging = initially_disabled;
            let config = Arc::new(ArcSwap::from_pointee(initial));
            let subscriber = subscriber(
                &dirs.log_dir,
                config.clone(),
                std::io::sink,
                EnvFilter::new("info"),
            )
            .unwrap();
            tracing::subscriber::with_default(subscriber, || {
                if initially_disabled {
                    assert!(!dirs.log_dir.join("log_out").exists());
                }
                // Use the same callsites across updates to catch cached filter decisions.
                for disabled in [initially_disabled, !initially_disabled, initially_disabled] {
                    set_disabled(&config, disabled);
                    tracing::info!("output probe disabled={disabled}");
                    tracing::warn!("error probe disabled={disabled}");
                }
            });
            let out = std::fs::read_to_string(dirs.log_dir.join("log_out")).unwrap();
            let err = std::fs::read_to_string(dirs.log_dir.join("log_err")).unwrap();
            assert!(out.contains("output probe disabled=false"));
            assert!(!out.contains("disabled=true"));
            assert_eq!(err.matches("error probe").count(), 3);
            assert!(!err.contains("output probe"));
        }
    }

    #[test]
    fn rotates_at_java_line_limits_and_retains_only_one_backup() {
        for (name, limit) in [("log_out", 100_000), ("log_err", 10_000)] {
            let dirs = FixtureDirs::new();
            let config = Arc::new(ArcSwap::from_pointee(dirs.config()));
            let subscriber =
                subscriber(&dirs.log_dir, config, std::io::sink, EnvFilter::new("info")).unwrap();
            let path = dirs.log_dir.join(name);
            let old = path.with_extension("old");
            tracing::subscriber::with_default(subscriber, || {
                let emit = |message: &str| {
                    if name == "log_out" {
                        tracing::info!("{message}");
                    } else {
                        tracing::warn!("{message}");
                    }
                };
                for _ in 0..limit {
                    emit("first batch");
                }
                assert!(
                    !old.exists(),
                    "must rotate only after exceeding the Java limit"
                );
                // A multiline event must count physical lines, just like Java.
                emit("threshold line\nafter rotation");
                let backup = std::fs::read_to_string(&old).unwrap();
                assert_eq!(backup.matches("first batch").count(), limit);
                assert!(backup.contains("threshold line"));
                assert!(!backup.contains("after rotation"));
                assert!(
                    std::fs::read_to_string(&path)
                        .unwrap()
                        .contains("after rotation")
                );
                for _ in 0..limit {
                    emit("second batch");
                }
                let backup = std::fs::read_to_string(&old).unwrap();
                assert!(!backup.contains("first batch"));
                assert_eq!(backup.matches("second batch").count(), limit);
                assert!(!path.with_extension("old.old").exists());
            });
        }
    }

    #[test]
    fn startup_rotates_enabled_files_and_preserves_disabled_output() {
        for disabled in [false, true] {
            let dirs = FixtureDirs::new();
            let mut config = dirs.config();
            config.disable_logging = disabled;
            for name in ["log_out", "log_err"] {
                std::fs::write(dirs.log_dir.join(name), "previous run").unwrap();
                std::fs::write(dirs.log_dir.join(format!("{name}.old")), "older run").unwrap();
            }
            let subscriber = subscriber(
                &dirs.log_dir,
                Arc::new(ArcSwap::from_pointee(config)),
                std::io::sink,
                EnvFilter::new("info"),
            )
            .unwrap();
            drop(subscriber);
            for name in ["log_out", "log_err"] {
                let path = dirs.log_dir.join(name);
                let old = std::fs::read_to_string(path.with_extension("old")).unwrap();
                if name == "log_out" && disabled {
                    assert_eq!(std::fs::read_to_string(path).unwrap(), "previous run");
                    assert_eq!(old, "older run");
                } else {
                    assert_eq!(old, "previous run");
                    assert!(
                        !std::fs::read_to_string(path)
                            .unwrap()
                            .contains("previous run")
                    );
                }
            }
        }
    }

    #[test]
    fn concurrent_events_survive_rotation_without_interleaving() {
        let dirs = FixtureDirs::new();
        let mut config = dirs.config();
        config.disable_logging = true;
        let subscriber = subscriber(
            &dirs.log_dir,
            Arc::new(ArcSwap::from_pointee(config)),
            std::io::sink,
            EnvFilter::new("info"),
        )
        .unwrap();
        let dispatch = tracing::Dispatch::new(subscriber);
        std::thread::scope(|scope| {
            for thread in 0..4 {
                let dispatch = &dispatch;
                scope.spawn(move || {
                    tracing::dispatcher::with_default(dispatch, || {
                        for event in 0..2_501 {
                            tracing::warn!("concurrent-event={thread}:{event}");
                        }
                    })
                });
            }
        });
        let combined = ["log_err.old", "log_err"]
            .map(|name| std::fs::read_to_string(dirs.log_dir.join(name)).unwrap())
            .join("");
        let events: std::collections::HashSet<_> = combined
            .lines()
            .filter_map(|line| line.split_once("concurrent-event=").map(|(_, id)| id))
            .collect();
        assert_eq!(events.len(), 10_004);
        for thread in 0..4 {
            for event in 0..2_501 {
                assert!(events.contains(format!("{thread}:{event}").as_str()));
            }
        }
    }
}
