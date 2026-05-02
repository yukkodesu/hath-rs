use std::path::Path;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, Registry, filter, fmt, prelude::*};
use crate::config::Config;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug = 1,
    Info = 2,
    Warning = 4,
    Error = 8,
}

#[derive(Clone, Debug)]
pub struct LoggingHandle {
    config: Config,
}

impl LoggingHandle {
    fn new(config: Config) -> Self {
        Self {
            config
        }
    }
}

pub fn init_logging(log_dir: &Path, config: Config) -> std::io::Result<LoggingHandle> {
    let handle = LoggingHandle::new(config);

    let out_log = log_dir.join("log_out");
    let err_log = log_dir.join("log_err");
    rotate_log(&out_log);
    rotate_log(&err_log);

    let file_appender = RollingFileAppender::new(Rotation::NEVER, log_dir, "log_out");
    let err_appender = RollingFileAppender::new(Rotation::NEVER, log_dir, "log_err");

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    
    let config = handle.config.clone();
    let file_filter = filter::filter_fn(move |_| config.disable_logging);
    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_target(false)
        .with_writer(file_appender)
        .with_filter(file_filter);

    let err_layer = fmt::layer()
        .with_ansi(false)
        .with_target(false)
        .with_writer(err_appender)
        .with_filter(tracing::level_filters::LevelFilter::WARN);

    let stdout_layer = fmt::layer().with_target(false);

    Registry::default()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer)
        .with(err_layer)
        .try_init()
        .ok(); // ignore double-init errors

    tracing::info!("Logging started");
    Ok(handle)
}

fn rotate_log(path: &Path) {
    let old = path.with_extension("old");
    let _ = std::fs::remove_file(&old);
    let _ = std::fs::rename(path, &old);
}
