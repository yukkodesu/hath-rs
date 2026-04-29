use std::path::Path;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, Registry, fmt, prelude::*};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug = 1,
    Info = 2,
    Warning = 4,
    Error = 8,
}

pub fn init_logging(log_dir: &Path, output_enabled: bool) -> std::io::Result<()> {
    let out_log = log_dir.join("log_out");
    let err_log = log_dir.join("log_err");
    rotate_log(&out_log);
    rotate_log(&err_log);

    let file_appender = if output_enabled {
        Some(RollingFileAppender::new(
            Rotation::NEVER,
            log_dir,
            "log_out",
        ))
    } else {
        None
    };

    let err_appender = RollingFileAppender::new(Rotation::NEVER, log_dir, "log_err");

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let file_layer = file_appender.map(|a| {
        fmt::layer()
            .with_ansi(false)
            .with_target(false)
            .with_writer(a)
    });

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
    Ok(())
}

fn rotate_log(path: &Path) {
    let old = path.with_extension("old");
    let _ = std::fs::remove_file(&old);
    let _ = std::fs::rename(path, &old);
}
