use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::protocol::{LegacyCacheState, ProtocolError};
use crate::writer::{self, EncodedSnapshot};

const ARTIFACTS: [&str; 3] = ["pcache_info", "pcache_ages", "pcache_lru"];
static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Error)]
pub enum ImportError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("failed to serialize persistent cache data: {0}")]
    Serialize(#[from] Box<bincode::ErrorKind>),
    #[error("I/O error while migrating persistent cache: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "target already contains pcache_* files; pass --replace to back them up and replace them"
    )]
    ReplaceRequired,
    #[error("migration output failure: {0}")]
    Write(String),
}

#[derive(Debug)]
pub struct PublishOutcome {
    pub backup_dir: Option<PathBuf>,
}

pub fn publish(
    data_dir: &Path,
    state: &LegacyCacheState,
    replace: bool,
) -> Result<PublishOutcome, ImportError> {
    publish_with_writer(data_dir, state, replace, writer::encode)
}

fn publish_with_writer<F>(
    data_dir: &Path,
    state: &LegacyCacheState,
    replace: bool,
    encode: F,
) -> Result<PublishOutcome, ImportError>
where
    F: FnOnce(&LegacyCacheState) -> Result<EncodedSnapshot, ImportError>,
{
    if data_dir.exists() && !data_dir.is_dir() {
        return Err(ImportError::Write(format!(
            "data directory is not a directory: {}",
            data_dir.display()
        )));
    }
    fs::create_dir_all(data_dir)?;

    let existing: Vec<&str> = ARTIFACTS
        .into_iter()
        .filter(|name| data_dir.join(name).exists())
        .collect();
    if !existing.is_empty() && !replace {
        return Err(ImportError::ReplaceRequired);
    }

    let encoded = encode(state)?;
    if writer::sha1_hex(&encoded.ages) != encoded.ages_hash
        || writer::sha1_hex(&encoded.lru) != encoded.lru_hash
    {
        return Err(ImportError::Write(
            "staged cache hash did not match generated info".into(),
        ));
    }

    let backup_dir = if existing.is_empty() {
        None
    } else {
        let path = unique_backup_dir(data_dir);
        fs::create_dir(&path)?;
        for name in existing {
            fs::rename(data_dir.join(name), path.join(name))?;
        }
        Some(path)
    };

    let staged_ages = staged_path(data_dir, "pcache_ages");
    let staged_lru = staged_path(data_dir, "pcache_lru");
    let staged_info = staged_path(data_dir, "pcache_info");
    let stages = [&staged_ages, &staged_lru, &staged_info];

    let result = (|| -> Result<(), ImportError> {
        write_synced(&staged_ages, &encoded.ages)?;
        write_synced(&staged_lru, &encoded.lru)?;
        fs::rename(&staged_ages, data_dir.join("pcache_ages"))?;
        fs::rename(&staged_lru, data_dir.join("pcache_lru"))?;
        write_synced(&staged_info, &encoded.info)?;
        fs::rename(&staged_info, data_dir.join("pcache_info"))?;
        Ok(())
    })();

    if result.is_err() {
        for path in stages {
            let _ = fs::remove_file(path);
        }
    }
    result?;

    Ok(PublishOutcome { backup_dir })
}

fn unique_backup_dir(data_dir: &Path) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    data_dir.join(format!(
        "pcache-java-backup-{millis}-{}-{sequence}",
        std::process::id()
    ))
}

fn staged_path(data_dir: &Path, name: &str) -> PathBuf {
    let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    data_dir.join(format!(
        ".{name}.migrate-{}-{sequence}.tmp",
        std::process::id()
    ))
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), ImportError> {
    let mut file: File = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{ImportError, publish, publish_with_writer};
    use crate::protocol::{LRU_CACHE_SIZE, LegacyCacheState};

    fn full_state() -> LegacyCacheState {
        LegacyCacheState {
            cache_count: 7,
            cache_size: 99,
            lru_clear_pointer: 17,
            static_range_ages: HashMap::from([("a3f0".to_string(), 1_700_000_000_000)]),
            lru_cache_table: vec![0; LRU_CACHE_SIZE],
        }
    }

    #[test]
    fn existing_files_require_replace() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("pcache_info"), b"old").unwrap();

        let error = match publish(temp.path(), &full_state(), false) {
            Err(error) => error,
            Ok(_) => panic!("migration unexpectedly succeeded"),
        };

        assert!(error.to_string().contains("--replace"));
        assert_eq!(
            std::fs::read(temp.path().join("pcache_info")).unwrap(),
            b"old"
        );
    }

    #[test]
    fn staged_write_failure_does_not_publish_info() {
        let temp = tempfile::tempdir().unwrap();
        let error = publish_with_writer(temp.path(), &full_state(), false, |_| {
            Err(ImportError::Write("injected failure".into()))
        })
        .unwrap_err();

        assert!(error.to_string().contains("injected failure"));
        assert!(!temp.path().join("pcache_info").exists());
    }

    #[test]
    fn replace_backs_up_all_old_artifacts_before_publish() {
        let temp = tempfile::tempdir().unwrap();
        for name in ["pcache_info", "pcache_ages", "pcache_lru"] {
            std::fs::write(temp.path().join(name), name).unwrap();
        }

        let outcome = publish(temp.path(), &full_state(), true).unwrap();
        let backup_dir = outcome.backup_dir.unwrap();
        for name in ["pcache_info", "pcache_ages", "pcache_lru"] {
            assert_eq!(
                std::fs::read(backup_dir.join(name)).unwrap(),
                name.as_bytes()
            );
            assert!(temp.path().join(name).is_file());
        }
    }
}
