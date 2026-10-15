//! Exclusive use of the SQLite store. The service holds the lock while it
//! runs, and the `issuer` verbs that move or confirm receipt custody hold it
//! for the whole command, so the two never use the store at the same time:
//! the receipt engine's projection rebuilds and quiescence reads must not race
//! a running service, and a service must not start in the middle of a move.
//!
//! The lock is an exclusive OS file lock on `<database file>.lock`, next to
//! the store. Every process that opens the store on the same host sees the
//! same file (on GCP, the bot container and the one-off `issuer` container both
//! mount the VM's data disk), and the kernel releases the lock when its holder
//! exits, so no stale lock can stay behind.

use sqlx::sqlite::SqliteConnectOptions;
use std::ffi::OsString;
use std::fs::{File, Permissions, TryLockError};
use std::io::{self, ErrorKind};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::{debug, warn};

/// Holds the store lock until it is dropped.
#[derive(Debug)]
pub(crate) struct StoreLock {
    pub(crate) path: PathBuf,
    _file: File,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreLockError {
    #[error(
        "another process holds the store lock {}: the issuance service or \
         another issuer command is using the store; stop it first",
        path.display()
    )]
    Held { path: PathBuf },
    #[error("cannot open or lock the store lock file {}: {source}", path.display())]
    LockFile { path: PathBuf, source: io::Error },
    #[error(
        "the store lock needs the database as a file path in the URL (for \
         example sqlite:///mnt/data/issuance.db), not as a SQLite URI: {}",
        filename.display()
    )]
    UriFilename { filename: PathBuf },
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

/// Takes the exclusive lock for the store at `database_url`, without waiting.
///
/// An in-memory store returns `None`: no other process can share it, so there
/// is nothing to exclude.
///
/// # Errors
///
/// [`StoreLockError::Held`] when another process holds the lock,
/// [`StoreLockError::LockFile`] when the lock file cannot be opened or
/// locked, [`StoreLockError::UriFilename`] when the URL names the database as
/// a SQLite `file:` URI, and a URL error when the URL cannot be parsed.
pub(crate) fn acquire(
    database_url: &str,
) -> Result<Option<StoreLock>, StoreLockError> {
    let options = SqliteConnectOptions::from_str(database_url)?;
    if is_in_memory(database_url) {
        debug!(target: "store", "In-memory store: no store lock to take");
        return Ok(None);
    }

    let filename = options.get_filename();
    if filename.as_os_str().as_encoded_bytes().starts_with(b"file:") {
        return Err(StoreLockError::UriFilename {
            filename: filename.to_path_buf(),
        });
    }

    let mut lock_path = OsString::from(filename);
    lock_path.push(".lock");
    let path = PathBuf::from(lock_path);

    let file = match open_lock_file(&path) {
        Ok(file) => file,
        Err(source) => return Err(StoreLockError::LockFile { path, source }),
    };

    match file.try_lock() {
        Ok(()) => {
            debug!(target: "store", path = %path.display(),
                "Acquired the store lock"
            );
            Ok(Some(StoreLock { path, _file: file }))
        }
        Err(TryLockError::WouldBlock) => {
            warn!(target: "store", path = %path.display(),
                "Another process holds the store lock"
            );
            Err(StoreLockError::Held { path })
        }
        Err(TryLockError::Error(source)) => {
            Err(StoreLockError::LockFile { path, source })
        }
    }
}

/// Opens the lock file read-only, and creates it when it is missing. flock(2)
/// locks a file whatever its open mode, so the lock needs no write access, and
/// a lock file that another user created (on the droplet, root through the
/// `issuer` wrapper) still locks for the service.
fn open_lock_file(path: &Path) -> io::Result<File> {
    match File::open(path) {
        Err(error) if error.kind() == ErrorKind::NotFound => {
            create_lock_file(path)
        }
        opened => opened,
    }
}

/// Sets mode 0644 whatever the umask, so every user can open the new file
/// read-only. When another process creates the file first, it is opened like
/// an existing one.
fn create_lock_file(path: &Path) -> io::Result<File> {
    match File::options().write(true).create_new(true).open(path) {
        Ok(file) => {
            file.set_permissions(Permissions::from_mode(0o644))?;
            Ok(file)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            File::open(path)
        }
        Err(error) => Err(error),
    }
}

/// sqlx 0.9 parses `sqlite::memory:` and `mode=memory` into an in-memory
/// store but exposes no getter for it, so the URL is read directly, with the
/// same rule: the name after the `sqlite://` or `sqlite:` scheme is exactly
/// `:memory:` (`sqlx-sqlite` `SqliteConnectOptions::from_str`).
fn is_in_memory(database_url: &str) -> bool {
    let (location, query) =
        database_url.split_once('?').unwrap_or((database_url, ""));
    let database =
        location.trim_start_matches("sqlite://").trim_start_matches("sqlite:");

    database == ":memory:" || query.split('&').any(|pair| pair == "mode=memory")
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::{StoreLockError, acquire};
    use crate::test_utils::logs_contain_at;

    fn file_store_url(directory: &tempfile::TempDir) -> (String, String) {
        let database = directory.path().join("issuance.db");
        let url = format!("sqlite:{}?mode=rwc", database.display());
        let lock = format!("{}.lock", database.display());
        (url, lock)
    }

    #[traced_test]
    #[test]
    fn a_held_lock_refuses_a_second_holder_until_it_drops() {
        let directory = tempfile::tempdir().unwrap();
        let (url, lock_path) = file_store_url(&directory);

        let first = acquire(&url).unwrap();
        assert!(first.is_some(), "a file store must take the lock");
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Acquired the store lock", &lock_path]
        ));

        let error = acquire(&url).unwrap_err();
        let StoreLockError::Held { path } = &error else {
            panic!("expected Held, got {error:?}");
        };
        assert_eq!(path.display().to_string(), lock_path);
        assert!(
            error.to_string().contains("stop it first"),
            "the refusal must say what to do, got {error}"
        );
        assert!(logs_contain_at!(
            Level::WARN,
            &["Another process holds the store lock", &lock_path]
        ));

        drop(first);
        assert!(
            acquire(&url).unwrap().is_some(),
            "a dropped lock must be free again"
        );
    }

    #[traced_test]
    #[test]
    fn an_in_memory_store_takes_no_lock() {
        for url in
            ["sqlite::memory:", "sqlite://:memory:", "sqlite://?mode=memory"]
        {
            assert!(acquire(url).unwrap().is_none(), "{url}");
        }
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["In-memory store: no store lock to take"]
        ));
    }

    /// sqlx opens a name that only ends in `:memory:` as a file, so that store
    /// takes the lock like any other file.
    #[traced_test]
    #[test]
    fn a_file_named_like_memory_takes_the_lock() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("backup:memory:");
        let url = format!("sqlite:{}?mode=rwc", database.display());

        let lock = acquire(&url).unwrap();

        assert!(lock.is_some(), "a file store must take the lock");
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Acquired the store lock", "backup:memory:.lock"]
        ));
    }

    /// sqlx opens a `file:` name as a SQLite URI, which is not a path the lock
    /// file can sit next to, so the URL is refused with a clear error.
    #[test]
    fn a_sqlite_uri_filename_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("issuance.db");
        let url = format!("sqlite:file:{}", database.display());

        let error = acquire(&url).unwrap_err();

        assert!(
            matches!(error, StoreLockError::UriFilename { .. }),
            "expected UriFilename, got {error:?}"
        );
        assert!(
            error.to_string().contains("sqlite:///"),
            "the refusal must show the path form to use, got {error}"
        );
    }

    /// The lock file can belong to another user (on the droplet, root runs
    /// `issuer` and the service runs as `st0x`), so an existing lock file must
    /// lock without write access to it.
    #[traced_test]
    #[test]
    fn an_existing_lock_file_locks_without_write_access() {
        let directory = tempfile::tempdir().unwrap();
        let (url, lock_path) = file_store_url(&directory);
        std::fs::write(&lock_path, "").unwrap();
        let mut permissions =
            std::fs::metadata(&lock_path).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&lock_path, permissions).unwrap();

        let _first = acquire(&url).unwrap();

        let error = acquire(&url).unwrap_err();
        assert!(
            matches!(error, StoreLockError::Held { .. }),
            "a read-only lock file must still lock, got {error:?}"
        );
        assert!(logs_contain_at!(
            Level::WARN,
            &["Another process holds the store lock", &lock_path]
        ));
    }

    #[test]
    fn a_lock_file_that_cannot_open_names_its_path() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("missing").join("issuance.db");
        let url = format!("sqlite:{}?mode=rwc", database.display());

        let error = acquire(&url).unwrap_err();

        let StoreLockError::LockFile { path, .. } = &error else {
            panic!("expected LockFile, got {error:?}");
        };
        let lock_path = format!("{}.lock", database.display());
        assert_eq!(path.display().to_string(), lock_path);
        assert!(
            error.to_string().contains(&lock_path),
            "the error must name the lock file, got {error}"
        );
    }

    /// A new lock file gets mode 0644, so a process of another user can open
    /// it read-only and lock it.
    #[traced_test]
    #[test]
    fn the_lock_file_sits_next_to_the_store() {
        let directory = tempfile::tempdir().unwrap();
        let (url, lock_path) = file_store_url(&directory);

        let lock = acquire(&url).unwrap().unwrap();

        assert_eq!(lock.path.display().to_string(), lock_path);
        let mode = std::fs::metadata(&lock_path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o644, "got mode {mode:o}");
        assert!(
            !directory.path().join("issuance.db").exists(),
            "taking the lock must not create the store itself"
        );
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Acquired the store lock", &lock_path]
        ));
    }
}
