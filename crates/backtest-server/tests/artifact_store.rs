use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use backtest_server::artifact_store::{ArtifactStore, ArtifactStoreError, sha256_hex};
use backtest_server::rpc_types::RESULT_FORMAT_VERSION;

struct TempDirectory {
    path: PathBuf,
}

impl TempDirectory {
    fn new(label: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "qs_backtest_artifact_{label}_{}_{}",
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn store(
    directory: &Path,
    chunk_size: usize,
    retention: Duration,
    max_total_bytes: u64,
) -> ArtifactStore {
    ArtifactStore::new(
        directory,
        12 * 1024 * 1024,
        chunk_size,
        retention,
        max_total_bytes,
    )
    .unwrap()
}

#[test]
fn artifact_store_roundtrips_chunks_and_checksum() {
    let directory = TempDirectory::new("roundtrip");
    let store = store(directory.path(), 7, Duration::from_secs(60), 1024);
    let payload = br#"{"schema":"two","values":[1,2,3,4,5]}"#;

    let reference = store.persist_json(payload).unwrap();
    assert_eq!(reference.format_version, RESULT_FORMAT_VERSION);
    assert_eq!(reference.byte_len, payload.len() as u64);
    assert_eq!(reference.sha256, sha256_hex(payload));
    assert_eq!(reference.chunk_size, 7);
    assert_eq!(
        reference.sha256,
        "47b92dadb20e0d148e0c1c98d80ef6c2cefd1714877539601a411090396695fa"
    );
    assert!(!reference.artifact_id.contains('/'));
    assert!(!reference.artifact_id.contains('\\'));

    let mut reconstructed = Vec::new();
    let mut offset = 0;
    loop {
        let chunk = store.read_chunk(&reference.artifact_id, offset).unwrap();
        assert_eq!(chunk.offset, offset);
        reconstructed.extend_from_slice(&chunk.bytes);
        offset += chunk.bytes.len() as u64;
        if chunk.eof {
            break;
        }
    }
    assert_eq!(reconstructed, payload);

    assert!(store.delete(&reference.artifact_id).unwrap());
    assert!(!store.delete(&reference.artifact_id).unwrap());
}

#[test]
fn artifact_store_cleanup_removes_expired_files() {
    let directory = TempDirectory::new("retention");
    let store = store(directory.path(), 8, Duration::from_millis(5), 1024);
    let reference = store.persist_json(b"expired").unwrap();

    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(store.cleanup_expired().unwrap(), 1);
    assert!(matches!(
        store.read_chunk(&reference.artifact_id, 0),
        Err(ArtifactStoreError::NotFound(_))
    ));
}

#[test]
fn artifact_store_rejects_capacity_pressure_without_evicting_live_files() {
    let directory = TempDirectory::new("capacity");
    let store = store(directory.path(), 8, Duration::from_secs(60), 12);
    let first = store.persist_json(b"12345678").unwrap();
    let error = store.persist_json(b"abcdefgh").unwrap_err();

    assert!(matches!(error, ArtifactStoreError::CapacityExceeded { .. }));
    assert_eq!(
        store.read_chunk(&first.artifact_id, 0).unwrap().bytes,
        b"12345678"
    );
}

#[test]
fn artifact_store_reclaims_expired_files_before_capacity_rejection() {
    let directory = TempDirectory::new("expired_capacity");
    let store = store(directory.path(), 8, Duration::from_millis(20), 12);
    let expired = store.persist_json(b"12345678").unwrap();

    std::thread::sleep(Duration::from_millis(50));
    let current = store.persist_json(b"abcdefgh").unwrap();
    assert!(matches!(
        store.read_chunk(&expired.artifact_id, 0),
        Err(ArtifactStoreError::NotFound(_))
    ));
    assert_eq!(
        store.read_chunk(&current.artifact_id, 0).unwrap().bytes,
        b"abcdefgh"
    );
}

#[test]
fn artifact_store_chunk_reads_refresh_retention() {
    let directory = TempDirectory::new("touch");
    let store = store(directory.path(), 8, Duration::from_millis(80), 1024);
    let reference = store.persist_json(b"kept-alive").unwrap();

    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        store.read_chunk(&reference.artifact_id, 0).unwrap().bytes,
        b"kept-ali"
    );
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(store.cleanup_expired().unwrap(), 0);
    assert!(store.read_chunk(&reference.artifact_id, 0).is_ok());

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(store.cleanup_expired().unwrap(), 1);
}

#[test]
fn artifact_store_rejects_traversal_ids_and_invalid_offsets() {
    let directory = TempDirectory::new("safety");
    let store = store(directory.path(), 8, Duration::from_secs(60), 1024);

    for artifact_id in ["../secret", "..\\secret", "a/b", "", "."] {
        assert!(matches!(
            store.read_chunk(artifact_id, 0),
            Err(ArtifactStoreError::InvalidId)
        ));
        assert!(matches!(
            store.delete(artifact_id),
            Err(ArtifactStoreError::InvalidId)
        ));
    }

    let reference = store.persist_json(b"small").unwrap();
    assert!(matches!(
        store.read_chunk(&reference.artifact_id, reference.byte_len + 1),
        Err(ArtifactStoreError::InvalidOffset { .. })
    ));
}

#[cfg(unix)]
#[test]
fn artifact_store_rejects_symlinks_outside_the_root() {
    use std::os::unix::fs::symlink;

    let directory = TempDirectory::new("symlink_root");
    let outside = TempDirectory::new("symlink_outside");
    let outside_file = outside.path().join("outside.json");
    std::fs::write(&outside_file, b"outside").unwrap();
    symlink(&outside_file, directory.path().join("result_escape.json")).unwrap();
    let store = store(directory.path(), 8, Duration::from_secs(60), 1024);

    assert!(matches!(
        store.read_chunk("result_escape", 0),
        Err(ArtifactStoreError::PathEscape)
    ));
}
