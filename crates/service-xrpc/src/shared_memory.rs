use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectRequest {
    pub client_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectResponse {
    pub(crate) client_id: usize,
    pub(crate) slot_name: String,
}

impl ConnectResponse {
    pub(crate) fn new(client_id: usize, slot_name: String) -> Self {
        Self {
            client_id,
            slot_name,
        }
    }

    pub(crate) fn into_parts(self) -> (usize, String) {
        (self.client_id, self.slot_name)
    }
}

/// Remove only xrpc SHM files owned by one exact service endpoint.
pub fn cleanup_owned_shared_memory(base_name: &str) -> std::io::Result<usize> {
    let directory = Path::new("/dev/shm");
    if !directory.exists() {
        return Ok(0);
    }
    let acceptor_prefix = format!("{base_name}-accept_");
    let client_prefix = format!("{base_name}-client-");
    let mut removed = 0;
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let owned_client =
            name.starts_with(&client_prefix) && (name.ends_with("_c2s") || name.ends_with("_s2c"));
        let owned_acceptor = name.starts_with(&acceptor_prefix)
            && (name.ends_with("_c2s") || name.ends_with("_s2c"));
        if owned_client || owned_acceptor {
            std::fs::remove_file(entry.path())?;
            removed += 1;
        }
    }
    Ok(removed)
}
