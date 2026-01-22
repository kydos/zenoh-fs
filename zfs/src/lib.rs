use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

pub const FS_EVT_DELAY: u64 = 1;
pub const SANITIZER_PERIOD: Duration = Duration::from_secs(3);
pub const DEFAULT_REPAIR_PACE: Duration = Duration::from_millis(0);
pub const GAP_DOWNLOAD_SCHEDULE: usize = 32;
pub const STUCK_CYCLES_RESET: usize = 3;
pub const MAX_ACCELERATION: usize = 33;

pub const ZFS_BASE_DIR: &str = "zfs";
pub const ZFS_DIGEST: &str = "zfs-digest";
pub const DOWNLOAD_SUBDIR: &str = "download";
pub const UPLOAD_SUBDIR: &str = "upload";
pub const FRAGS_SUBDIR: &str = "frags";
pub const DIGEST_SUBDIR: &str = "digest";
pub const FRAGMENT_SIZE: usize = 32 * 1024;

/// The ZFS structure is as follows:
///
/// ```text
/// .zfsd
///   +- digest
///   |    +- download
///   |    +- upload
///   |
///   +- frags
///        +- download
///        |    +- zfs
///        |       +- some
///        |            +- key
///        |                +- zfs-digest
///        |                +- 0
///        |                +- 1
///        |                +- ..
///        |                +- n
///        |
///        +- upload
/// ```
///
/// The structure used on the Zenoh filesystem storage is the following:
///
/// ```text
/// zfs
///  +- some
///       +- key
///            +- zfs-digest
///            +- 0
///            +- 1
///            +- ..
///            +- n
/// ```
///
/// Where zfs is just the top level directory under the Zenoh File System backend.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FragmentationDigest {
    pub name: String,
    pub size: u64,
    pub crc: u64,
    pub fragment_size: usize,
    pub fragments: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum DownloadStatus {
    Downloading,
    Completed,
    Reparing,
    Failed,
    Cleaning,
}

type DownloadRegistry = HashMap<String, DownloadStatus>;

pub struct ZFS {
    download_registry: DownloadRegistry,
    file_registry: Vec<(String, String)>,
}

impl ZFS {
    pub fn new() -> ZFS {
        ZFS {
            download_registry: HashMap::<String, DownloadStatus>::new(),
            file_registry: Vec::<(String, String)>::new(),
        }
    }

    pub fn add_download(&mut self, id: &str) {
        self.download_registry
            .insert(id.to_string(), DownloadStatus::Downloading);
    }

    pub fn remove_download(&mut self, id: &str) {
        self.download_registry.remove(id);
    }

    pub fn get_download_status(&self, id: &str) -> Option<&DownloadStatus> {
        self.download_registry.get(id)
    }

    pub fn set_download_status(&mut self, id: &str, status: DownloadStatus) {
        self.download_registry.insert(id.to_string(), status);
    }

    pub fn add_file(&mut self, name: &str, key: &str) {
        self.file_registry.push((name.to_string(), key.to_string()))
    }

    pub fn file_list(&self) -> &Vec<(String, String)> {
        &self.file_registry
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadDigest {
    pub path: String,
    pub key: String,
    pub fragment_size: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DownloadDigest {
    pub key: String,
    pub path: String,
    pub pace: usize,
}

// #[derive(Debug)]
// struct SanitizerRegistryEntry {
//     digest: std::sync::Arc<DownloadDigest>,
//     tide_level: usize,
//     gap_num: usize,
//     stuck_cycles: usize,
// }

mod frag;
mod sanitizer;
mod transfer;

pub use frag::*;
pub use sanitizer::{download_sanitizer, upload_sanitizer};
pub use transfer::*;
#[derive(Debug, Clone)]
pub struct ZFSKey(String);

impl From<&str> for ZFSKey {
    fn from(key: &str) -> Self {
        ZFSKey(format!("{}/{}", ZFS_BASE_DIR, key))
    }
}
impl From<String> for ZFSKey {
    fn from(key: String) -> Self {
        ZFSKey(format!("{}/{}", ZFS_BASE_DIR, key))
    }
}
impl From<&String> for ZFSKey {
    fn from(key: &String) -> Self {
        ZFSKey(format!("{}/{}", ZFS_BASE_DIR, key.clone()))
    }
}

pub fn zfs_err2str<E: Debug>(e: E) -> String {
    format!("{:?}", e)
}

pub fn zfsd_home() -> String {
    if let Ok(path) = std::env::var("ZFSD_HOME") {
        path
    } else {
        format!("{}/{}", std::env::var("HOME").unwrap(), ".zfsd")
    }
}

// ZFS key-related functions
// pub fn zfs_key(key: &str) -> String {
//     format!("{}/{}", ZFS_BASE_DIR, key)
// }

pub fn zfs_frags_digest_for_key(key: &ZFSKey) -> String {
    format!("{}/{}", &key.0, ZFS_DIGEST)
}
// pub fn zfs_download_frags_digest_for_key(key: &str) -> String {
//     format!("{}/{}", key, ZFS_DIGEST)
// }

pub fn zfs_nth_frag_key(key: &ZFSKey, n: u32) -> String {
    format!("{}/{}", &key.0, n)
}

// ZFSD path-related functions
pub fn zfsd_upload_digest_dir() -> String {
    format!("{}/{}/{}", zfsd_home(), DIGEST_SUBDIR, UPLOAD_SUBDIR)
}
pub fn zfsd_download_digest_dir() -> String {
    format!("{}/{}/{}", zfsd_home(), DIGEST_SUBDIR, DOWNLOAD_SUBDIR)
}

pub fn zfsd_upload_frags_dir() -> String {
    format!("{}/{}/{}", zfsd_home(), FRAGS_SUBDIR, UPLOAD_SUBDIR)
}

pub fn zfsd_download_frags_dir() -> String {
    format!("{}/{}/{}", zfsd_home(), FRAGS_SUBDIR, DOWNLOAD_SUBDIR)
}

pub fn zfsd_download_frags_dir_for_key(k: &ZFSKey) -> String {
    format!("{}/{}", zfsd_download_frags_dir(), &k.0)
}

pub fn zfsd_upload_frags_dir_for_key(k: &ZFSKey) -> String {
    format!("{}/{}", zfsd_upload_frags_dir(), &k.0)
}

pub fn zfsd_upload_frag_dir_to_key(path: &str) -> Option<ZFSKey> {
    path.strip_prefix(&zfsd_upload_frags_dir())
        .map(|s| ZFSKey(s[1..].to_string())) // skip the initial "/"
}

pub async fn zfs_read_download_digest_from(
    path: &std::path::Path,
) -> Result<DownloadDigest, String> {
    tokio::fs::read(path)
        .await
        .map_err(zfs_err2str)
        .and_then(|bs| serde_json::from_slice::<crate::DownloadDigest>(&bs).map_err(zfs_err2str))
}
