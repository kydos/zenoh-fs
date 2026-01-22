use crate::*;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::Mutex;
use zenoh::Session;

pub async fn cleanup_download(
    digest: &DownloadDigest,
    download_manifest: &str,
) -> Result<(), String> {
    // Check first if the file has been really created
    let target = std::path::Path::new(&digest.path);
    let zfs_key = ZFSKey::from(&digest.key);
    let frags_path = zfsd_download_frags_dir_for_key(&zfs_key);
    let fmanif_exists = std::path::Path::new(&format!("{}/{}", &frags_path, ZFS_DIGEST)).exists();
    if target.exists() && fmanif_exists {
        let defrag_digest = read_defrag_digest(&frags_path).await.unwrap();
        let size = target.metadata().unwrap().len();

        tokio::time::sleep(Duration::from_secs(2 * FS_EVT_DELAY)).await;
        if size == defrag_digest.size {
            let frags_path = zfsd_download_frags_dir_for_key(&zfs_key);
            let _ignore = std::fs::remove_dir_all(&frags_path);
            log::info!("Removing file: {download_manifest}");
            let _ignore = std::fs::remove_file(std::path::Path::new(download_manifest));
        } else {
            log::debug!(
                "The target {} is still being reassembled, clean up will be scheduled later {} != {}",&digest.path, size, defrag_digest.size,

            );
        }
    } else if !target.exists() && fmanif_exists {
        // We try to defragment...
        let _ignore = defragment(&digest.key, &digest.path).await;
    }
    Ok(())
}

async fn compute_download_gaps(
    z: std::sync::Arc<Session>,
    digest: &DownloadDigest,
) -> Result<BTreeSet<usize>, String> {
    let zfs_key = ZFSKey::from(&digest.key);
    let frags_path = zfsd_download_frags_dir_for_key(&zfs_key);
    let frag_digest_key = zfs_frags_digest_for_key(&zfs_key);
    if let Ok(defrag_digest) = download_fragmentation_digest(z, &frag_digest_key).await {
        let mut frag_set = BTreeSet::new();
        for i in 0..defrag_digest.fragments {
            frag_set.insert(i as usize);
        }
        let path = std::path::Path::new(&frags_path);
        if let Ok(entries) = path.read_dir() {
            for entry in entries.flatten() {
                let name = entry
                    .path()
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap()
                    .to_string();
                if let Ok(n) = name.parse() {
                    frag_set.remove(&n);
                }
            }
        }
        Ok(frag_set)
    } else {
        Err(format!("Unable to read defrag digest for {:?}", &digest))
    }
}

// fn compute_acceleration_factor(stuck_cycles: usize) -> usize {
//     let r = (stuck_cycles / STUCK_CYCLES_RESET) + 1;
//     let a = std::cmp::max(1, r / 2);
//     let f = std::cmp::min(a * r, MAX_ACCELERATION);
//     log::debug!("Acceleration factor for {} is {}", stuck_cycles, f);
//     f
// }

async fn repair_gaps(
    z: Arc<zenoh::Session>,
    zfs: Arc<Mutex<ZFS>>,
    digest: DownloadDigest,
    entry_path: PathBuf,
    id: String,
    pace: Duration,
) {
    let zfs_key = ZFSKey::from(&digest.key);
    let mut gaps: Vec<usize> = compute_download_gaps(z.clone(), &digest)
        .await
        .unwrap()
        .into_iter()
        .collect();
    for i in 0..3 {
        log::info!(target: "Sanitizer", "Gaps repair iteration: {i}");
        for g in gaps.iter() {
            let _ = download_fragment(z.clone(), zfs_key.clone(), *g as u32).await;
            tokio::time::sleep(pace).await;
        }
        gaps.clear();
        gaps = compute_download_gaps(z.clone(), &digest)
            .await
            .unwrap()
            .into_iter()
            .collect();
        if gaps.is_empty() {
            log::info!(target: "Sanitizer", "Reparied the download, file is ready at {}", digest.path);
            log::info!(target: "Sanitizer", "Cleaning up {:?}", entry_path);
            let _ = cleanup_download(&digest, &entry_path.to_string_lossy()).await;
            (*zfs.lock().await).set_download_status(&id, DownloadStatus::Completed);
            return;
        } else {
            gaps.sort_unstable();
        }
    }
    (*zfs.lock().await).set_download_status(&id, DownloadStatus::Failed);
    log::info!(target: "Sanitizer", "Failed to repar the download, in spite of trying very hard!");
}

pub async fn download_sanitizer(z: Arc<zenoh::Session>, zfs: Arc<Mutex<ZFS>>) {
    let d3 = zfsd_download_digest_dir();
    let dpath = std::path::Path::new(&d3);
    loop {
        tokio::time::sleep(SANITIZER_PERIOD).await;
        log::debug!("Running Sanitizer...");
        if let Ok(entries) = dpath.read_dir() {
            for entry in entries.flatten() {
                log::debug!("Sanitizer looking into <{:?}>", &entry);
                let id = entry.file_name().to_string_lossy().to_string();
                let zfs_clone = zfs.clone();
                let mut zfs = zfs.lock().await;
                match zfs.get_download_status(&id) {
                    None | Some(DownloadStatus::Failed) => {
                        log::info!("Sanitizer reparing <{:?}>", &entry);
                        (*zfs).set_download_status(&id, DownloadStatus::Reparing);

                        let digest = zfs_read_download_digest_from(entry.path().as_path())
                            .await
                            .unwrap();
                        log::info!(target: "sanitizer", "Download Digest: {:?}", &digest);

                        tokio::task::spawn({
                            repair_gaps(
                                z.clone(),
                                zfs_clone,
                                digest,
                                entry.path(),
                                id.to_string(),
                                DEFAULT_REPAIR_PACE.clone(),
                            )
                        });
                    }
                    Some(DownloadStatus::Downloading) => {
                        log::info!("Sanitizer ignoring <{:?}> as it is downloading", &entry);
                        continue;
                    }
                    Some(DownloadStatus::Completed) => {
                        (*zfs).set_download_status(&id, DownloadStatus::Cleaning);
                        continue;
                    }
                    Some(DownloadStatus::Cleaning) => {
                        (*zfs).remove_download(&id);
                        let _ = fs::remove_file(&entry.path()).await;
                        continue;
                    }
                    _ => continue,
                }
            }
        } else {
            log::warn!(target: "zfsd", "Sanitizer unable to list the directory {:?}", dpath);
        }
    }
}
pub async fn upload_sanitizer() {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        // TODO: Implement Upload Sanitizer
    }
}
