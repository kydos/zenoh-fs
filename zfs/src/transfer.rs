use crate::*;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use zenoh::qos::CongestionControl;
use zenoh::query::*;
use zenoh::Session;

pub async fn upload_fragment(z: &Session, path: &str, zfs_key: &ZFSKey) {
    log::debug!(target: "transfer", "Uploading fragment {} for key {:?}", path, zfs_key);
    let path = PathBuf::from(path);
    let bs = std::fs::read(path.as_path())
        .unwrap_or_else(|_| panic!("path: {} should be valid", &path.to_string_lossy()));
    z.put(&zfs_key.0, bs)
        .congestion_control(CongestionControl::Block)
        .await
        .unwrap();
}

pub async fn download_fragment(z: Arc<Session>, zfs_key: ZFSKey, n: u32) -> Result<(), String> {
    log::debug!(target: "transfer", "Downloading fragment # {} for key {:?}", n, &zfs_key);

    let path = zfsd_download_frags_dir_for_key(&zfs_key);
    // let frag_key = format!("{}/{}/{}", zfs_upload_frags_key_prefix(), key, n);
    let frag_key = zfs_nth_frag_key(&zfs_key, n);
    let frag = format!("{}/{}", &path, n);
    if Path::new(&frag).exists() {
        log::debug!(
            "The fragment {} already has already been downloaded, skipping.",
            &frag
        );
        return Ok(());
    }

    // First check if the fragment is already there -- there is potential concurrency between
    // the sanitizer and the regular download process.
    log::debug!(target: "zfsd", "Retrieving fragment: {:?}/{}", &zfs_key, n);
    let replies = z
        .get(&frag_key.clone())
        .target(QueryTarget::DEFAULT)
        .await
        .unwrap();

    if let Ok(reply) = replies.recv_async().await {
        if let Ok(r) = reply.result() {
            let bs = r.payload().to_bytes();

            tokio::fs::write(std::path::Path::new(&frag), bs)
                .await
                .map_err(|e| format!("{:?}", e))
        } else {
            Err(format!("Unable to retrieve fragment: {}", &frag_key))
        }
    } else {
        Err(format!("Unable to retrieve fragment: {}", &frag_key))
    }
}
pub async fn download_fragmentation_digest(
    z: std::sync::Arc<Session>,
    digest_key: &str,
) -> Result<FragmentationDigest, String> {
    log::debug!(target: "zfsd", "Retrieving fragmentation digest: {}", &digest_key);
    let replies = z
        .get(digest_key)
        .target(QueryTarget::DEFAULT)
        .await
        .unwrap();

    if let Ok(reply) = replies.recv_async().await {
        if let Ok(r) = reply.result() {
            let bs = r.payload().to_bytes();
            let digest = serde_json::from_slice::<FragmentationDigest>(&bs).unwrap();
            Ok(digest)
        } else {
            Err("Unable to retriefe manifest".to_string())
        }
    } else {
        Err("Unable to retriefe manifest".to_string())
    }
}

pub async fn download(
    z: std::sync::Arc<Session>,
    zfs: Arc<Mutex<ZFS>>,
    download_spec_path: PathBuf,
) -> Result<(), String> {
    log::info!("Starting download for {:?}.", &download_spec_path);

    let bs = match tokio::fs::read(download_spec_path.as_path()).await {
        Ok(bs) => bs,
        Err(e) => {
            log::info!("Unable to read file at {:?}", download_spec_path.as_path());
            return Err(format!(
                "Unable to read file at {:?}\n",
                download_spec_path.as_path()
            ));
        }
    };

    let download_spec = match serde_json::from_slice::<DownloadDigest>(&bs) {
        Ok(ds) => ds,
        Err(e) => {
            log::info!("Failed to deserialize {:?} -- {e}.", &download_spec_path);
            return Err(format!("{:?}", e));
        }
    };
    let zfs_key = ZFSKey::from(&download_spec.key);
    if std::path::Path::new(&download_spec.path).exists() {
        log::info!(
            "The file {} has already been downloaded.",
            &download_spec.path
        );
        return Ok(());
    }

    let id = download_spec_path.file_name().unwrap().to_string_lossy();
    (*zfs.lock().await).add_download(&id);
    log::info!(target: "tranfer", "Set {} status to Downloading", &id);

    // let frag_digest = format!("{}/{}/{}", zfs_upload_frags_key_prefix(), download_spec.key, ZFS_DIGEST);
    let frag_digest = zfs_frags_digest_for_key(&zfs_key);
    log::debug!(target: "tranfer", "Get Frag Digest: {}", &frag_digest);
    let digest = download_fragmentation_digest(z.clone(), &frag_digest).await?;

    let frags_dir = zfsd_download_frags_dir_for_key(&zfs_key);
    tokio::fs::create_dir_all(std::path::Path::new(&frags_dir))
        .await
        .unwrap();

    let bar = ProgressBar::new(digest.fragments.into());
    bar.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {frag}/{total_frags} ({eta})")
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-"));

    write_defrag_digest(&digest, &frags_dir).await?;
    for i in 0..digest.fragments {
        download_fragment(z.clone(), zfs_key.clone(), i).await?;
        bar.inc(1);
    }

    log::debug!(target: "zfsd", "Degragmenting into {}", &download_spec.path);
    let p = std::path::Path::new(&download_spec.path);
    match p.parent() {
        Some(parent) => {
            std::fs::create_dir_all(parent).unwrap();
            let _ = defragment(&download_spec.key, &download_spec.path)
                .await
                .map(|r| {
                    if r {
                        bar.finish();
                    } else {
                        log::warn!(
                            "The file received for {} was currupted.",
                            &download_spec.key
                        )
                    }
                });

            sanitizer::cleanup_download(&download_spec, &download_spec_path.to_string_lossy()).await
        }
        None => {
            log::warn!(target: "zfsd", "Invalid target path: {:?}\n Unable to defragment", p);
            bar.finish_with_message("failed to defragment (see log)");
            Ok(())
        }
    }
}
