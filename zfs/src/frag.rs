use crate::*;
use checksum::crc::Crc;
use std::path::Path;
use std::path::PathBuf;
use tokio::fs::{create_dir_all, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn fragment(
    zfs: Arc<Mutex<ZFS>>,
    file_path: &str,
    zkey: &str,
    fragment_size: usize,
) -> Result<crate::FragmentationDigest, String> {
    let home = zfs.lock().await.home.clone();
    let zfs_key = ZFSKey::from(zkey);
    match Crc::new(file_path).checksum() {
        Ok(checksum) => {
            let mut file = match File::open(file_path).await {
                Ok(f) => f,
                Err(_) => return Err(format!("Unable to open the file {}", file_path)),
            };
            let mut bs = vec![0_u8; fragment_size];
            log::debug!("bs.len() = {}", bs.len());
            let mut fid = 0;
            let frag_path = zfsd_upload_frags_dir_for_key(home, &zfs_key);
            log::debug!("Target dir: {:?}", frag_path);
            create_dir_all(Path::new(&frag_path)).await.unwrap();
            loop {
                match file.read(&mut bs).await {
                    Ok(n) if n > 0 => {
                        let fname = format!("{}/{}", &frag_path, fid);
                        let mut f = match File::create(&fname).await {
                            Ok(f) => f,
                            Err(e) => {
                                log::debug!(
                                    "Error {:?} while creating the fragment: {}",
                                    e,
                                    &fname
                                );
                                panic!("IO Error")
                            }
                        };
                        let _ignore = f.write(&bs[0..n]).await;
                        fid += 1;
                    }
                    _ => break,
                }
            }

            let digest = crate::FragmentationDigest {
                name: zkey.into(),
                size: file.metadata().await.unwrap().len(),
                crc: checksum.crc64,
                fragment_size,
                fragments: fid,
            };
            log::debug!("{:?}", digest);
            write_defrag_digest(&digest, &frag_path)
                .await
                .map(|_| digest)
        }
        Err(e) => Err(e.into()),
    }
}

pub async fn fragment_from_digest(zfs: Arc<Mutex<ZFS>>, path: String) -> Result<(), String> {
    let path = PathBuf::from(path);
    // let mut target = PathBuf::from(path.parent().unwrap());
    // target.push(frag_size);

    let bs = std::fs::read(path.as_path()).unwrap();
    let upload_spec = match serde_json::from_slice::<crate::UploadDigest>(&bs) {
        Ok(us) => us,
        Err(e) => return Err(format!("{:?}", e)),
    };
    // log::debug!(target: "zfsd", "Uploading: {} as {}", &upload_spec.path, &upload_spec.key);
    if !std::path::Path::new(&upload_spec.path).exists() {
        // log::warn!(target: "zfsd", "The file {} does not exit", &upload_spec.path);
        return Ok(());
    }
    crate::frag::fragment(
        zfs,
        &upload_spec.path,
        &upload_spec.key,
        upload_spec.fragment_size,
    )
    .await
    .unwrap();
    Ok(())
}

pub async fn read_defrag_digest(base_path: &str) -> Result<FragmentationDigest, String> {
    let path: PathBuf = [base_path, crate::ZFS_DIGEST].iter().collect();
    log::debug!("read_defrag_digest: Trying to read: {:?}", &path.as_path());
    let rbs = tokio::fs::read(path.as_path()).await;
    log::debug!(
        "read_defrag_digest: Trying to deserialize: {:?}",
        &path.as_path()
    );
    rbs.map_err(|e| format!("{:?}", e)).and_then(|bs| {
        match serde_json::from_slice::<crate::FragmentationDigest>(&bs) {
            Ok(digest) => Ok(digest),
            Err(e) => Err(format!("{:?}", e)),
        }
    })
}

pub async fn write_defrag_digest(
    digest: &FragmentationDigest,
    base_path: &str,
) -> Result<(), String> {
    let bs = serde_json::to_vec(&digest).unwrap();
    let digest_path = format!("{}/{}", base_path, ZFS_DIGEST);
    match File::create(Path::new(&digest_path)).await {
        Ok(mut fdigest) => {
            fdigest.write_all(&bs).await.unwrap();
            Ok(())
        }
        Err(e) => Err(format!(
            "Could not create file: {:?} because of {:?}",
            &digest_path, e
        )),
    }
}
pub async fn defragment(zfs: Arc<Mutex<ZFS>>, key: &str, dest: &str) -> Result<bool, String> {
    let home = zfs.lock().await.home.clone();
    let zfs_key = ZFSKey::from(key);
    let fragments_path = zfsd_download_frags_dir_for_key(home, &zfs_key);

    match read_defrag_digest(&fragments_path).await {
        Ok(digest) => {
            let dest_path = Path::new(dest);
            let dest_dir = dest_path.parent().unwrap().to_str().unwrap().to_string();
            create_dir_all(Path::new(&dest_dir)).await.unwrap();

            let mut f = File::create(Path::new(&dest)).await.unwrap();
            for i in 0..digest.fragments {
                let frag_path = format!("{}/{}", fragments_path, i);
                let bs = std::fs::read(Path::new(&frag_path)).unwrap();
                f.write_all(&bs).await.unwrap();
            }

            drop(f);
            let crc64 = Crc::new(dest).checksum().unwrap().crc64;
            Ok(crc64 == digest.crc)
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::io::Write;

    fn create_test_dir() -> PathBuf {
        let test_id = uuid::Uuid::new_v4().to_string();
        let test_dir = std::env::temp_dir().join(format!("zfs_test_{}", test_id));
        std::fs::create_dir_all(&test_dir).unwrap();
        test_dir
    }

    fn cleanup_test_dir(path: &Path) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn test_write_and_read_defrag_digest() {
        let test_dir = create_test_dir();

        let digest = FragmentationDigest {
            name: "test/key".to_string(),
            size: 1024,
            crc: 12345678,
            fragment_size: 256,
            fragments: 4,
        };

        let result = write_defrag_digest(&digest, test_dir.to_str().unwrap()).await;
        assert!(result.is_ok());

        let read_result = read_defrag_digest(test_dir.to_str().unwrap()).await;
        assert!(read_result.is_ok());

        let read_digest = read_result.unwrap();
        assert_eq!(read_digest.name, digest.name);
        assert_eq!(read_digest.size, digest.size);
        assert_eq!(read_digest.crc, digest.crc);
        assert_eq!(read_digest.fragment_size, digest.fragment_size);
        assert_eq!(read_digest.fragments, digest.fragments);

        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_read_defrag_digest_missing_file() {
        let test_dir = create_test_dir();

        let result = read_defrag_digest(test_dir.to_str().unwrap()).await;
        assert!(result.is_err());

        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    #[serial]
    async fn test_fragment_and_defragment_roundtrip() {
        let test_dir = create_test_dir();
        let test_id = test_dir.file_name().unwrap().to_str().unwrap();

        // Set ZFSD_HOME to our test directory for isolation
        std::env::set_var("ZFSD_HOME", test_dir.to_str().unwrap());

        // Create a test file with known content
        let source_file = test_dir.join("source.bin");
        let test_data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        {
            let mut f = std::fs::File::create(&source_file).unwrap();
            f.write_all(&test_data).unwrap();
        }

        // Fragment the file
        let zkey = format!("{}/testkey", test_id);
        let fragment_size = 256;
        let result = fragment(source_file.to_str().unwrap(), &zkey, fragment_size).await;
        assert!(result.is_ok());

        let digest = result.unwrap();
        assert_eq!(digest.fragment_size, fragment_size);
        assert_eq!(digest.fragments, 4); // 1000 bytes / 256 = 3.9, so 4 fragments

        // Copy fragments to download location to simulate download
        let upload_frags = zfsd_upload_frags_dir_for_key(&zkey);
        let download_frags = zfsd_download_frags_dir_for_key(&zkey);
        std::fs::create_dir_all(&download_frags).unwrap();

        for i in 0..digest.fragments {
            let src = format!("{}/{}", upload_frags, i);
            let dst = format!("{}/{}", download_frags, i);
            std::fs::copy(&src, &dst).unwrap();
        }
        // Write digest to download location
        write_defrag_digest(&digest, &download_frags).await.unwrap();

        // Defragment the file
        let dest_file = test_dir.join("dest.bin");
        let defrag_result = defragment(&zkey, dest_file.to_str().unwrap()).await;
        assert!(defrag_result.is_ok());
        assert!(defrag_result.unwrap()); // CRC should match

        // Verify content matches
        let restored_data = std::fs::read(&dest_file).unwrap();
        assert_eq!(restored_data, test_data);

        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    #[serial]
    async fn test_fragment_small_file() {
        let test_dir = create_test_dir();
        let test_id = test_dir.file_name().unwrap().to_str().unwrap();

        std::env::set_var("ZFSD_HOME", test_dir.to_str().unwrap());

        // Create a small file (smaller than fragment size)
        let source_file = test_dir.join("small.bin");
        let test_data = b"Hello, World!";
        {
            let mut f = std::fs::File::create(&source_file).unwrap();
            f.write_all(test_data).unwrap();
        }

        let zkey = format!("{}/smallkey", test_id);
        let fragment_size = 1024; // Larger than file
        let result = fragment(source_file.to_str().unwrap(), &zkey, fragment_size).await;
        assert!(result.is_ok());

        let digest = result.unwrap();
        assert_eq!(digest.fragments, 1); // Should be single fragment

        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    #[serial]
    async fn test_fragment_exact_multiple() {
        let test_dir = create_test_dir();
        let test_id = test_dir.file_name().unwrap().to_str().unwrap();

        std::env::set_var("ZFSD_HOME", test_dir.to_str().unwrap());

        // Create a file that's exactly a multiple of fragment size
        let source_file = test_dir.join("exact.bin");
        let test_data: Vec<u8> = vec![0xAB; 512];
        {
            let mut f = std::fs::File::create(&source_file).unwrap();
            f.write_all(&test_data).unwrap();
        }

        let zkey = format!("{}/exactkey", test_id);
        let fragment_size = 128;
        let result = fragment(source_file.to_str().unwrap(), &zkey, fragment_size).await;
        assert!(result.is_ok());

        let digest = result.unwrap();
        assert_eq!(digest.fragments, 4); // 512 / 128 = 4 fragments

        cleanup_test_dir(&test_dir);
    }

    #[test]
    fn test_fragmentation_digest_serialization() {
        let digest = FragmentationDigest {
            name: "test/key".to_string(),
            size: 2048,
            crc: 9876543210,
            fragment_size: 512,
            fragments: 4,
        };

        let json = serde_json::to_string(&digest).unwrap();
        let parsed: FragmentationDigest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.name, digest.name);
        assert_eq!(parsed.size, digest.size);
        assert_eq!(parsed.crc, digest.crc);
        assert_eq!(parsed.fragment_size, digest.fragment_size);
        assert_eq!(parsed.fragments, digest.fragments);
    }
}
