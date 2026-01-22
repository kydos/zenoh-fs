use clap::{Arg, Command};
use std::process::exit;
use zfs::{zfsd_upload_digest_dir, UploadDigest};

fn write_upload_digest(zfsd_home: String, digest: UploadDigest) -> std::io::Result<()> {
    let uid = uuid::Uuid::new_v4();
    let fname = format!("{}/{}", zfsd_upload_digest_dir(zfsd_home), uid);
    let bs = serde_json::to_vec(&digest)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(&fname, &bs)?;
    Ok(())
}

fn parse_args() -> (String, String, usize, String) {
    let args = Command::new("zut: zfs utility to upload files.")
        .arg(
            Arg::new("path")
                .short('p')
                .long("path")
                .value_name("PATH")
                .help("The path for the file to upload.")
                .required(true),
        )
        .arg(
            Arg::new("key")
                .short('k')
                .long("key")
                .value_name("KEY")
                .help("The key under which this file will be stored in zfs.")
                .required(true),
        )
        .arg(
            Arg::new("fragment")
                .short('f')
                .long("fragment")
                .value_name("BYTES")
                .help("The size of the fragment")
                .default_value("32768"),
        )
        .arg(
            Arg::new("home")
                .short('H')
                .long("home")
                .value_name("PATH")
                .help("The the home path for zfsd."),
        )
        .get_matches();

    let fragment_size = args
        .get_one::<String>("fragment")
        .unwrap()
        .parse()
        .unwrap_or_else(|e| {
            eprintln!("Invalid fragment size: {}", e);
            exit(1);
        });

    let home = {
        if let Some(h) = args.get_one::<String>("home") {
            h.to_string()
        } else {
            if let Ok(path) = std::env::var("ZFSD_HOME") {
                path
            } else {
                format!("{}/{}", std::env::var("HOME").unwrap(), ".zfsd")
            }
        }
    };

    (
        args.get_one::<String>("path").unwrap().to_string(),
        args.get_one::<String>("key").unwrap().to_string(),
        fragment_size,
        home,
    )
}

fn main() {
    let (path, key, fragment_size, zfsd_home) = parse_args();
    if std::path::Path::new(&path).exists() {
        let digest = UploadDigest {
            path,
            key,
            fragment_size,
        };
        if let Err(e) = write_upload_digest(zfsd_home, digest) {
            eprintln!("Failed to write upload digest: {}", e);
            exit(1);
        }
    } else {
        eprintln!("The file {} does not exist", &path);
        exit(1);
    }
}
