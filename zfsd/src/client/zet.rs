use clap::{Arg, Command};
use std::process::exit;
use zfs::{zfsd_download_digest_dir, DownloadDigest};

fn write_download_digest(digest: DownloadDigest) -> std::io::Result<()> {
    let uid = uuid::Uuid::new_v4();
    let fname = format!("{}/{}", zfsd_download_digest_dir(), uid);
    let bs = serde_json::to_vec(&digest).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;
    std::fs::write(&fname, &bs)?;
    Ok(())
}

fn parse_args() -> (String, String, usize) {
    let args = Command::new("zet: zfs utility to download files.")
        .arg(
            Arg::new("path")
                .short('p')
                .long("path")
                .value_name("PATH")
                .help("The path to download the file to.")
                .required(true),
        )
        .arg(
            Arg::new("key")
                .short('k')
                .long("key")
                .value_name("KEY")
                .help("The key of the file to download.")
                .required(true),
        )
        .arg(
            Arg::new("tempo")
                .short('t')
                .long("tempo")
                .value_name("MSEC")
                .help("The time in msec that should be waited before downloading the next fragment (0 means as fast as possible).")
                .default_value("0"),
        )
        .get_matches();

    let pace = args
        .get_one::<String>("tempo")
        .unwrap()
        .parse()
        .unwrap_or_else(|e| {
            eprintln!("Invalid tempo value: {}", e);
            exit(1);
        });

    (
        args.get_one::<String>("path").unwrap().to_string(),
        args.get_one::<String>("key").unwrap().to_string(),
        pace,
    )
}

fn main() {
    let (path, key, pace) = parse_args();
    let digest = DownloadDigest { path, key, pace };
    if let Err(e) = write_download_digest(digest) {
        eprintln!("Failed to write download digest: {}", e);
        exit(1);
    }
}
