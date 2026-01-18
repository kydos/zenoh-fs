use clap::{Arg, Command};
use futures::TryFutureExt;
use notify::{recommended_watcher, RecursiveMode, Result, Watcher};
use std::fs::create_dir_all;
use std::process::exit;
use std::sync::mpsc::channel;
use zenoh::config::WhatAmI;
use zfs::*;

fn init() -> Result<()> {
    create_dir_all(zfsd_upload_frags_dir())
        .and(create_dir_all(zfsd_download_frags_dir()))
        .and(create_dir_all(zfsd_upload_digest_dir()))
        .and(create_dir_all(zfsd_download_digest_dir()))
        .map_err(|e| notify::Error::generic(&format!("{:?}", e)))
}

#[tokio::main]
async fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .format_target(true)
        .format_timestamp_secs()
        .init();

    log::info!(target: "zfsd", "Starting up...");
    let zconf = parse_args();

    let z = std::sync::Arc::new(zenoh::open(zconf).await.unwrap());
    init().expect("zfsd failed to initalise!");
    let (tx, rx) = channel();
    let mut watcher = recommended_watcher(tx).unwrap();

    watcher
        .watch(
            std::path::Path::new(&zfsd_download_digest_dir()),
            RecursiveMode::NonRecursive,
        )
        .unwrap();
    watcher
        .watch(
            std::path::Path::new(&zfsd_upload_digest_dir()),
            RecursiveMode::NonRecursive,
        )
        .unwrap();
    watcher
        .watch(
            std::path::Path::new(&zfsd_upload_frags_dir()),
            RecursiveMode::Recursive,
        )
        .unwrap();

    tokio::task::spawn(download_sanitizer(z.clone()));

    log::info!(target:"zfsd", "Up and Running!");
    while let Ok(r) = rx.recv() {
        if let Ok(evt) = r {
            if evt.kind.is_create() && !evt.paths.is_empty() && evt.paths[0].is_file() {
                log::debug!(target: "zfsd", "Received Create Event {:?}", &evt);
                let path = evt.paths[0].clone();
                let Some(parent) = path.parent() else {
                    log::warn!(target: "zfsd", "Path has no parent: {:?}", &path);
                    continue;
                };

                if parent.ends_with(DOWNLOAD_SUBDIR) {
                    log::info!(target: "zfsd", "Downloading {:?}", &path);
                    tokio::task::spawn(zfs::download(z.clone(), path.clone()).or_else(
                        |e| async move {
                            log::warn!("Failed to download due to: {}", e);
                            Ok::<(), String>(())
                        },
                    ));
                } else if parent.ends_with(UPLOAD_SUBDIR) {
                    log::info!(target: "zfsd","Fragmenting {:?}", &path);
                    let Some(p) = path.to_str() else {
                        log::warn!(target: "zfsd", "Path is not valid UTF-8: {:?}", &path);
                        continue;
                    };
                    let p = p.to_string();
                    let _ignore =
                        tokio::task::spawn(zfs::fragment_from_digest(p).or_else(|e| async move {
                            log::warn!("Failed to fragment due to: {}", e);
                            Ok::<(), String>(())
                        }));
                } else {
                    let Some(fpath) = path.to_str() else {
                        log::warn!(target: "zfsd", "Path is not valid UTF-8: {:?}", &path);
                        continue;
                    };
                    if !fpath.contains(DOWNLOAD_SUBDIR) {
                        match fpath.find(FRAGS_SUBDIR) {
                            Some(_) => match zfsd_upload_frag_dir_to_key(fpath) {
                                Some(key_suffix) => {
                                    let key = zfs_key(&key_suffix);
                                    log::debug!(target: "zfsd", "Uploading fragment : {:?} as {:?}", path, &key);
                                    upload_fragment(&z, fpath, &key).await;
                                }
                                None => {
                                    log::warn!(target: "zfsd", "Unable to extract key from {}", fpath);
                                }
                            },
                            None => {
                                log::warn!(target: "zfsd", "Ignoring {:?} path...", &path);
                            }
                        }
                    }
                }
            } else {
                log::debug!(target: "zfsd", "Ignoring create event for directory {:?}", &evt);
            }
        }
    }
}

fn parse_args() -> zenoh::config::Config {
    let args = Command::new("zenoh distributed file system")
        .arg(
            Arg::new("mode")
                .short('m')
                .long("mode")
                .value_name("MODE")
                .help("The zenoh session mode (peer by default).")
                .value_parser(["peer", "client"]),
        )
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("A zenoh configuration file."),
        )
        .arg(
            Arg::new("remote-endpoints")
                .short('r')
                .long("remote-endpoints")
                .value_name("ENDPOINTS")
                .num_args(1..)
                .help("The locators for a remote zenoh endpoint such as routers"),
        )
        .get_matches();

    let mut config = args.get_one::<String>("config").map_or_else(
        || zenoh::Config::default(),
        |conf_file| {
            zenoh::Config::from_file(conf_file).unwrap_or_else(|e| {
                eprintln!("Failed to load config file '{}': {}", conf_file, e);
                exit(1);
            })
        },
    );

    if let Some(mode) = args.get_one::<String>("mode") {
        let mode_value = match mode.as_str() {
            "peer" => WhatAmI::Peer,
            "client" => WhatAmI::Client,
            _ => unreachable!("clap validates mode values"),
        };
        config.set_mode(Some(mode_value)).unwrap();
    }

    if let Some(values) = args.get_many::<String>("remote-endpoints") {
        let endpoints: Vec<_> = values
            .map(|v| {
                v.parse().unwrap_or_else(|e| {
                    eprintln!("Invalid endpoint '{}': {}", v, e);
                    exit(1);
                })
            })
            .collect();
        config
            .connect
            .endpoints
            .set(endpoints)
            .expect("Failed to set endpoints");
    }

    config
}
