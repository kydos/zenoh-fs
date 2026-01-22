use clap::{Arg, Command};
use derive_builder::Builder;
use futures::TryFutureExt;
use notify::{recommended_watcher, RecursiveMode, Watcher};
use std::process::exit;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use zenoh::config::WhatAmI;
use zfs::*;

#[tokio::main]
async fn main() {
    let (zfsconf, zconf) = parse_args();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .format_target(true)
        .format_timestamp_secs()
        .init();

    log::info!(target: "zfsd", "Starting up...");

    let z = Arc::new(zenoh::open(zconf).await.unwrap());
    let zfs = ZFSBuilder::default()
        .home(zfsconf.home.clone())
        .recovery_pace(zfsconf.repair_pace)
        .recovery_priority(zfsconf.repair_priority)
        .registry_key(zfsconf.registry_key)
        .z_session(z.clone())
        .build()
        .unwrap();

    zfs.init().await.expect("zfsd failed to initalise!");
    let zfs = Arc::new(Mutex::new(zfs));

    let (tx, rx) = channel();
    let mut watcher = recommended_watcher(tx).unwrap();

    watcher
        .watch(
            std::path::Path::new(&zfsd_download_digest_dir(zfsconf.home.clone())),
            RecursiveMode::NonRecursive,
        )
        .unwrap();
    watcher
        .watch(
            std::path::Path::new(&zfsd_upload_digest_dir(zfsconf.home.clone())),
            RecursiveMode::NonRecursive,
        )
        .unwrap();
    watcher
        .watch(
            std::path::Path::new(&zfsd_upload_frags_dir(zfsconf.home.clone())),
            RecursiveMode::Recursive,
        )
        .unwrap();

    tokio::task::spawn(download_sanitizer(zfs.clone()));

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
                    tokio::task::spawn(zfs::download(zfs.clone(), path.clone()).or_else(
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
                    let _ignore = tokio::task::spawn(
                        zfs::fragment_from_digest(zfs.clone(), p).or_else(|e| async move {
                            log::warn!("Failed to fragment due to: {}", e);
                            Ok::<(), String>(())
                        }),
                    );
                } else {
                    let Some(fpath) = path.to_str() else {
                        log::warn!(target: "zfsd", "Path is not valid UTF-8: {:?}", &path);
                        continue;
                    };
                    if !fpath.contains(DOWNLOAD_SUBDIR) {
                        match fpath.find(FRAGS_SUBDIR) {
                            Some(_) => {
                                match zfsd_upload_frag_dir_to_key(zfsconf.home.clone(), fpath) {
                                    Some(zfs_key) => {
                                        log::debug!(target: "zfsd", "Uploading fragment : {:?} as {:?}", path, &zfs_key);
                                        upload_fragment(zfs.clone(), fpath, &zfs_key).await;
                                    }
                                    None => {
                                        log::warn!(target: "zfsd", "Unable to extract key from {}", fpath);
                                    }
                                }
                            }
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

#[derive(Builder)]
struct ZFSConfig {
    #[builder(default = DEFAULT_REPAIR_PACE)]
    repair_pace: Duration,
    #[builder(default = zenoh::qos::Priority::Background)]
    repair_priority: zenoh::qos::Priority,
    home: String,
    #[builder(default = DEFAULT_REGISTRY_KEY.to_string())]
    registry_key: String,
}

fn parse_args() -> (ZFSConfig, zenoh::config::Config) {
    let args = Command::new("zenoh's distributed and latency tolerant file system")
        .about("A distributed and latency tolerant file-system built on zenoh")
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
        .arg(
            Arg::new("repair-pace")
                .short('p')
                .long("repair-pace")
                .value_name("PACE")
                .help("The period (in millisecs) used to pace request of missing fragments."),
        )
        .arg(
            Arg::new("repair-priority")
                .short('P')
                .long("repair-priority")
                .value_name("INT")
                .help("The Zenoh priority (1-7) to be associated with reparing traffic, the default is 7 (zenoh::qos::Priority::Background)"),
        )
        .arg(
            Arg::new("registry-key")
                .short('k')
                .long("registry-key")
                .value_name("KEY")
                .help("The used by the registry queryable"),
        )
        .arg(
            Arg::new("home")
                .short('H')
                .long("home")
                .value_name("PATH")
                .help("The the home path for zfsd."),
        )
        .get_matches();

    let mut config =
        args.get_one::<String>("config")
            .map_or_else(zenoh::Config::default, |conf_file| {
                zenoh::Config::from_file(conf_file).unwrap_or_else(|e| {
                    eprintln!("Failed to load config file '{}': {}", conf_file, e);
                    exit(1);
                })
            });

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

    let pace = if let Some(v) = args.get_one::<u64>("repair-pace") {
        Duration::from_millis(*v)
    } else {
        DEFAULT_REPAIR_PACE
    };

    let reg_key = if let Some(v) = args.get_one::<String>("registry-key") {
        v.to_string()
    } else {
        DEFAULT_REGISTRY_KEY.to_string()
    };

    let prio = if let Some(v) = args.get_one::<u8>("repair-priority") {
        if *v < 1 {
            zenoh::qos::Priority::RealTime
        } else if *v > 7 {
            zenoh::qos::Priority::Background
        } else {
            zenoh::qos::Priority::try_from(*v as u8).unwrap()
        }
    } else {
        zenoh::qos::Priority::Background
    };

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
    let zfsc = ZFSConfigBuilder::default()
        .registry_key(reg_key)
        .repair_pace(pace)
        .repair_priority(prio)
        .home(home)
        .build()
        .unwrap();

    (zfsc, config)
}
