# zenoh-fs

[![CI](https://github.com/eclipse-zenoh/zenoh-fs/actions/workflows/ci.yml/badge.svg)](https://github.com/eclipse-zenoh/zenoh-fs/actions/workflows/ci.yml)

A Zenoh-based delay tolerant distributed file system supporting extremely large data files.

`zenoh-fs` provides utilities to fragment and reassemble large files for efficient transfer over [Zenoh](https://zenoh.io/) pub/sub infrastructure, enabling seamless upload and download to/from Zenoh storages. The protocol used by `zenoh-fs` is delay-tolerant, as such it is designed to be deployed across networks that face frequent connection losses or even endpoint restarts.

## Architecture

The project consists of the following components:

| Component | Description |
|-----------|-------------|
| `zfs` | Core library providing file fragmentation, reassembly, and transfer utilities |
| `zfsd` | Daemon that monitors directories and automatically uploads/downloads files |
| `zut` | CLI utility for uploading files to Zenoh storages |
| `zet` | CLI utility for downloading files from Zenoh storages |

## Prerequisites

- **Rust toolchain**: Install via [rustup](https://www.rust-lang.org/tools/install)
- **Zenoh router**: Install from [eclipse-zenoh/zenoh](https://github.com/eclipse-zenoh/zenoh)
- **Zenoh filesystem backend**: Install from [eclipse-zenoh/zenoh-backend-filesystem](https://github.com/eclipse-zenoh/zenoh-backend-filesystem)

Ensure all Zenoh plugin libraries are available under `~/.zenoh/lib`.

## Building

```bash
cargo build --release --all
```

Binaries will be available in `./target/release/`.

## Usage

### Starting Zenoh Router

Start the Zenoh router with the storage plugin configured:

```bash
zenohd -c zenoh.json5
```

### Starting the Daemon

The `zfsd` daemon monitors directories for files to upload and manages downloads:

```bash
./target/release/zfsd -c zenoh.json5 -r <zenohd-locator>
```

### Uploading Files

Use `zut` to upload a file:

```bash
./target/release/zut -k <key> -p <file_path>
```

**Example:**
```bash
./target/release/zut -k mydata/document -p ./large_file.bin
```

**Options:**
- `-k, --key <KEY>` - The key under which the file will be stored
- `-p, --path <PATH>` - Path to the file to upload
- `-f, --fragment <BYTES>` - Fragment size in bytes (default: 32768)

### Downloading Files

Use `zet` to download a file:

```bash
./target/release/zet -k <key> -p <destination>
```

**Example:**
```bash
./target/release/zet -k mydata/document -p ./downloaded_file.bin
```

**Options:**
- `-k, --key <KEY>` - The key of the file to download
- `-p, --path <PATH>` - Destination path for the downloaded file

## Listing Hosted Files
The registry of hotest files is available through a Zenoh queriable that answers on the key expression 
`zfsd/registry/*`. If you have handy a Zenoh distribution you can easy get the list of hosted files by simply:

```bash
  $ z_get -s - zfsd/registry/ls
```

## HowTo Use
`zenoh-fs` does not provide programming API since you can easily craft from any applications, and even from
a command line script the digest that are necessary to **upload** and **download** files.

Check the `zet` and  `zut` command line applications to understand how this works. You'll notice that those applications are not using any `zenoh-fs` API, they are just dropping files that are JSON representations of these types:

```rust
pub struct UploadDigest {
    pub path: String,
    pub key: String,
    pub fragment_size: usize,
}
```
```rust
pub struct DownloadDigest {
    pub key: String,
    pub path: String,
    pub pace: usize,
}
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ZFSD_HOME` | Directory for zfsd working files | `~/.zfsd` |
| `ZBACKEND_FS_ROOT` | Root directory for Zenoh filesystem backend | (Zenoh default) |

### Zenoh Configuration

The included `zenoh.json5` configures:
- Filesystem storage backend for the `zfs/**` key expression
- REST API on port 8000

## Directory Structure

### Local (`$ZFSD_HOME`)

```
.zfsd/
├── digest/
│   ├── download/    # Download task metadata
│   └── upload/      # Upload task metadata
└── frags/
    ├── download/    # Downloaded fragments (pending reassembly)
    └── upload/      # Fragments awaiting upload
```

### Zenoh Storage

```
zfs/
└── <key>/
    ├── zfs-digest   # File metadata (name, size, checksum, fragment count)
    ├── 0            # Fragment 0
    ├── 1            # Fragment 1
    └── ...          # Additional fragments
```

## Deployment

**Local testing:** Run a single Zenoh router with `zfsd` on the same machine.

**Distributed setup:** Start a Zenoh router, then run `zfsd` instances on multiple machines. Use `zut` on one machine to upload and `zet` on another to download.

## License

This project is licensed under the Eclipse Public License 2.0 - see the [LICENSE](LICENSE) file for details.
