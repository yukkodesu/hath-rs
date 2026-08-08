# hath-rs

A stubborn, unofficial Rust implementation of the [Hentai@Home](https://ehwiki.org/wiki/Hentai@Home) client, with best-effort 1:1 feature parity with the official Java client.

Currently updated to match Hentai@Home Java 1.6.5 build 178.

`hath-rs` is neither an official Hentai@Home project nor affiliated with E-Hentai.

## Features

### New

- Low memory usage: about 45 MiB on x86_64 at roughly 50 hits/min, and 20 MiB on aarch64 at roughly 75 hits/min
- Optional mimalloc allocator for lower RSS
- A modern, fast, memory-safe Rust implementation

### Works

Official-client functionality implemented by hath-rs:

- Cache and proxy serving
- Gallery downloader
- Speed tests
- Cache pruning and disk-space checks
- Bandwidth limiting, flood control, and logging
- Full feature parity with Java 1.6.5 build 178

## Planned

- A Prometheus/OpenMetrics endpoint exposing cache, traffic, connection, and client-health metrics.

## Acknowledgements

Thanks to [James](https://github.com/james58899) and the [hath-rust](https://github.com/james58899/hath-rust) project. When hath-rs was originally developed for personal use, it relied on OpenSSL to support legacy RC2-encrypted PKCS#12 certificates. Because OpenSSL made statically linked builds harder to distribute, its current PKCS#12 implementation was informed by hath-rust's approach to those legacy certificates.

## Run with Docker

The published image is Alpine-based and available for `linux/amd64` and `linux/arm64`:

```sh
mkdir hath-rs && cd hath-rs
curl -fsSLO https://raw.githubusercontent.com/yukkodesu/hath-rs/master/deploy/docker-compose.example.yml
mv docker-compose.example.yml docker-compose.yml
export HATH_CLIENT_ID='your-client-id'
export HATH_CLIENT_KEY='your-client-key'
export HATH_PORT='11451'
sed -i.bak \
  -e "s/HATH_CLIENT_ID: xxxx/HATH_CLIENT_ID: ${HATH_CLIENT_ID}/" \
  -e "s/HATH_CLIENT_KEY: xxxx/HATH_CLIENT_KEY: ${HATH_CLIENT_KEY}/" \
  -e "s/11451/${HATH_PORT}/g" \
  docker-compose.yml
rm docker-compose.yml.bak
docker compose up -d
```

The copied compose file stores persistent state in `./volumes/` and publishes the port selected by `HATH_PORT`. Configure the same external port and hostname in the Hentai@Home client settings.

To use a specific version instead of `latest`, set `HATH_IMAGE` before starting Compose:

```sh
export HATH_IMAGE=ghcr.io/yukkodesu/hath-rs:v0.1.0
docker compose up -d
```

For a local image build:

```sh
curl -fsSLo docker-compose.yml https://raw.githubusercontent.com/yukkodesu/hath-rs/master/deploy/docker-compose.build.yml
sed -i.bak \
  -e "s/HATH_CLIENT_ID: xxxx/HATH_CLIENT_ID: ${HATH_CLIENT_ID}/" \
  -e "s/HATH_CLIENT_KEY: xxxx/HATH_CLIENT_KEY: ${HATH_CLIENT_KEY}/" \
  -e "s/11451/${HATH_PORT}/g" \
  docker-compose.yml
rm docker-compose.yml.bak
docker compose up -d --build
```

## Run a release tarball

Set `VERSION` and `TARGET` for your platform, then download, verify, and extract the matching release archive:

```sh
VERSION=0.1.0
TARGET=x86_64-unknown-linux-musl
curl -fLO "https://github.com/yukkodesu/hath-rs/releases/download/v${VERSION}/hath-rs-v${VERSION}-${TARGET}.tar.gz"
curl -fLO "https://github.com/yukkodesu/hath-rs/releases/download/v${VERSION}/SHA256SUMS"
grep "hath-rs-v${VERSION}-${TARGET}.tar.gz$" SHA256SUMS | sha256sum -c -
tar -xzf "hath-rs-v${VERSION}-${TARGET}.tar.gz"
cd "hath-rs-v${VERSION}-${TARGET}"
export HATH_CLIENT_ID='your-client-id'
export HATH_CLIENT_KEY='your-client-key'
export HATH_PORT='11451'
./hath-rs
```

The archives are statically linked with musl and require a 64-bit Linux host. By default, `hath-rs` creates and uses `cache`, `data`, `download`, `log`, and `tmp` in the current directory. Keep those directories when upgrading.

To place a directory elsewhere, set its matching environment variable or pass the equivalent CLI option. For example:

```sh
HATH_DATA_DIR=/srv/hath/data HATH_CACHE_DIR=/srv/hath/cache ./hath-rs
```

## Configuration

Command-line options can also be provided as `HATH_*` environment variables. Run `hath-rs --help` for the full list. The essential options are:

| Variable | Purpose |
| --- | --- |
| `HATH_CLIENT_ID` | Hentai@Home client ID |
| `HATH_CLIENT_KEY` | Hentai@Home client key |
| `HATH_PORT` | Client listening port |
| `HATH_CACHE_DIR` | Cached-file directory |
| `HATH_DATA_DIR` | Persistent client state, including `client_login` |
| `HATH_LOG_DIR` | Log directory |
| `HATH_TEMP_DIR` | Temporary-file directory |
| `HATH_DOWNLOAD_DIR` | Gallery download directory |

Credentials may instead be stored as `CLIENT_ID-CLIENT_KEY` in `client_login` under `HATH_DATA_DIR`. Never commit that file or expose your client key.

## Upgrading

Stop the client cleanly, then start the new image or binary with the same directory paths. Check the release notes for compatibility notes before upgrading.

## License

This project is licensed under the [GNU Affero General Public License v3.0](LICENSE).
