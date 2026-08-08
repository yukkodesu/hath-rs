# Rustls, AWS-LC, and Legacy PKCS#12 Design

## Goal

Remove the OpenSSL runtime dependency while retaining support for the H@H
certificate archive, whose PKCS#12 bags use RC2-40-CBC. Build both inbound
server TLS and outbound HTTPS on rustls with the AWS-LC crypto provider, and
produce an Alpine/musl image with no OpenSSL libraries or providers.

## Scope

This design changes the Rust TLS implementation, legacy PKCS#12 import path,
and Docker build/runtime dependencies. It preserves the existing certificate
download, certificate refresh, traffic suspension, admission, HTTP handling,
and proxy behavior.

It does not adopt the upstream `hath-rust` rustls fork or change TLS policy
beyond the current minimum TLS 1.2 requirement. A future compatibility issue
may justify that fork, but it is not part of this migration.

## Architecture

```text
hathcert.p12
    |
    | p12 crate: ASN.1, PKCS#12 PBE/KDF, SHA-1, RC2-40-CBC
    v
certificate-chain DER + PKCS#8 private-key DER
    |
    +-- x509-parser: read leaf notAfter
    |
    v
rustls ServerConfig (AWS-LC provider)
    |
    v
tokio-rustls TlsAcceptor -> Hyper

reqwest (rustls-no-provider) <- AWS-LC provider configured explicitly
```

The `p12` crate is the compatibility boundary for the H@H legacy certificate
container. RC2 is never offered as a TLS cipher. Once imported, the key and
certificate remain standard DER objects consumed by rustls.

## Components

### Certificate material

`src/server/tls.rs` remains responsible for downloading `hathcert.p12` and for
the certificate-refresh lifecycle. Its certificate construction routine will:

1. Download the P12 archive when forced or absent, as it does today.
2. Parse it with `p12::PFX`.
3. Extract a PKCS#8 private-key bag and all X.509 certificate bags using the
   configured client key as the P12 password.
4. Reject archives missing a private key or leaf certificate with
   `HathError::Tls`.
5. Parse the leaf certificate validity with a pure Rust X.509 parser and
   calculate Unix expiry time.
6. Reject an already expired or near-expiry certificate using the current
   24-hour renewal window.
7. Build a TLS-1.2-or-newer rustls `ServerConfig` using the AWS-LC provider and
   convert it to `tokio_rustls::TlsAcceptor`.

No PEM conversion, OpenSSL provider loading, or dynamically loaded module is
permitted in this path.

### Inbound TLS server

`AppState` stores the current rustls acceptor rather than an OpenSSL
`SslContext`. Each accepted TCP connection performs the rustls handshake before
the existing post-handshake admission checks, retaining the Java-compatible
connection order. The successful TLS stream continues into the existing Hyper
HTTP/1 connection handler.

Certificate refresh retains its current full-restart flow. A newly constructed
acceptor is installed only for the new server generation.

### Outbound HTTPS

All reqwest clients use `rustls-no-provider` and are explicitly configured with
the same AWS-LC `CryptoProvider`. This includes direct downloader clients,
image-proxy clients, and threaded proxy speed-test clients. SOCKS proxy support
is retained. `rustls-native-certs` loads the platform trust store into each
client's rustls `RootCertStore`, preserving the current native-TLS trust model.
The Docker image retains `ca-certificates` as the source of those roots.

### Docker image

The builder uses Alpine and installs the native build prerequisites required by
`aws-lc-sys` (C compiler/toolchain, CMake, Perl, and NASM where applicable).
It does not install OpenSSL development packages. The previous
`RUSTFLAGS=-C target-feature=-crt-static` override is removed so the musl
target can produce a static executable.

The runtime image does not install `openssl`, `libssl3`, or `libcrypto3`.
It retains `tini`, `ca-certificates`, and `libgcc`, which the existing Alpine
deployment requires. The entrypoint and mounted H@H data directories remain
unchanged.

## Error Handling

P12 parse, password, bag-extraction, certificate-conversion, rustls
configuration, and X.509 validity failures are converted to descriptive
`HathError::Tls` values. Existing startup and refresh behavior reports these
failures through the readiness channel and aborts the affected server
generation; it does not silently start plaintext HTTP or fall back to OpenSSL.

Early TLS disconnects continue to be logged at debug level. Other handshake
failures remain warnings with the peer address.

## Validation

The migration requires a committed, non-sensitive test P12 fixture encrypted
with RC2-40-CBC. Tests verify successful extraction, wrong-password and corrupt
input rejection, rustls certified-key construction, and the existing 24-hour
expiry rule. A container verification builds the Alpine image, checks its ELF
dependencies, verifies the absence of OpenSSL/provider files, and starts the
application through the certificate-loading path.

The production H@H certificate is not committed. Before merge, the maintainer
must perform one manual startup/refresh test with a real credentialed H@H
instance.

## Trade-offs

This removes the operational dependency on OpenSSL's legacy provider and
supports static musl builds. In exchange, PKCS#12 compatibility depends on the
`p12` crate's supported bag/PBE combinations. The RC2-40-CBC fixture and a
manual production validation are required safeguards. The migration also
changes TLS implementation defaults, so protocol/cipher compatibility is
validated rather than assumed.
