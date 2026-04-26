# rustls → OpenSSL TLS Migration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace rustls/tokio-rustls with OpenSSL-based TLS to unify on a single TLS library, eliminate cert/key format conversions, and improve cipher suite compatibility with the Java H@H client.

**Architecture:** The current hybrid stack (openssl for PKCS12 parsing → convert to rustls types → rustls for TLS) becomes a single-stack design: openssl for PKCS12 parsing → build `SslContext` directly → tokio-openssl for async TLS accept. On the client side, reqwest switches from its `rustls` feature to `native-tls` (which resolves to OpenSSL on Linux).

**Tech Stack:** openssl 0.10.78, tokio-openssl 0.6.5, reqwest 0.13.2 (native-tls feature)

---

### Task 1: Update dependencies

**Files:**
- Modify: `hath-rs/Cargo.toml`

- [ ] **Step 1: Replace TLS dependencies in Cargo.toml**

Remove:
```toml
rustls = "0.23"
tokio-rustls = "0.26"
rustls-pemfile = "2"
```

Add:
```toml
tokio-openssl = "0.6"
```

Change reqwest features from `["rustls", "socks"]` to `["native-tls", "socks"]`:
```toml
reqwest = { version = "0.13.2", default-features = false, features = ["native-tls", "socks"] }
```

- [ ] **Step 2: Update Cargo.lock**

Run: `cargo update -p reqwest -p openssl 2>&1`
Expected: no errors

- [ ] **Step 3: Verify dependency tree**

Run: `cargo tree -p hath-rs -i rustls 2>&1`
Expected: `error: package ID specification `rustls` did not match any packages`

Run: `cargo tree -p hath-rs -i tokio-openssl 2>&1`
Expected: shows `tokio-openssl v0.6.5` in dependency tree

---

### Task 2: Remove rustls error conversion

**Files:**
- Modify: `hath-rs/src/error.rs:61-65`

- [ ] **Step 1: Delete `From<rustls::Error>` impl**

Remove lines 61-65:
```rust
impl From<rustls::Error> for HathError {
    fn from(e: rustls::Error) -> Self {
        HathError::Tls(e.to_string())
    }
}
```

The `From<openssl::error::ErrorStack>` (lines 49-53) and `From<openssl::ssl::Error>` (lines 55-59) are kept — they already handle OpenSSL errors.

- [ ] **Step 2: Verify error.rs compiles**

Run: `cargo check -p hath-rs 2>&1 | head -5`
Expected: compilation errors in server.rs (not error.rs) — we'll fix those in the next tasks.

---

### Task 3: Rewrite `build_tls_acceptor()` to return OpenSSL types

**Files:**
- Modify: `hath-rs/src/server.rs:22-27` (imports)
- Modify: `hath-rs/src/server.rs:571-641` (function body)

- [ ] **Step 1: Replace imports (lines 22-27)**

Remove:
```rust
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;
```

Add:
```rust
use openssl::ssl::{SslAcceptor, SslMethod, SslContext};
use openssl::x509::X509Ref;
```

Keep (unchanged):
```rust
use openssl::asn1::Asn1Time;
use openssl::pkcs12::Pkcs12;
use openssl::provider::Provider;
```

- [ ] **Step 2: Change return type and function signature**

Change lines 569-571 from:
```rust
async fn build_tls_acceptor(config: &Config, force_download: bool) -> Result<(TlsAcceptor, i64)> {
    let cert_path = config.data_dir.join("hathcert.p12");
```
To:
```rust
async fn build_tls_acceptor(config: &Config, force_download: bool) -> Result<(SslContext, i64)> {
    let cert_path = config.data_dir.join("hathcert.p12");
```

- [ ] **Step 3: Remove cert-to-DER conversion block (lines 614-622)**

Remove:
```rust
    // Convert cert and chain from OpenSSL X509 → DER → rustls CertificateDer
    let cert_der = cert.to_der()?;
    let mut cert_chain = vec![CertificateDer::from(cert_der)];
    if let Some(chain) = pkcs12.ca {
        for ca in chain {
            let ca_der = ca.to_der()?;
            cert_chain.push(CertificateDer::from(ca_der));
        }
    }
```

- [ ] **Step 4: Remove key-to-PEM conversion block (lines 624-631)**

Remove:
```rust
    // Convert private key: OpenSSL PKey → PEM PKCS#8 → rustls PrivatePkcs8KeyDer
    let key_pem = key.private_key_to_pem_pkcs8()?;
    let mut pem_reader = std::io::BufReader::new(key_pem.as_slice());
    let key_der = rustls_pemfile::pkcs8_private_keys(&mut pem_reader)
        .next()
        .ok_or_else(|| HathError::Tls("No private key found in PEM".into()))?
        .map_err(|e| HathError::Tls(format!("Failed to parse private key: {}", e)))?;
    let private_key = PrivateKeyDer::Pkcs8(key_der);
```

- [ ] **Step 5: Replace rustls ServerConfig builder with SslAcceptor (lines 633-640)**

Remove:
```rust
    // Build rustls ServerConfig with TLS 1.2 + 1.3 (matching Java)
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|e| HathError::Tls(format!("Failed to build TLS config: {}", e)))?;

    let acceptor = TlsAcceptor::from(Arc::new(config));
    Ok((acceptor, cert_expiry_unix))
```

Add:
```rust
    // Build OpenSSL SslAcceptor with Mozilla intermediate profile (TLS 1.2 + 1.3, matching Java)
    let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server())
        .map_err(|e| HathError::Tls(format!("Failed to create SslAcceptor: {}", e)))?;
    acceptor
        .set_private_key(key)
        .map_err(|e| HathError::Tls(format!("Failed to set private key: {}", e)))?;
    acceptor
        .set_certificate(cert)
        .map_err(|e| HathError::Tls(format!("Failed to set certificate: {}", e)))?;
    if let Some(chain) = pkcs12.ca {
        for ca in chain {
            acceptor
                .add_extra_chain_cert(ca)
                .map_err(|e| HathError::Tls(format!("Failed to add chain cert: {}", e)))?;
        }
    }
    let ctx = acceptor.build();
    Ok((ctx, cert_expiry_unix))
```

- [ ] **Step 6: Compile check server.rs specifically**

Run: `cargo check -p hath-rs 2>&1`
Expected: errors in `start_server()` (Task 4), not in `build_tls_acceptor()`.

---

### Task 4: Update `AppState` and TLS accept in `start_server()`

**Files:**
- Modify: `hath-rs/src/server.rs:50-51` (AppState field)
- Modify: `hath-rs/src/server.rs:907` (store acceptor)
- Modify: `hath-rs/src/server.rs:1003-1021` (accept TLS)

- [ ] **Step 1: Change AppState.tls_acceptor type (lines 50-51)**

Change from:
```rust
    /// TLS acceptor that can be swapped at runtime (e.g. cert refresh).
    pub tls_acceptor: Arc<ArcSwapOption<TlsAcceptor>>,
```
To:
```rust
    /// TLS context that can be swapped at runtime (e.g. cert refresh).
    pub tls_acceptor: Arc<ArcSwapOption<SslContext>>,
```

- [ ] **Step 2: Update import for ArcSwapOption**

Add `SslContext` to imports (already done in Task 3 Step 1 if `openssl::ssl::SslContext` was added). Verify line 24 includes `SslContext`.

- [ ] **Step 3: Update store call (line 907)**

Change from:
```rust
    state.tls_acceptor.store(Some(Arc::new(tls_acceptor.clone())));
```
To:
```rust
    state.tls_acceptor.store(Some(Arc::new(tls_acceptor)));
```

(`SslContext` is not `Clone`, but `.build()` gives ownership. We wrap it in `Arc` and store it. No `.clone()` needed.)

- [ ] **Step 4: Replace TLS accept code (lines 1003-1021)**

Change from:
```rust
                // Load current TLS acceptor (may have been refreshed)
                let acceptor = state.tls_acceptor.load_full()
                    .expect("TLS acceptor not initialized");

                let conn_state = state.clone();
                tokio::spawn(async move {
                    let _guard = ConnectionGuard {
                        active_connections: conn_state.active_connections.clone(),
                        stats: conn_state.stats.clone(),
                    };

                    let tls_stream = match acceptor.accept(stream).await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!("TLS accept failed: {}", e);
                            return;
                        }
                    };

                    let io = TokioIo::new(tls_stream);
```
To:
```rust
                // Load current TLS context (may have been refreshed)
                let ssl_context = state.tls_acceptor.load_full()
                    .expect("TLS context not initialized");

                let conn_state = state.clone();
                tokio::spawn(async move {
                    let _guard = ConnectionGuard {
                        active_connections: conn_state.active_connections.clone(),
                        stats: conn_state.stats.clone(),
                    };

                    let ssl = match openssl::ssl::Ssl::new(ssl_context.as_ref()) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!("SSL context error: {}", e);
                            return;
                        }
                    };
                    let mut tls_stream = match tokio_openssl::SslStream::new(ssl, stream) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!("TLS accept failed: {}", e);
                            return;
                        }
                    };
                    use std::pin::Pin;
                    if let Err(e) = Pin::new(&mut tls_stream).accept().await {
                        tracing::warn!("TLS accept failed: {}", e);
                        return;
                    }

                    let io = TokioIo::new(tls_stream);
```

- [ ] **Step 5: Add `use std::pin::Pin` to imports**

Add to the end of the import block (around line 34):
```rust
use std::pin::Pin;
```

Actually, check if `Pin` is already imported. If it is (line 31: `use std::pin::Pin;` is already there), skip. If not, add it.

- [ ] **Step 6: Build and fix any remaining compilation errors**

Run: `cargo check -p hath-rs 2>&1`
Expected: clean compilation (no errors, no warnings from our changes).

---

### Task 5: Verify full compilation

**Files:** None (verification only)

- [ ] **Step 1: Full release check**

Run: `cargo check --release -p hath-rs 2>&1`
Expected: `Finished` with no errors.

- [ ] **Step 2: Check for unused imports**

Run: `cargo check -p hath-rs 2>&1 | grep -i "unused import\|warning.*unused"`
Expected: no warnings about `rustls`, `tokio_rustls`, `rustls_pemfile`, `CertificateDer`, `PrivateKeyDer`, `TlsAcceptor`.

- [ ] **Step 3: Verify no rustls references remain**

Run: `grep -rn "rustls\|rustls_pemfile" hath-rs/src/`
Expected: no matches.

---

### Task 6: Self-review

**Files:** None (review only)

- [ ] **Step 1: Verify each change against the original Java TLS configuration**

Check:
- TLS 1.2 + 1.3 both enabled? Yes — Mozilla intermediate v5 profile enables TLS 1.2 + 1.3.
- No client certificate auth? Yes — SslAcceptor default is no client auth.
- Cert chain (CA certs) attached? Yes — `add_extra_chain_cert()` covers this.

- [ ] **Step 2: Verify certificate refresh still works**

Trace the flow: `do_cert_refresh` flag → `spawn_cert_refresh_watcher` → calls `start_server` → which calls `build_tls_acceptor()` → stores new `SslContext` in `state.tls_acceptor` → new connections use the refreshed context. Same flow as before.

- [ ] **Step 3: Verify reqwest client changes are transparent**

Check files using `Client::builder()`:
- `rpc_client.rs:20-24` — uses `Client::builder()`, no TLS-specific API calls.
- `downloader.rs:52-65` — uses `Client::builder()`, optional proxy, no TLS-specific API calls.
- `proxy_downloader.rs:55` — uses `Client::builder()`, no TLS-specific API calls.

None of these touch TLS APIs directly; reqwest handles the backend transparently. No source changes needed.
