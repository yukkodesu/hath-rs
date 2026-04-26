# Stage 1: Build (requires BuildKit for cache mounts)
FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev openssl openssl-dev openssl-libs-static libcrypto3 pkgconfig libc-dev perl

WORKDIR /build

# Layer 1: Dependencies only (cached when Cargo.toml/Cargo.lock unchanged)
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs
RUN --mount=type=cache,target=/build/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release

# Layer 2: Full source (incremental if only src/ changed)
COPY src/ src/
RUN --mount=type=cache,target=/build/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release && \
    mkdir -p /build/out && \
    cp /build/target/release/hath-rs /build/out/hath-rs

# Stage 2: Runtime
FROM alpine:latest

RUN apk add --no-cache tini ca-certificates libgcc musl openssl openssl-dev openssl-libs-static libcrypto3

COPY --from=builder /build/out/hath-rs /usr/local/bin/hath-rs
COPY docker-entrypoint.sh /docker-entrypoint.sh

VOLUME ["/hath/cache", "/hath/data", "/hath/download", "/hath/log", "/hath/tmp"]
WORKDIR /hath

ENTRYPOINT ["/sbin/tini", "--", "/bin/sh", "/docker-entrypoint.sh"]
