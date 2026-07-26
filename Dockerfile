# Stage 1: Build
FROM rust:alpine AS builder

RUN apk add --no-cache \
    build-base musl-dev openssl-dev pkgconf

# rust:alpine defaults to static musl linking. Keep OpenSSL dynamic so its
# legacy provider can be loaded from Alpine's libcrypto3 package at runtime.
ENV RUSTFLAGS="-C target-feature=-crt-static"

WORKDIR /build

COPY . .
RUN --mount=type=cache,target=/build/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --features mimalloc && \
    mkdir -p /build/out && \
    cp /build/target/release/hath-rs /build/out/hath-rs

# Stage 2: Runtime
FROM alpine:latest

RUN apk add --no-cache \
    tini ca-certificates openssl

COPY --from=builder /build/out/hath-rs /usr/local/bin/hath-rs
COPY docker-entrypoint.sh /docker-entrypoint.sh

VOLUME ["/hath/cache", "/hath/data", "/hath/download", "/hath/log", "/hath/tmp"]
WORKDIR /hath

ENTRYPOINT ["tini", "--", "/bin/sh", "/docker-entrypoint.sh"]
