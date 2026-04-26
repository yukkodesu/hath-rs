# Stage 1: Build
FROM rust:trixie AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl-dev pkg-config perl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
RUN --mount=type=cache,target=/build/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo fetch --locked

COPY src/ src/
RUN --mount=type=cache,target=/build/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release && \
    mkdir -p /build/out && \
    cp /build/target/release/hath-rs /build/out/hath-rs

# Stage 2: Runtime
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini ca-certificates openssl openssl-provider-legacy \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/out/hath-rs /usr/local/bin/hath-rs
COPY docker-entrypoint.sh /docker-entrypoint.sh

VOLUME ["/hath/cache", "/hath/data", "/hath/download", "/hath/log", "/hath/tmp"]
WORKDIR /hath

ENTRYPOINT ["tini", "--", "/bin/sh", "/docker-entrypoint.sh"]
