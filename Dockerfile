# Stage 1: Build
FROM rust:trixie AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl-dev pkg-config perl \
    && rm -rf /var/lib/apt/lists/*

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
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini ca-certificates openssl openssl-provider-legacy \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/out/hath-rs /usr/local/bin/hath-rs
COPY docker-entrypoint.sh /docker-entrypoint.sh

VOLUME ["/hath/cache", "/hath/data", "/hath/download", "/hath/log", "/hath/tmp"]
WORKDIR /hath

ENTRYPOINT ["tini", "--", "/bin/sh", "/docker-entrypoint.sh"]
