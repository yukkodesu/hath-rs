# Stage 1: Build
FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev openssl-dev pkgconfig libc-dev perl openssl-libs-static

WORKDIR /build
COPY . .

RUN cargo build --release

# Stage 2: Runtime
FROM alpine:latest

RUN apk add --no-cache tini ca-certificates libgcc

COPY --from=builder /build/target/release/hath-rs /usr/local/bin/hath-rs
COPY docker-entrypoint.sh /docker-entrypoint.sh

VOLUME ["/hath/cache", "/hath/data", "/hath/download", "/hath/log", "/hath/tmp"]
WORKDIR /hath

ENTRYPOINT ["/sbin/tini", "--", "/bin/sh", "/docker-entrypoint.sh"]
