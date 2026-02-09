# FOIAcquire Builder - Compiles Rust binary only
# Used by CI to pre-build binaries that runtime images can copy

FROM rust:alpine AS builder

ARG FEATURES="browser,postgres,redis-backend"

RUN apk add --no-cache musl-dev

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY templates ./templates

RUN if [ -n "$FEATURES" ]; then \
      cargo build --release --features "$FEATURES"; \
    else \
      cargo build --release; \
    fi \
    && strip target/release/foia

# Dummy command for container creation
CMD ["true"]
