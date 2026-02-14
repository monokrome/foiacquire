# FOIAcquire Builder - Compiles Rust binary only
# Used by CI to pre-build binaries that runtime images can copy
#
# Uses cargo-chef for dependency caching: when only source changes,
# the dependency layer is reused and only workspace crates are rebuilt.

FROM rust:alpine AS chef
RUN apk add --no-cache musl-dev && cargo install cargo-chef
WORKDIR /build

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG FEATURES="browser,postgres,redis-backend,gis"

COPY --from=planner /build/recipe.json recipe.json

# Build dependencies only — cached until Cargo.toml/Cargo.lock change
RUN if [ -n "$FEATURES" ]; then \
      cargo chef cook --release --features "$FEATURES" --recipe-path recipe.json; \
    else \
      cargo chef cook --release --recipe-path recipe.json; \
    fi

# Build the actual binary
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN if [ -n "$FEATURES" ]; then \
      cargo build --release --features "$FEATURES"; \
    else \
      cargo build --release; \
    fi \
    && strip target/release/foia

CMD ["true"]
