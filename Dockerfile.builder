# FOIAcquire Builder - Compiles Rust binary only
# Used by CI to pre-build binaries that runtime images can copy

FROM rust:alpine AS builder

ARG FEATURES="browser,postgres"

RUN apk add --no-cache musl-dev

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
COPY templates ./templates
COPY build.rs ./

RUN if [ -n "$FEATURES" ]; then \
      cargo build --release --features "$FEATURES"; \
    else \
      cargo build --release; \
    fi \
    && strip target/release/foia

# Output stage - just the binary for easy extraction
FROM scratch AS export
COPY --from=builder /build/target/release/foia /foia
