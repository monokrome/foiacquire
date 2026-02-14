# FOIAcquire - FOIA document acquisition and research system
#
# Uses cargo-chef for dependency caching: when only source changes,
# the dependency layer is reused and only workspace crates are rebuilt.

# Stage 1: Install cargo-chef
FROM rust:alpine AS chef
RUN apk add --no-cache musl-dev && cargo install cargo-chef
WORKDIR /build

# Stage 2: Generate dependency recipe
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3: Build
FROM chef AS builder
ARG FEATURES="browser,postgres,redis-backend,gis"

COPY --from=planner /build/recipe.json recipe.json

RUN if [ -n "$FEATURES" ]; then \
      cargo chef cook --release --features "$FEATURES" --recipe-path recipe.json; \
    else \
      cargo chef cook --release --recipe-path recipe.json; \
    fi

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN if [ -n "$FEATURES" ]; then \
      cargo build --release --features "$FEATURES"; \
    else \
      cargo build --release; \
    fi \
    && strip target/release/foia

# Stage 4: Runtime image
FROM alpine:3.21

ARG WITH_TESSERACT="false"
ARG WITH_TOR="true"

RUN apk add --no-cache ca-certificates su-exec python3 py3-pip poppler-utils \
    && pip3 install --no-cache-dir --break-system-packages yt-dlp \
    && if [ "$WITH_TESSERACT" = "true" ]; then \
         apk add --no-cache tesseract-ocr tesseract-ocr-data-eng; \
       fi \
    && if [ "$WITH_TOR" = "true" ]; then \
         apk add --no-cache tor snowflake; \
       fi

ENV DATA_DIR=/opt/foiacquire
ENV USER_ID=1000
ENV GROUP_ID=1000

RUN adduser -D -u 1000 foiacquire \
    && mkdir -p /opt/foiacquire \
    && chown foiacquire:foiacquire /opt/foiacquire

WORKDIR /opt/foiacquire
VOLUME /opt/foiacquire

COPY --from=builder /build/target/release/foia /usr/local/bin/foiacquire
COPY bin/foiacquire-entrypoint.sh /entrypoint.sh
RUN chmod 755 /usr/local/bin/foiacquire /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--help"]
