# FOIAcquire - FOIA document acquisition and research system

# Stage 1: Build the Rust binary
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

# Stage 2: Runtime image
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
