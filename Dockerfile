# FOIAcquire - FOIA document acquisition and research system
FROM rust:alpine AS builder

ARG FEATURES="browser"

RUN apk add --no-cache musl-dev sqlite-dev openssl-dev openssl-libs-static pkgconfig

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --features "$FEATURES"

# Runtime image
FROM alpine:latest

ARG WITH_TESSERACT="false"

RUN apk add --no-cache sqlite-libs ca-certificates \
    && if [ "$WITH_TESSERACT" = "true" ]; then \
         apk add --no-cache tesseract-ocr tesseract-ocr-data-eng; \
       fi

ENV TARGET_PATH=/opt/foiacquire

RUN adduser -D foiacquire \
    && mkdir -p /opt/foiacquire \
    && chown foiacquire:foiacquire /opt/foiacquire

USER foiacquire

WORKDIR /opt/foiacquire
VOLUME /opt/foiacquire

COPY --from=builder /build/target/release/foiacquire /usr/local/bin/foiacquire
COPY --chmod=755 bin/foiacquire-entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["status"]
