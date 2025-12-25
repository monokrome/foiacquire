# FOIAcquire - FOIA document acquisition and research system
FROM rust:alpine AS builder

ARG FEATURES="browser"

# Add edge/testing for onnxruntime
RUN echo "@testing https://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories \
    && echo "@community https://dl-cdn.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories \
    && apk add --no-cache musl-dev sqlite-dev openssl-dev openssl-libs-static pkgconfig cmake make g++ \
       onnxruntime-dev@testing

ENV ORT_LIB_LOCATION=/usr/lib

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --features "$FEATURES"

# Runtime image
FROM alpine:latest

ARG WITH_TESSERACT="false"
ARG WITH_ONNX="false"

RUN echo "@testing https://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories \
    && echo "@community https://dl-cdn.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories \
    && apk add --no-cache sqlite-libs ca-certificates \
    && if [ "$WITH_TESSERACT" = "true" ]; then \
         apk add --no-cache tesseract-ocr tesseract-ocr-data-eng; \
       fi \
    && if [ "$WITH_ONNX" = "true" ]; then \
         apk add --no-cache onnxruntime@testing; \
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
