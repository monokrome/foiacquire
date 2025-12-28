# FOIAcquire - FOIA document acquisition and research system
FROM debian:bookworm-slim

ARG TARGETARCH
ARG WITH_TESSERACT="false"

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libsqlite3-0 \
        libpq5 \
        gosu \
    && if [ "$WITH_TESSERACT" = "true" ]; then \
         apt-get install -y --no-install-recommends tesseract-ocr tesseract-ocr-eng; \
       fi \
    && rm -rf /var/lib/apt/lists/*

ENV TARGET_PATH=/opt/foiacquire
ENV USER_ID=1000
ENV GROUP_ID=1000

# Create default non-root user (can be overridden with USER_ID env var)
RUN useradd -m -u 1000 foiacquire \
    && mkdir -p /opt/foiacquire \
    && chown foiacquire:foiacquire /opt/foiacquire

WORKDIR /opt/foiacquire
VOLUME /opt/foiacquire

# Copy pre-built binary for the target architecture
COPY --chmod=755 dist/${TARGETARCH}/foiacquire /usr/local/bin/foiacquire
COPY --chmod=755 bin/foiacquire-entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--help"]
