# FOIAcquire - FOIA document acquisition and research system
FROM alpine:latest

ARG TARGETARCH
ARG WITH_TESSERACT="false"

RUN apk add --no-cache ca-certificates su-exec python3 py3-pip poppler-utils \
    && pip3 install --no-cache-dir --break-system-packages yt-dlp \
    && if [ "$WITH_TESSERACT" = "true" ]; then \
         apk add --no-cache tesseract-ocr tesseract-ocr-data-eng; \
       fi

ENV TARGET_PATH=/opt/foiacquire
ENV USER_ID=1000
ENV GROUP_ID=1000

# Create default non-root user (can be overridden with USER_ID env var)
RUN adduser -D -u 1000 foiacquire \
    && mkdir -p /opt/foiacquire \
    && chown foiacquire:foiacquire /opt/foiacquire

WORKDIR /opt/foiacquire
VOLUME /opt/foiacquire

# Copy pre-built binary for the target architecture
COPY dist/${TARGETARCH}/foiacquire /usr/local/bin/foiacquire
COPY bin/foiacquire-entrypoint.sh /entrypoint.sh
RUN chmod 755 /usr/local/bin/foiacquire /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--help"]
