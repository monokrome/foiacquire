# FOIAcquire - FOIA document acquisition and research system
FROM alpine:latest

ARG TARGETARCH
ARG WITH_TESSERACT="false"

ARG WITH_POSTGRES="true"

RUN apk add --no-cache sqlite-libs ca-certificates \
    && if [ "$WITH_POSTGRES" = "true" ]; then \
         apk add --no-cache libpq; \
       fi \
    && if [ "$WITH_TESSERACT" = "true" ]; then \
         apk add --no-cache tesseract-ocr tesseract-ocr-data-eng; \
       fi

ENV TARGET_PATH=/opt/foiacquire

# Create non-root user for running the application
RUN adduser -D -u 1000 foiacquire \
    && mkdir -p /opt/foiacquire \
    && chown foiacquire:foiacquire /opt/foiacquire

USER foiacquire

WORKDIR /opt/foiacquire
VOLUME /opt/foiacquire

# Copy pre-built binary for the target architecture
COPY --chmod=755 dist/${TARGETARCH}/foiacquire /usr/local/bin/foiacquire
COPY --chmod=755 bin/foiacquire-entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--help"]
