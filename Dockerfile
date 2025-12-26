# FOIAcquire - FOIA document acquisition and research system
FROM alpine:latest

ARG TARGETARCH
ARG WITH_TESSERACT="false"

RUN apk add --no-cache sqlite-libs ca-certificates su-exec shadow \
    && if [ "$WITH_TESSERACT" = "true" ]; then \
         apk add --no-cache tesseract-ocr tesseract-ocr-data-eng; \
       fi

ENV TARGET_PATH=/opt/foiacquire
ENV USER_ID=1000
ENV GROUP_ID=1000

RUN adduser -D foiacquire \
    && mkdir -p /opt/foiacquire \
    && chown foiacquire:foiacquire /opt/foiacquire

WORKDIR /opt/foiacquire
VOLUME /opt/foiacquire

# Copy pre-built binary for the target architecture
COPY --chmod=755 dist/${TARGETARCH}/foiacquire /usr/local/bin/foiacquire
COPY --chmod=755 bin/foiacquire-entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--help"]
