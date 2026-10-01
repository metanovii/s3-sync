# Image built by GoReleaser (dockers_v2): the binary for each platform is in
# $TARGETPLATFORM/. For a local build from source use Dockerfile.dev.
FROM alpine:3@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6
ARG TARGETPLATFORM
RUN apk upgrade --no-cache && mkdir -p /var/lib/s3-sync
EXPOSE 9090
COPY $TARGETPLATFORM/s3-sync /usr/bin/s3-sync
ENTRYPOINT ["/usr/bin/s3-sync"]
CMD ["--config", "/etc/s3-sync/config.yaml", "run"]
