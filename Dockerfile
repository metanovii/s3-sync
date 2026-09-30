# Image built by GoReleaser (dockers_v2): the binary for each platform is in
# $TARGETPLATFORM/. For a local build from source use Dockerfile.dev.
FROM alpine:3@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11
ARG TARGETPLATFORM
RUN apk upgrade --no-cache && mkdir -p /var/lib/s3-sync
EXPOSE 9090
COPY $TARGETPLATFORM/s3-sync /usr/bin/s3-sync
ENTRYPOINT ["/usr/bin/s3-sync"]
CMD ["--config", "/etc/s3-sync/config.yaml", "run"]
