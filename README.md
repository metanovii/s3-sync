# s3-sync

One-way 1:1 mirror between S3-compatible buckets of different providers
(DigitalOcean Spaces, Selectel, AWS, MinIO and others). One process serves
many source/target pairs.

It periodically lists the source, compares the listing with a local SQLite
database of what has already been copied, and copies only new and changed
objects, with all their metadata. Objects deleted in the source are deleted in
the target after a delay, with a guard against mass deletion. Only the plain
S3 API is used: no provider-side replication or bucket notifications.

The full design is in [`docs/spec.md`](docs/spec.md).

## Install

Binaries for Linux, macOS and Windows are on the
[releases page](https://github.com/metanovii/s3-sync/releases); the checksum
file is signed with cosign.

Container image for `linux/amd64` and `linux/arm64`:

```bash
docker pull ghcr.io/metanovii/s3-sync:latest
```

From source:

```bash
go install github.com/metanovii/s3-sync@latest
```

## Configuration

Full example with every field and its default:
[`config.example.yaml`](config.example.yaml). A minimal one:

```yaml
providers:
  do:
    endpoint: https://fra1.digitaloceanspaces.com
    region: fra1
    access_key: ${DO_ACCESS_KEY}
    secret_key: ${DO_SECRET_KEY}
  selectel:
    endpoint: https://s3.ru-1.storage.selcloud.ru
    region: ru-1
    path_style: true
    access_key: ${SELECTEL_ACCESS_KEY}
    secret_key: ${SELECTEL_SECRET_KEY}

sync:
  - source: do/static
    target: selectel/static
  - source: do/uploads/images          # a prefix inside the bucket
    target: selectel/images
```

- `providers` describes S3 endpoints and their credentials. Omit both keys to
  use the AWS SDK default chain (environment, profile, instance role).
- `sync` lists the pairs to mirror, as `<provider>/<bucket>[/<prefix>]`. The
  prefix is a directory: `images` matches `images/...`, not `images2/...`.
  The source prefix is replaced by the target prefix:
  `do/uploads/images` → `selectel/images` copies `images/a.jpg` of bucket
  `uploads` to `a.jpg` of bucket `images`.
- `defaults` holds settings every sync inherits (interval, ACL handling,
  deletion guard); any sync can override them.
- `${NAME}` is replaced with the environment variable `NAME`; an unset
  variable is an error. Keep secrets in the environment, not in the file.
- Sizes use binary units (`100MiB`), durations Go syntax (`10m`, `2h`).

### ACL

`acl: skip` (default) leaves ACL alone. `acl: copy` reads each object's ACL
in the source and sets the same canned ACL on the target. A canned ACL such as
`acl: public-read` is set on every copy. Selectel does not support object
ACL: keep `skip` there and use a bucket policy for public access. On start,
s3-sync writes a probe object to check that the target really keeps the ACL,
and refuses to start otherwise.

### Deletions

A key that vanished from the source is deleted in the target after
`delete.delay` (1 hour by default). If more keys are due at once than
`delete.max_count` (10000) or `delete.max_fraction` (1% of the objects, for
syncs with at least `delete.min_count` = 100 objects), nothing is deleted and
`s3sync_deletions_held` shows how many are waiting. This protects the target
when the source listing is wrong. After checking the source, raise the limit
for that sync (0 is no limit) and restart. `delete.enabled: false` never
deletes.

## Usage

```bash
# Check the configuration; secrets must be in the environment.
s3-sync --config config.yaml validate

# Check it without secrets, e.g. in CI: unset variables become "0".
s3-sync --config config.yaml validate --no-env

# See what one pass would copy and delete; changes nothing, works while run is active.
s3-sync --config config.yaml run --dry-run

# Mirror continuously.
s3-sync --config config.yaml run
```

Global flags: `--config` (`-c`, default `config.yaml`), `--debug`,
`--logFormat` (`console` or `json`, default `console`).

With Docker:

```bash
docker run --rm \
  -e DO_ACCESS_KEY -e DO_SECRET_KEY -e SELECTEL_ACCESS_KEY -e SELECTEL_SECRET_KEY \
  -v "$PWD/config.yaml:/etc/s3-sync/config.yaml:ro" \
  -v s3-sync-state:/var/lib/s3-sync \
  -p 9090:9090 \
  ghcr.io/metanovii/s3-sync:latest
```

The image runs `--config /etc/s3-sync/config.yaml run` by default and keeps
the state in `/var/lib/s3-sync`.

## Monitoring

`metrics.listen` (`:9090` by default) serves `/metrics`, `/healthz` and
`/readyz`. The main metrics, all with `source` and `target` labels:

- `s3sync_last_success_timestamp_seconds` — end of the last successful pass;
- `s3sync_copied_objects_total`, `s3sync_copied_bytes_total`;
- `s3sync_deletions_held` — deletions blocked by the guard;
- `s3sync_errors_total{operation}` — failed copies, deletions, ACL calls.

The full list is in [`docs/spec.md`](docs/spec.md#metrics-and-health),
alerting rules in [`_examples/alerts.rules.yml`](_examples/alerts.rules.yml).

## Examples

- [`_examples/docker-compose.yml`](_examples/docker-compose.yml): one
  container with a volume for the state.
- [`_examples/kubernetes.yaml`](_examples/kubernetes.yaml): Secret,
  ConfigMap, PersistentVolumeClaim and Deployment.
- [`_examples/alerts.rules.yml`](_examples/alerts.rules.yml): Prometheus
  alerting rules.
- [`_examples/github-actions-validate.yml`](_examples/github-actions-validate.yml):
  check a configuration kept in another repository.

## Running in production

- Run exactly one `run` per state database: one replica, Deployment strategy
  `Recreate`. A second `run` on the same database refuses to start.
- Keep the database on a local disk or a `ReadWriteOnce` volume, not on NFS.
  Losing it is not data loss: the next pass lists the target and records
  objects that are already there instead of copying them again.
- Set `rate_limit.requests_per_bucket` below the provider limit: the source
  bucket usually also serves production traffic. DigitalOcean asks to agree
  load above 150 requests per second with support.
- Start with `run --dry-run` to see what the first pass will do.

## Development

```bash
make test                                          # unit tests with -race and coverage
go test -tags e2e ./internal/syncer                # end-to-end tests, start MinIO with docker
go run . -c config.example.yaml validate --no-env  # try the example
docker build -f Dockerfile.dev -t s3-sync:dev .    # local image
```

A tag `v*` builds the release with GoReleaser: binaries, signed checksums
and the multi-platform image in `ghcr.io/metanovii/s3-sync`.
