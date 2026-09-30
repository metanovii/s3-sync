# s3-sync specification

## Purpose

`s3-sync` keeps one-way 1:1 copies of S3-compatible buckets (or prefixes)
between different providers. One process serves many source/target pairs.

The first use case is moving a CDN origin from DigitalOcean Spaces to
Selectel S3: the target must hold the same objects, with the same metadata,
so that the CDN can be switched over to it.

## Non-goals

- Provider-side replication and bucket event notifications are not used.
  Spaces has no bucket notifications, and the tool must work with any
  S3-compatible provider.
- No two-way synchronization.
- No storage class handling: in Selectel and Spaces the class is a bucket
  property chosen at creation; in AWS it is managed by bucket lifecycle rules.
- Object tags and `x-amz-website-redirect-location` are not copied.

## Terms

- **Provider**: one S3 endpoint with its credentials and rate limits
  (`providers.<name>` in the configuration).
- **Location**: `<provider>/<bucket>[/<prefix>]`.
- **Sync**: one source location and one target location. A sync is
  identified by the pair `<source> -> <target>`; changing either side makes a
  new sync with empty state.
- **Pass**: one run of the algorithm below for one sync.
- **State**: the SQLite database with what has been copied.

## Configuration

See [`config.example.yaml`](../config.example.yaml). Rules:

- Unknown keys are errors; all errors are reported at once, with the lines
  of the file.
- `${NAME}` in any string value is replaced with the environment variable
  `NAME`. An unset variable is an error; an empty one is allowed. Values that
  came from the environment are never quoted in error messages.
- Sizes are written with binary units: `B`, `KiB`, `MiB`, `GiB`, `TiB`.
  Ambiguous units (`M`, `MB`) are rejected.
- Durations use Go syntax: `90s`, `10m`, `2h`.
- `defaults` holds sync settings; each `sync` entry may override any of them.
  Nested blocks (`delete`) are merged field by field.

### Location and prefix

`<provider>/<bucket>[/<prefix>]`. The prefix is a directory: `images` means
keys starting with `images/`, not `images2/`. Leading and trailing slashes of
the prefix are ignored.

A key is mapped from source to target by replacing the source prefix with the
target prefix: `<source prefix><rest>` becomes `<target prefix><rest>`. With
`do/uploads/images` -> `selectel/static/img`, the key `images/a.jpg` of bucket
`uploads` becomes `img/a.jpg` in bucket `static`. A source key
equal to the source prefix (a "directory" object such as `images/`) would map
to an empty key and is skipped.

Bucket names are checked loosely (3 to 255 letters, digits, `.`, `_`, `-`,
no `..`), because providers differ and legacy AWS buckets allow upper case.

### Validation

`s3-sync validate` and every start reject a configuration where:

- a sync refers to an unknown provider;
- two syncs have the same pair;
- two targets overlap: same bucket and one prefix contains the other, on the
  same endpoint (host compared case-insensitively, default port ignored) or on
  any two AWS endpoints, since AWS bucket names are global;
- a target overlaps any source (this includes cycles between syncs);
- `workers` is less than 1;
- more than one provider takes credentials from the AWS SDK default chain
  (no `access_key`/`secret_key`) and at least one of them is not AWS: they
  would silently share the same keys;
- only one of `access_key` and `secret_key` is set;
- a value is out of range (see `config.example.yaml`).

`validate --no-env` treats unset environment variables as `"0"`, so that CI
can check the configuration without its secrets.

## State

SQLite database at `state.path`, on a local disk (not NFS). `run` holds an
exclusive lock on `<state.path>.lock`; a second `run` on the same database
refuses to start. `run --dry-run` works on a copy (`VACUUM INTO` a temporary
file next to the database, on the same volume) and does not need the lock.
Copies older than 24 hours, left by crashed dry runs, are removed.

```sql
CREATE TABLE syncs (
  id              TEXT PRIMARY KEY,   -- "<source> -> <target>"
  source_endpoint TEXT NOT NULL,      -- normalized endpoint of the source provider
  target_endpoint TEXT NOT NULL,      -- normalized endpoint of the target provider
  pass            INTEGER NOT NULL,   -- number of the last started pass
  last_success    INTEGER,            -- unix time of the last successful pass
  last_full_check INTEGER             -- unix time of the last target listing
);

CREATE TABLE objects (
  sync_id        TEXT NOT NULL,
  key            TEXT NOT NULL,       -- source key
  size           INTEGER NOT NULL,    -- from the source listing
  etag           TEXT NOT NULL,       -- from the source listing
  last_modified  INTEGER NOT NULL,    -- from the source listing, whole seconds
  acl            TEXT NOT NULL,       -- canned ACL set on the target copy, or ''
  copied_at      INTEGER NOT NULL,
  seen_pass      INTEGER NOT NULL,    -- last pass that saw the key in the source
  missing_since  INTEGER,             -- unix time the key vanished from the source
  PRIMARY KEY (sync_id, key)
) WITHOUT ROWID;

CREATE TABLE target_keys (            -- target listing of the current pass
  sync_id       TEXT NOT NULL,
  key           TEXT NOT NULL,        -- mapped to the source key
  size          INTEGER NOT NULL,
  last_modified INTEGER NOT NULL,
  PRIMARY KEY (sync_id, key)
) WITHOUT ROWID;
```

Records always hold the values of the source **listing**, not of the
`GetObject` response: the next listing is compared with them. LastModified is
kept in whole seconds, as listings have milliseconds and headers do not. If an
object changed between listing and copying on a provider that ignores
`If-Match`, the record is stale and the next pass copies the object again, so
the copy converges.

Losing the database is not data loss: the next pass rebuilds it from the
target (see "First pass").

The sync id is built from provider names. If the endpoint of a provider
changes while its name stays, the stored `source_endpoint` or
`target_endpoint` no longer matches. Endpoints are stored normalized
(lower-case host, default port removed), so writing `:443` does not count
as a change. On a mismatch the rows of that sync are dropped and the next
pass is a first pass.

## Pass

1. Increment `syncs.pass`.
2. If no pass of the sync has listed the target to the end yet (**first
   pass**, `last_full_check` is empty) or the full check is due, list the
   target into `target_keys`. An interrupted first pass stays a first pass.
3. If the full check is due, compare the target with state (see "Full
   check").
4. List the source with `ListObjectsV2`, page by page, with
   `EncodingType=url` (keys with control characters would break the XML
   otherwise). A page that says it is truncated but has no new continuation
   token, or does not say whether it is truncated, fails the pass: treating
   it as the end would make the remaining keys look deleted. For every page, in one
   transaction, look the keys up in state and set `seen_pass`, clearing
   `missing_since`. A key is **changed** when it is absent from state or its
   size, ETag or LastModified differ. The algorithm does not depend on the
   listing order. Changed keys go to the worker pool; when the pool is full,
   the listing waits.
5. Only if the listing reached the last page without errors: keys of this
   sync with `seen_pass` lower than the current pass get `missing_since`,
   then "Deletion" runs.
6. With `acl: copy` and a full check: ACL are re-read (see "ACL").
7. Set `last_success` (and `last_full_check` after a target listing).

A pass fails when listing or state access fails. A pass stopped by
`SIGINT`/`SIGTERM` is not counted as failed. Copies cut by the pass timeout
or a stop are counted as interrupted, not failed, so a long first copy made
of several timed-out passes does not raise `s3sync_last_pass_failed_objects`. Failed copies of single
objects are logged and counted; they have no up-to-date record, so the next
pass tries them again. A pass that runs longer than `timeout` is cancelled and
counted as failed. The next pass starts `interval` after the previous one
ends; passes of one sync never overlap.

### First pass

During the source listing of a first pass, a changed key whose mapped target
key is in `target_keys` with the same size and a LastModified not earlier than
the source object's is recorded as copied and not transferred (**adopted**).
A target copy older than the source object is copied again: the source may
have been replaced by an object of the same size. With `acl` other than `skip` its ACL is still set
on the target. This avoids copying terabytes again after the database is lost.

### Copy

- `GetObject` is sent with `If-Match: <ETag from the listing>`.
  `412 Precondition Failed` means the object changed after listing: it is
  skipped and the next pass picks it up.
- The body is streamed from the source to the target, never held in memory.
  Streamed bodies are signed with `UNSIGNED-PAYLOAD` (the AWS SDK does this by
  itself only over HTTPS). `Accept-Encoding: identity` keeps compressed
  objects byte for byte.
- Objects below 64 MiB: one `PutObject` with `Content-Length`. Larger: a
  multipart upload with 64 MiB parts, more when the object would need over
  10000 parts. Every part is a ranged `GetObject` with `If-Match`, so a part
  is retried alone and all parts come from one version. Any failure or
  cancellation aborts the upload.
- An object or part is tried 3 times, with 1 and 2 seconds between tries.
  HTTP exchanges time out after 1 minute without response headers or 2
  minutes without data.
- A multipart copy that does not fit into the pass `timeout` starts over on
  the next pass; this is logged as a warning.
- Objects that keep answering `412` on 3 passes in a row are summed up in one
  warning per pass (keys in the debug log): the provider may compare ETags
  differently.
- A listed object that answers `404` is logged with a hint that the provider
  may encode keys in listings differently.
- All metadata is copied: `Content-Type`, `Cache-Control`,
  `Content-Encoding`, `Content-Disposition`, `Content-Language`, `Expires`,
  `x-amz-meta-*`.
- The record is written only after the target confirmed the upload.

### ACL

`acl` of a sync:

- `skip`: ACL are neither read nor written.
- `copy`: `GetObjectAcl` on the source; the grants are recognised as a
  canned ACL and that canned ACL is applied to the target copy. Owner ids
  differ between providers, so the owner's grant is matched by being the
  owner's, not by id:

  | Grants besides `FULL_CONTROL` for the owner | Canned ACL           |
  |---------------------------------------------|----------------------|
  | none                                        | `private`            |
  | `READ` for group `AllUsers`                 | `public-read`        |
  | `READ` and `WRITE` for group `AllUsers`     | `public-read-write`  |
  | `READ` for group `AuthenticatedUsers`       | `authenticated-read` |

  Groups are identified by their URI
  (`http://acs.amazonaws.com/groups/global/AllUsers`, `.../AuthenticatedUsers`,
  with `http` or `https`). An object with any other grants is copied without
  ACL and counted in `s3sync_acl_not_copied_total`; each pass logs one summary
line, the keys are in the debug log.
- a canned ACL (any of `private`, `public-read`, `public-read-write`,
  `authenticated-read`, `aws-exec-read`, `bucket-owner-read`,
  `bucket-owner-full-control`): applied to every target copy without reading
  the source.

Changing an ACL does not change ETag or LastModified, so the regular pass does
not see it; with `acl: copy` the full check reads every source ACL again and
applies the ones that changed.

### Deletion

Target copies whose source key has been missing for at least
`delete.delay` are **due**. Deletions are computed only after a complete
listing.

If the number of due keys exceeds `delete.max_count`, or, when the sync has at
least `delete.min_count` objects, exceeds `delete.max_fraction` of them,
nothing is deleted in that pass: the number is exported as
`s3sync_deletions_held` and logged as a warning. This protects the target
when the source listing is wrong (changed keys, wrong prefix, a provider
failure). After checking the source, raise the limit in the configuration (a
zero limit is no limit) and restart; the next pass deletes them. A key that
reappears in the source is no longer missing and is not deleted.

Otherwise the due target copies are deleted through the worker pool and their
records removed. With `delete.enabled: false` missing keys are only recorded.

### Full check

Every `full_check` (0 disables it) the target is listed and compared with
state:

- a record whose target copy is absent or has another size is dropped, so the
  same pass copies the object again (`s3sync_full_check_recopied_total`);
- target objects unknown to state are counted in
  `s3sync_target_unexpected_objects` and logged, not deleted;
- keys under `<target prefix>.s3-sync-probe/` are ignored;
- with `acl: copy`, source ACL are read again and changed ones applied.

Target ETag is never compared with source ETag: for multipart uploads it
depends on the part layout and differs between providers.

## Start-up

For every sync, before the first pass (not in dry-run):

- probe objects left under `<target prefix>.s3-sync-probe/` by a crash are
  deleted;
- incomplete multipart uploads under the target prefix older than 24 hours
  are aborted (Selectel has no bucket lifecycle rules to do it). A provider
  that does not implement `ListMultipartUploads` only gets a warning;
- a probe object `<target prefix>.s3-sync-probe/<random> a+b%20c` is
  written and listed back; a target that returns the key changed stops the
  start, since keys with spaces, `+` or `%` would not be mirrored correctly;
- when `acl` is not `skip`, a probe object
  `<target prefix>.s3-sync-probe/<random>` is written with the ACL (`copy`
  probes with `public-read`), its ACL read back and the object deleted. With
  `acl: copy` one source object's ACL is read as well. A target that rejects
  or silently ignores the ACL stops the start, instead of every object being
  copied without it.

On `SIGINT`/`SIGTERM` passes are cancelled, in-flight multipart uploads
aborted, and the process exits after the running operations end.

## Rate limiting and concurrency

- `workers`: one pool for all copy and delete operations of the process.
- `rate_limit.requests_per_bucket`: requests per second of every kind to one
  bucket, shared by all syncs using that bucket. Retries made by the AWS SDK
  itself are not counted.
- `rate_limit.bandwidth_total`: bytes per second to and from the provider,
  both directions together.
- Limits belong to the endpoint, not to the provider name: two providers with
  the same endpoint (e.g. different keys) share them, and the smaller
  configured limit wins.
- `checksum`: `RequestChecksumCalculation` and `ResponseChecksumValidation`
  of the AWS SDK. With `when_supported` the SDK adds CRC32 checksums that
  some S3-compatible providers are reported to reject, so the default is
  `when_required`, except for AWS endpoints.

## CLI

Global flags: `--config` (`-c`, default `config.yaml`), `--debug`,
`--logFormat` (`console` or `json`, default `console`).

```
s3-sync --config config.yaml run [--dry-run]
s3-sync --config config.yaml validate [--no-env]
```

`run --dry-run` runs one pass of every sync on a copy of the state, logs what
would be copied, adopted and deleted, writes nothing to the targets and
exits. It works while `run` is active.

## Metrics and health

HTTP on `metrics.listen`: `/metrics`, `/healthz` (process alive), `/readyz`
(start-up finished). Metrics carry `source` and `target` labels:

| Metric | Type | Meaning |
|---|---|---|
| `s3sync_last_success_timestamp_seconds` | gauge | end of the last successful pass |
| `s3sync_pass_duration_seconds` | gauge | duration of the last pass |
| `s3sync_passes_total{result}` | counter | passes by `success` / `failure` |
| `s3sync_objects` | gauge | objects in state |
| `s3sync_copied_objects_total`, `s3sync_copied_bytes_total` | counter | copies |
| `s3sync_adopted_objects_total` | counter | found already copied on a first pass |
| `s3sync_skipped_objects_total` | counter | changed during copying (412) |
| `s3sync_deleted_objects_total` | counter | target copies deleted |
| `s3sync_deletions_pending` | gauge | missing, waiting for `delete.delay` |
| `s3sync_deletions_held` | gauge | due but blocked by the delete limits |
| `s3sync_errors_total{operation}` | counter | `copy`, `delete`, `get_acl`, `put_acl`, `state`, `pass` |
| `s3sync_last_pass_failed_objects` | gauge | objects that failed in the last pass (interruptions excluded) |
| `s3sync_acl_not_copied_total` | counter | copied without ACL: grants match no canned ACL |
| `s3sync_target_unexpected_objects` | gauge | target objects unknown to state |
| `s3sync_full_check_recopied_total` | counter | copied again by the full check |

## Deployment in Kubernetes

One replica, `strategy: Recreate`, a `ReadWriteOnce` PVC for the state.

## Provider notes

- DigitalOcean Spaces: 800 operations per second per new bucket; load above
  150 requests per second should be agreed with support; LIST may be limited
  harder under load
  ([limits](https://docs.digitalocean.com/products/spaces/details/limits/)).
  No bucket notifications, no inventory
  ([S3 compatibility](https://docs.digitalocean.com/products/spaces/reference/s3-compatibility/)).
- Selectel: object ACL can be read, not set; bucket ACL not supported; bucket
  policy supported; no bucket lifecycle
  ([S3 API](https://docs.selectel.ru/en/api/object-storage-s3/)).
  Storage class is fixed at bucket creation. Use `acl: skip` and a bucket
  policy for public access.
- MinIO (used in the end-to-end tests) does not keep object ACL; the start-up
  probe detects it.
- AWS SDK for Go v2 adds request checksums by default
  ([checksums](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/s3-checksums.html)).

## Not yet verified on the real providers

The end-to-end tests run against MinIO. On Spaces and Selectel still to
confirm:

- Spaces honours `If-Match` on `GetObject` (without it, see "State").
- Changing an ACL in Spaces leaves ETag and LastModified unchanged.
- Selectel accepts or rejects `PutObject` with `x-amz-acl` (the probe will
  tell).
- Spaces and Selectel accept or reject the CRC32 checksums of
  `when_supported`.
- What `GetObjectAcl` returns in Spaces for the owner and for public objects.
