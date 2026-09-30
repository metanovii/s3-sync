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

- Unknown keys are errors.
- `${NAME}` in any string value is replaced with the environment variable
  `NAME`. An unset variable is an error; an empty one is allowed.
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
target prefix: with `do/uploads/images` -> `selectel/images`, source key
`images/a.jpg` becomes target key `a.jpg`. A source key equal to the source
prefix (a "directory" object such as `images/`) would map to an empty key and
is skipped.

Bucket names are checked loosely (3 to 255 letters, digits, `.`, `_`, `-`),
because providers differ and legacy AWS buckets allow upper case.

### Validation

`s3-sync validate` and every start reject a configuration where:

- a sync refers to an unknown provider;
- two syncs have the same pair;
- two targets overlap: same bucket and one prefix contains the other, on the
  same endpoint (host compared case-insensitively, default port ignored) or on
  any two AWS endpoints, since AWS bucket names are global;
- `workers` is less than 1;
- a target overlaps any source (this includes cycles between syncs);
- more than one provider takes credentials from the AWS SDK default chain
  (no `access_key`/`secret_key`) and at least one of them is not AWS: they
  would silently share the same keys;
- only one of `access_key` and `secret_key` is set;
- a value is out of range (see `config.example.yaml`).

## State

SQLite database at `state.path`, on a local disk (not NFS). The process
holds an exclusive lock file next to it; a second `s3-sync run` refuses to
start. Other commands (`confirm-delete`) use the database concurrently with
`busy_timeout`.

Tables:

```sql
CREATE TABLE syncs (
  id              TEXT PRIMARY KEY,   -- "<source> -> <target>"
  source_endpoint TEXT NOT NULL,      -- normalized endpoint of the source provider
  target_endpoint TEXT NOT NULL,      -- normalized endpoint of the target provider
  pass            INTEGER NOT NULL,   -- number of the last started pass
  last_success    INTEGER,            -- unix time of the last successful pass
  last_full_check INTEGER,            -- unix time of the last full check
  confirmed_pass  INTEGER             -- held keys up to this pass may be deleted
);

CREATE TABLE objects (
  sync_id        TEXT NOT NULL,
  key            TEXT NOT NULL,       -- source key
  size           INTEGER NOT NULL,
  etag           TEXT NOT NULL,       -- source ETag
  last_modified  INTEGER NOT NULL,    -- source LastModified, unix time
  acl            TEXT,                -- canned ACL applied to the target copy
  copied_at      INTEGER NOT NULL,
  seen_pass      INTEGER NOT NULL,    -- last pass that saw the key in source
  missing_since  INTEGER,             -- unix time the key vanished from source
  held_pass      INTEGER,             -- pass that held its deletion
  PRIMARY KEY (sync_id, key)
) WITHOUT ROWID;
```

Losing the database is not data loss: the next pass rebuilds it from the
target (see "First pass").

The sync id is built from provider names. If the endpoint of a provider
changes while its name stays, the stored `source_endpoint` or
`target_endpoint` no longer matches. Endpoints are stored normalized
(lower-case host, default port removed), so writing `:443` does not count
as a change. On a mismatch the rows of that sync are dropped and
the next pass is a first pass.

## Pass

1. Increment `syncs.pass`.
2. List the source with `ListObjectsV2`, page by page. For every page, look
   the keys up in state and set `seen_pass`. A key is **changed** when it is
   absent from state or its size, ETag or LastModified differ. The algorithm
   does not depend on the listing order. Each page is handled in one
   transaction.
3. Copy every changed object (see "Copy").
4. Only if the listing reached the last page without errors: keys of this
   sync with `seen_pass` lower than the current pass are **missing**. Set
   `missing_since` for newly missing keys, clear it for keys that are back.
   Then run "Deletion".
5. On success, set `last_success`.

A pass that runs longer than `timeout` is cancelled and counted as failed.
The next pass starts `interval` after the previous one ends; passes of one
sync never overlap.

### First pass

When the state has no rows for a sync, the target is listed first into a
temporary SQLite table (key, size). During the source listing, a changed key
whose mapped target key is in that table with the same size is recorded as
copied, with ETag and LastModified taken from the source listing, and is not
transferred. With `acl: copy` the ACL is still read and applied. This avoids
copying terabytes again after the database is lost.

### Copy

- `GetObject` is sent with `If-Match: <ETag from the listing>`. `412
  Precondition Failed` means the object changed after listing: skip it, the
  next pass picks it up.
- The object is streamed to the target, never buffered whole in memory.
  Objects of 64 MiB or more are uploaded in parts of 64 MiB; the part size
  grows when the object would need more than 10000 parts.
- All metadata is copied: `Content-Type`, `Cache-Control`,
  `Content-Encoding`, `Content-Disposition`, `Content-Language`, `Expires`,
  `x-amz-meta-*`.
- The state row is written only after the target confirmed the upload.

### ACL

`acl` of a sync:

- `skip`: ACL are neither read nor written.
- `copy`: `GetObjectAcl` on the source; the grants are recognised as a
  canned ACL and that canned ACL is applied to the target copy. Owner ids
  differ between providers, so grants to the owner are matched by being the
  owner's, not by id:

  | Grants besides `FULL_CONTROL` for the owner                   | Canned ACL           |
  |---------------------------------------------------------------|----------------------|
  | none                                                          | `private`            |
  | `READ` for group `AllUsers`                                   | `public-read`        |
  | `READ` and `WRITE` for group `AllUsers`                       | `public-read-write`  |
  | `READ` for group `AuthenticatedUsers`                         | `authenticated-read` |

  Groups are identified by their URI
  (`http://acs.amazonaws.com/groups/global/AllUsers`,
  `.../AuthenticatedUsers`). An object with any other grants is copied
  without ACL and counted in `s3sync_acl_not_copied_objects`.
- a canned ACL (any of `private`, `public-read`, `public-read-write`,
  `authenticated-read`, `aws-exec-read`, `bucket-owner-read`,
  `bucket-owner-full-control`): applied to every target copy without reading
  the source.

Changing an ACL does not change ETag or LastModified, so the regular pass does
not see it; the full check does (with `acl: copy`).

### Deletion

Target objects whose source key has been missing for at least `delete.delay`
are **due**. Deletions are computed only after a complete listing (step 4).

Every pass handles two groups separately:

1. **Newly due keys** (due, not held). If their number exceeds
   `delete.max_count`, or, when the sync has at least `delete.min_count`
   objects, exceeds `delete.max_fraction` of them, none of them is deleted:
   they get `held_pass` = the current pass. Otherwise they are deleted.
2. **Held keys** (`held_pass` set). They are deleted only if
   `held_pass <= syncs.confirmed_pass`. A held key that reappears in the
   source loses `missing_since` and `held_pass` and is not deleted.

Deleted keys lose their rows. Deletions run through the usual workers and
rate limits.

`s3-sync confirm-delete <source> <target> --count N` deletes nothing itself.
It checks that `N` equals the number of held keys and sets
`syncs.confirmed_pass` to the largest `held_pass`; otherwise it fails and
prints the current count. Keys held by later passes are not covered: they
need their own confirmation. Keys that reappeared are no longer held, so the
confirmation only ever deletes a subset of what the operator saw.

With `delete.enabled: false` missing keys are only recorded.

### Full check

Every `full_check` the target is listed and compared with state:

- size differs or object absent: copy again;
- object present in the target but not in state: reported in metrics and log,
  not deleted; keys under `<target prefix>.s3-sync-probe/` are ignored;
- with `acl: copy`: the source ACL is read again and applied if it changed.

Target ETag is never compared with source ETag: for multipart uploads it
depends on the part layout and differs between providers.

### Incomplete multipart uploads

On `SIGTERM` in-flight uploads are aborted with `AbortMultipartUpload`. On
start, `ListMultipartUploads` with the target prefix removes uploads older
than 24 hours; uploads outside the prefix belong to others and are left
alone. Selectel has no bucket lifecycle rules, so nothing else cleans them up.

## Startup probe

When a sync writes ACL (`acl` is not `skip`), the process writes an object
`<target prefix>.s3-sync-probe/<random>` with an ACL to the target (inside
the prefix, so it never lands in another sync's target), reads the ACL back and
deletes the object, whatever `delete.enabled` is. Probe objects left by a
crash are deleted on the next start. With `acl: copy` it also calls
`GetObjectAcl` on the source. A failed probe stops the start.

## Rate limiting and concurrency

- `workers`: one pool for all copy and delete operations of the process.
- `rate_limit.requests_per_bucket`: requests per second of every kind (LIST,
  HEAD, GET, PUT, DELETE, ACL) to one bucket of the provider, shared by all
  syncs using that bucket.
- `rate_limit.bandwidth_total`: bytes per second to and from the provider,
  both directions together.
- `checksum`: `RequestChecksumCalculation` of the AWS SDK. With
  `when_supported` the SDK adds CRC32 checksums that some S3-compatible
  providers are reported to reject, so the default is `when_required`, except
  for AWS endpoints.

## CLI

Global flags: `--config` (`-c`, default `config.yaml`), `--debug`,
`--logFormat` (`console` or `json`, default `console`).

```
s3-sync --config config.yaml run [--dry-run]
s3-sync --config config.yaml validate [--no-env]
s3-sync --config config.yaml confirm-delete <source> <target> --count N
```

`validate --no-env` treats unset environment variables as `"0"`, so that CI
can check the configuration without its secrets.

`--dry-run` lists and compares as a normal pass, logs what would be copied and
deleted, and changes neither the target nor the state.

## Metrics and health

HTTP on `metrics.listen`: `/metrics`, `/healthz` (process alive), `/readyz`
(configuration loaded, state open). Metrics carry `source` and `target`
labels:

- `s3sync_last_success_timestamp_seconds`
- `s3sync_pass_duration_seconds`
- `s3sync_pass_failures_total`
- `s3sync_objects` (objects in state)
- `s3sync_copied_objects_total`, `s3sync_copied_bytes_total`
- `s3sync_deleted_objects_total`
- `s3sync_deletions_pending` (missing, waiting for `delete.delay`)
- `s3sync_deletions_held` (blocked by the guards)
- `s3sync_errors_total{operation}`
- `s3sync_acl_not_copied_objects`
- `s3sync_target_unexpected_objects` (found by the full check)

## Deployment in Kubernetes

One replica, `strategy: Recreate`, a `ReadWriteOnce` PVC for the state.
`confirm-delete` runs with `kubectl exec` in the same pod.

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
  Storage class is fixed at bucket creation.
- AWS SDK for Go v2 adds request checksums by default
  ([checksums](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/s3-checksums.html)).

## To verify during implementation

- Spaces honours `If-Match` on `GetObject`.
- Changing an ACL in Spaces leaves ETag and LastModified unchanged.
- Selectel accepts or rejects `PutObject` with `x-amz-acl`.
- Spaces and Selectel accept or reject the CRC32 checksums of
  `when_supported`.
- What `GetObjectAcl` returns in Spaces for the owner and for public objects,
  to confirm the ACL table.
