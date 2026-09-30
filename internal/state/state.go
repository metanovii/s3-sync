// Package state keeps what has been copied, in SQLite.
package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/flock"
	_ "modernc.org/sqlite"
)

const schemaVersion = 2

const schema = `
CREATE TABLE IF NOT EXISTS syncs (
  id              TEXT PRIMARY KEY,
  source_endpoint TEXT NOT NULL,
  target_endpoint TEXT NOT NULL,
  pass            INTEGER NOT NULL DEFAULT 0,
  last_success    INTEGER,
  last_full_check INTEGER
);
CREATE TABLE IF NOT EXISTS objects (
  sync_id        TEXT NOT NULL,
  key            TEXT NOT NULL,
  size           INTEGER NOT NULL,
  etag           TEXT NOT NULL,
  last_modified  INTEGER NOT NULL,
  acl            TEXT NOT NULL DEFAULT '',
  copied_at      INTEGER NOT NULL,
  seen_pass      INTEGER NOT NULL,
  missing_since  INTEGER,
  PRIMARY KEY (sync_id, key)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS objects_seen ON objects (sync_id, seen_pass);
CREATE INDEX IF NOT EXISTS objects_missing ON objects (sync_id, missing_since);
CREATE TABLE IF NOT EXISTS target_keys (
  sync_id       TEXT NOT NULL,
  key           TEXT NOT NULL,
  size          INTEGER NOT NULL,
  last_modified INTEGER NOT NULL,
  PRIMARY KEY (sync_id, key)
) WITHOUT ROWID;
`

// batch is the number of keys in one IN (...) list.
const batch = 500

// ErrLocked means another process holds the state database.
var ErrLocked = errors.New("state database is used by another s3-sync process")

// DB is the state database.
type DB struct {
	db   *sql.DB
	lock *flock.Flock
	// temp is a file to remove on Close (dry-run copy).
	temp string
}

// Object is one source object as recorded in state. LastModified is kept in
// whole seconds: listings have milliseconds, response headers do not.
type Object struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	ACL          string
}

// Same reports whether two records describe the same object version.
func (o Object) Same(p Object) bool {
	return o.Size == p.Size && o.ETag == p.ETag && o.LastModified.Unix() == p.LastModified.Unix()
}

// Open locks and opens the database at path, creating it when needed.
func Open(path string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	lock := flock.New(path + ".lock")
	ok, err := lock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("lock %s: %w", lock.Path(), err)
	}
	if !ok {
		return nil, ErrLocked
	}
	d, err := open(path)
	if err != nil {
		_ = lock.Unlock()
		return nil, err
	}
	d.lock = lock
	return d, nil
}

// OpenCopy opens a private copy of the database at path for a dry run, in
// the same directory. It does not take the lock, so it works while `run` is
// active. A missing database gives an empty copy.
func OpenCopy(path string) (*DB, error) {
	// Next to the database: the volume that holds it has room for a copy,
	// unlike a small or read-only /tmp.
	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); err != nil {
		dir = ""
	}
	removeStaleCopies(dir)
	f, err := os.CreateTemp(dir, dryRunPattern)
	if err != nil {
		return nil, err
	}
	temp := f.Name()
	_ = f.Close()
	_ = os.Remove(temp) // VACUUM INTO needs a file that does not exist

	if _, err := os.Stat(path); err == nil {
		src, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(10000)")
		if err != nil {
			return nil, err
		}
		_, err = src.Exec("VACUUM INTO ?", temp)
		_ = src.Close()
		if err != nil {
			return nil, fmt.Errorf("copy %s: %w", path, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	d, err := open(temp)
	if err != nil {
		_ = os.Remove(temp)
		return nil, err
	}
	d.temp = temp
	return d, nil
}

const dryRunPattern = ".s3-sync-dry-run-*.db"

// staleCopy is the age after which a dry-run copy is considered left by a
// crashed dry run.
const staleCopy = 24 * time.Hour

// removeStaleCopies deletes dry-run copies (with their -wal and -shm files)
// older than staleCopy, so that crashed dry runs do not fill the volume.
func removeStaleCopies(dir string) {
	if dir == "" {
		dir = os.TempDir()
	}
	files, _ := filepath.Glob(filepath.Join(dir, dryRunPattern+"*"))
	for _, f := range files {
		if fi, err := os.Stat(f); err == nil && time.Since(fi.ModTime()) > staleCopy {
			_ = os.Remove(f)
		}
	}
}

func open(path string) (*DB, error) {
	dsn := "file:" + path + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection: SQLite has one writer anyway, and this rules out
	// SQLITE_BUSY between our own connections.
	db.SetMaxOpenConns(1)
	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return &DB{db: db}, nil
}

func migrate(db *sql.DB) error {
	var v int
	if err := db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		return err
	}
	if v > schemaVersion {
		return fmt.Errorf("schema version %d is newer than this s3-sync (%d)", v, schemaVersion)
	}
	if v == 1 {
		// Version 2 added target_keys.last_modified. The table only holds
		// the target listing of the current pass, so it is recreated.
		if _, err := db.Exec("DROP TABLE IF EXISTS target_keys"); err != nil {
			return err
		}
	}
	if _, err := db.Exec(schema); err != nil {
		return err
	}
	_, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", schemaVersion))
	return err
}

// Close closes the database and releases the lock.
func (d *DB) Close() error {
	err := d.db.Close()
	if d.lock != nil {
		err = errors.Join(err, d.lock.Unlock())
	}
	if d.temp != "" {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			_ = os.Remove(d.temp + suffix)
		}
	}
	return err
}

// Sync is the stored state of one sync.
type Sync struct {
	Pass          int64
	LastSuccess   time.Time
	LastFullCheck time.Time
}

// EnsureSync registers the sync. When the stored endpoints differ from the
// given ones, the objects of the sync are dropped and reset is true.
func (d *DB) EnsureSync(ctx context.Context, id, sourceEndpoint, targetEndpoint string) (reset bool, err error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var src, dst string
	err = tx.QueryRowContext(ctx, "SELECT source_endpoint, target_endpoint FROM syncs WHERE id = ?", id).Scan(&src, &dst)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(ctx, "INSERT INTO syncs (id, source_endpoint, target_endpoint) VALUES (?, ?, ?)",
			id, sourceEndpoint, targetEndpoint); err != nil {
			return false, err
		}
	case err != nil:
		return false, err
	case src != sourceEndpoint || dst != targetEndpoint:
		reset = true
		for _, q := range []string{
			"DELETE FROM objects WHERE sync_id = ?",
			"DELETE FROM target_keys WHERE sync_id = ?",
			"UPDATE syncs SET last_full_check = NULL WHERE id = ?",
		} {
			if _, err := tx.ExecContext(ctx, q, id); err != nil {
				return false, err
			}
		}
		if _, err := tx.ExecContext(ctx, "UPDATE syncs SET source_endpoint = ?, target_endpoint = ? WHERE id = ?",
			sourceEndpoint, targetEndpoint, id); err != nil {
			return false, err
		}
	}
	return reset, tx.Commit()
}

// GetSync returns the stored state of the sync.
func (d *DB) GetSync(ctx context.Context, id string) (Sync, error) {
	var s Sync
	var success, full sql.NullInt64
	err := d.db.QueryRowContext(ctx, "SELECT pass, last_success, last_full_check FROM syncs WHERE id = ?", id).
		Scan(&s.Pass, &success, &full)
	if err != nil {
		return s, err
	}
	if success.Valid {
		s.LastSuccess = time.Unix(success.Int64, 0)
	}
	if full.Valid {
		s.LastFullCheck = time.Unix(full.Int64, 0)
	}
	return s, nil
}

// BeginPass increments and returns the pass number.
func (d *DB) BeginPass(ctx context.Context, id string) (int64, error) {
	var pass int64
	err := d.db.QueryRowContext(ctx, "UPDATE syncs SET pass = pass + 1 WHERE id = ? RETURNING pass", id).Scan(&pass)
	return pass, err
}

// SetLastSuccess records the end of a successful pass.
func (d *DB) SetLastSuccess(ctx context.Context, id string, t time.Time) error {
	_, err := d.db.ExecContext(ctx, "UPDATE syncs SET last_success = ? WHERE id = ?", t.Unix(), id)
	return err
}

// SetLastFullCheck records the time of a full check.
func (d *DB) SetLastFullCheck(ctx context.Context, id string, t time.Time) error {
	_, err := d.db.ExecContext(ctx, "UPDATE syncs SET last_full_check = ? WHERE id = ?", t.Unix(), id)
	return err
}

// CountObjects returns the number of objects of the sync.
func (d *DB) CountObjects(ctx context.Context, id string) (int64, error) {
	var n int64
	err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM objects WHERE sync_id = ?", id).Scan(&n)
	return n, err
}

// MarkSeen handles one listing page in one transaction: every listed key
// already in state gets seen_pass and loses missing_since. It returns the
// listed objects that are absent from state or differ from their record.
func (d *DB) MarkSeen(ctx context.Context, id string, pass int64, listed []Object) (changed []Object, err error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	stored := make(map[string]Object, len(listed))
	for start := 0; start < len(listed); start += batch {
		part := listed[start:min(start+batch, len(listed))]
		args := make([]any, 0, len(part)+1)
		args = append(args, id)
		for _, o := range part {
			args = append(args, o.Key)
		}
		in := placeholders(len(part))
		rows, err := tx.QueryContext(ctx,
			"SELECT key, size, etag, last_modified FROM objects WHERE sync_id = ? AND key IN ("+in+")", args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var o Object
			var lm int64
			if err := rows.Scan(&o.Key, &o.Size, &o.ETag, &lm); err != nil {
				_ = rows.Close()
				return nil, err
			}
			o.LastModified = time.Unix(lm, 0)
			stored[o.Key] = o
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return nil, err
		}
		upd := append([]any{pass}, args...)
		if _, err := tx.ExecContext(ctx,
			"UPDATE objects SET seen_pass = ?, missing_since = NULL WHERE sync_id = ? AND key IN ("+in+")", upd...); err != nil {
			return nil, err
		}
	}
	for _, o := range listed {
		if s, ok := stored[o.Key]; !ok || !s.Same(o) {
			changed = append(changed, o)
		}
	}
	return changed, tx.Commit()
}

// PutObject records a copied object.
func (d *DB) PutObject(ctx context.Context, id string, pass int64, o Object, copiedAt time.Time) error {
	_, err := d.db.ExecContext(ctx, `
INSERT INTO objects (sync_id, key, size, etag, last_modified, acl, copied_at, seen_pass, missing_since)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
ON CONFLICT (sync_id, key) DO UPDATE SET
  size = excluded.size, etag = excluded.etag, last_modified = excluded.last_modified,
  acl = excluded.acl, copied_at = excluded.copied_at, seen_pass = excluded.seen_pass,
  missing_since = NULL`,
		id, o.Key, o.Size, o.ETag, o.LastModified.Unix(), o.ACL, copiedAt.Unix(), pass)
	return err
}

// SetACL updates the recorded ACL of an object.
func (d *DB) SetACL(ctx context.Context, id, key, acl string) error {
	_, err := d.db.ExecContext(ctx, "UPDATE objects SET acl = ? WHERE sync_id = ? AND key = ?", acl, id, key)
	return err
}

// MarkMissing sets missing_since for keys not seen in the pass. Call it only
// after a complete listing.
func (d *DB) MarkMissing(ctx context.Context, id string, pass int64, now time.Time) error {
	_, err := d.db.ExecContext(ctx,
		"UPDATE objects SET missing_since = ? WHERE sync_id = ? AND seen_pass < ? AND missing_since IS NULL",
		now.Unix(), id, pass)
	return err
}

// Due returns keys missing since before or earlier.
func (d *DB) Due(ctx context.Context, id string, before time.Time) ([]string, error) {
	rows, err := d.db.QueryContext(ctx,
		"SELECT key FROM objects WHERE sync_id = ? AND missing_since IS NOT NULL AND missing_since <= ? ORDER BY key",
		id, before.Unix())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// CountMissing returns the number of missing keys that are not yet due.
func (d *DB) CountMissing(ctx context.Context, id string, before time.Time) (int64, error) {
	var n int64
	err := d.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM objects WHERE sync_id = ? AND missing_since > ?", id, before.Unix()).Scan(&n)
	return n, err
}

// DeleteObject removes the record of a key.
func (d *DB) DeleteObject(ctx context.Context, id, key string) error {
	_, err := d.db.ExecContext(ctx, "DELETE FROM objects WHERE sync_id = ? AND key = ?", id, key)
	return err
}

// ListObjects returns up to limit records with keys after the given one.
func (d *DB) ListObjects(ctx context.Context, id, after string, limit int) ([]Object, error) {
	rows, err := d.db.QueryContext(ctx,
		"SELECT key, size, etag, last_modified, acl FROM objects WHERE sync_id = ? AND key > ? AND missing_since IS NULL ORDER BY key LIMIT ?",
		id, after, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Object
	for rows.Next() {
		var o Object
		var lm int64
		if err := rows.Scan(&o.Key, &o.Size, &o.ETag, &lm, &o.ACL); err != nil {
			return nil, err
		}
		o.LastModified = time.Unix(lm, 0)
		out = append(out, o)
	}
	return out, rows.Err()
}

// TargetObject is one object of the target listing, with its key mapped to
// the source key.
type TargetObject struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// ClearTargetKeys empties the target listing of the sync.
func (d *DB) ClearTargetKeys(ctx context.Context, id string) error {
	_, err := d.db.ExecContext(ctx, "DELETE FROM target_keys WHERE sync_id = ?", id)
	return err
}

// AddTargetKeys stores one page of the target listing, with keys already
// mapped to source keys.
func (d *DB) AddTargetKeys(ctx context.Context, id string, keys []TargetObject) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx, "INSERT OR REPLACE INTO target_keys (sync_id, key, size, last_modified) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for _, k := range keys {
		if _, err := stmt.ExecContext(ctx, id, k.Key, k.Size, k.LastModified.Unix()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// TargetObjects returns the given keys found in the target listing.
func (d *DB) TargetObjects(ctx context.Context, id string, keys []string) (map[string]TargetObject, error) {
	found := make(map[string]TargetObject)
	for start := 0; start < len(keys); start += batch {
		part := keys[start:min(start+batch, len(keys))]
		args := make([]any, 0, len(part)+1)
		args = append(args, id)
		for _, k := range part {
			args = append(args, k)
		}
		rows, err := d.db.QueryContext(ctx,
			"SELECT key, size, last_modified FROM target_keys WHERE sync_id = ? AND key IN ("+placeholders(len(part))+")", args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var o TargetObject
			var lm int64
			if err := rows.Scan(&o.Key, &o.Size, &lm); err != nil {
				_ = rows.Close()
				return nil, err
			}
			o.LastModified = time.Unix(lm, 0)
			found[o.Key] = o
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return nil, err
		}
	}
	return found, nil
}

// CompareTarget compares state with the target listing. Records of present
// objects whose target copy is absent or has another size are dropped, so
// that the pass copies them again. It returns the number of dropped records
// and of target objects that state does not know.
func (d *DB) CompareTarget(ctx context.Context, id string) (dropped, unexpected int64, err error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback() }()
	// Count first: a record dropped below still names a known object.
	err = tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM target_keys t
WHERE t.sync_id = ? AND NOT EXISTS (
  SELECT 1 FROM objects o WHERE o.sync_id = t.sync_id AND o.key = t.key)`, id).Scan(&unexpected)
	if err != nil {
		return 0, 0, err
	}
	res, err := tx.ExecContext(ctx, `
DELETE FROM objects AS o
WHERE o.sync_id = ? AND o.missing_since IS NULL AND NOT EXISTS (
  SELECT 1 FROM target_keys t WHERE t.sync_id = o.sync_id AND t.key = o.key AND t.size = o.size)`, id)
	if err != nil {
		return 0, 0, err
	}
	if dropped, err = res.RowsAffected(); err != nil {
		return 0, 0, err
	}
	return dropped, unexpected, tx.Commit()
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}
