package state

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const id = "src/a -> dst/b"

func obj(key string, size int64, etag string) Object {
	return Object{Key: key, Size: size, ETag: etag, LastModified: time.Unix(1700000000, 0)}
}

func openTest(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	d, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d, path
}

func TestLock(t *testing.T) {
	_, path := openTest(t)
	_, err := Open(path)
	require.ErrorIs(t, err, ErrLocked)
}

func TestPassLifecycle(t *testing.T) {
	ctx := context.Background()
	d, _ := openTest(t)

	reset, err := d.EnsureSync(ctx, id, "a.example", "b.example")
	require.NoError(t, err)
	require.False(t, reset)

	pass, err := d.BeginPass(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(1), pass)

	listed := []Object{obj("a", 1, `"x"`), obj("b", 2, `"y"`)}
	changed, err := d.MarkSeen(ctx, id, pass, listed)
	require.NoError(t, err)
	require.Equal(t, listed, changed)
	for _, o := range changed {
		require.NoError(t, d.PutObject(ctx, id, pass, o, time.Now()))
	}

	// Second pass: "a" unchanged, "b" changed, "c" new; milliseconds in the
	// listing must not count as a change.
	pass, err = d.BeginPass(ctx, id)
	require.NoError(t, err)
	a := obj("a", 1, `"x"`)
	a.LastModified = a.LastModified.Add(300 * time.Millisecond)
	changed, err = d.MarkSeen(ctx, id, pass, []Object{a, obj("b", 3, `"z"`), obj("c", 1, `"w"`)})
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c"}, keys(changed))

	// Third pass lists only "c": "a" and "b" become missing.
	pass, err = d.BeginPass(ctx, id)
	require.NoError(t, err)
	require.NoError(t, d.PutObject(ctx, id, pass, obj("c", 1, `"w"`), time.Now()))
	_, err = d.MarkSeen(ctx, id, pass, []Object{obj("c", 1, `"w"`)})
	require.NoError(t, err)
	now := time.Unix(1800000000, 0)
	require.NoError(t, d.MarkMissing(ctx, id, pass, now))

	due, err := d.Due(ctx, id, now.Add(-time.Hour))
	require.NoError(t, err)
	require.Empty(t, due)
	n, err := d.CountMissing(ctx, id, now.Add(-time.Hour))
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	due, err = d.Due(ctx, id, now)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, due)

	// "a" comes back before deletion: no longer missing.
	pass, err = d.BeginPass(ctx, id)
	require.NoError(t, err)
	_, err = d.MarkSeen(ctx, id, pass, []Object{obj("a", 1, `"x"`), obj("c", 1, `"w"`)})
	require.NoError(t, err)
	require.NoError(t, d.MarkMissing(ctx, id, pass, now.Add(time.Minute)))
	due, err = d.Due(ctx, id, now.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, due)

	require.NoError(t, d.DeleteObject(ctx, id, "b"))
	total, err := d.CountObjects(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(2), total)
}

func TestEndpointChangeResets(t *testing.T) {
	ctx := context.Background()
	d, _ := openTest(t)
	_, err := d.EnsureSync(ctx, id, "a.example", "b.example")
	require.NoError(t, err)
	require.NoError(t, d.PutObject(ctx, id, 1, obj("a", 1, `"x"`), time.Now()))

	reset, err := d.EnsureSync(ctx, id, "a.example", "b.example")
	require.NoError(t, err)
	require.False(t, reset)

	reset, err = d.EnsureSync(ctx, id, "a.example", "c.example")
	require.NoError(t, err)
	require.True(t, reset)
	n, err := d.CountObjects(ctx, id)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestCompareTarget(t *testing.T) {
	ctx := context.Background()
	d, _ := openTest(t)
	_, err := d.EnsureSync(ctx, id, "a", "b")
	require.NoError(t, err)
	for _, o := range []Object{obj("same", 1, "e"), obj("resized", 2, "e"), obj("absent", 3, "e")} {
		require.NoError(t, d.PutObject(ctx, id, 1, o, time.Now()))
	}
	require.NoError(t, d.ClearTargetKeys(ctx, id))
	lm := time.Unix(1700000000, 0)
	require.NoError(t, d.AddTargetKeys(ctx, id, []TargetObject{{"same", 1, lm}, {"resized", 5, lm}, {"extra", 7, lm}}))

	found, err := d.TargetObjects(ctx, id, []string{"same", "extra", "nope"})
	require.NoError(t, err)
	require.Equal(t, map[string]TargetObject{"same": {"same", 1, lm}, "extra": {"extra", 7, lm}}, found)

	dropped, unexpected, err := d.CompareTarget(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(2), dropped)
	require.Equal(t, int64(1), unexpected)

	left, err := d.ListObjects(ctx, id, "", 10)
	require.NoError(t, err)
	require.Equal(t, []string{"same"}, keys(left))
}

func TestOpenCopy(t *testing.T) {
	ctx := context.Background()
	d, path := openTest(t)
	_, err := d.EnsureSync(ctx, id, "a", "b")
	require.NoError(t, err)
	require.NoError(t, d.PutObject(ctx, id, 1, obj("a", 1, "e"), time.Now()))

	// Works while the original is locked; changes stay in the copy.
	c, err := OpenCopy(path)
	require.NoError(t, err)
	require.NoError(t, c.DeleteObject(ctx, id, "a"))
	require.NoError(t, c.Close())

	n, err := d.CountObjects(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	empty, err := OpenCopy(filepath.Join(t.TempDir(), "missing.db"))
	require.NoError(t, err)
	require.NoError(t, empty.Close())
}

func TestManyKeys(t *testing.T) {
	ctx := context.Background()
	d, _ := openTest(t)
	_, err := d.EnsureSync(ctx, id, "a", "b")
	require.NoError(t, err)
	var listed []Object
	for i := 0; i < 1234; i++ {
		listed = append(listed, obj(time.Duration(i).String(), 1, "e"))
	}
	changed, err := d.MarkSeen(ctx, id, 1, listed)
	require.NoError(t, err)
	require.Len(t, changed, len(listed))
}

func keys(objs []Object) []string {
	var k []string
	for _, o := range objs {
		k = append(k, o.Key)
	}
	return k
}

func TestMigrateV1(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE target_keys (sync_id TEXT, key TEXT, size INTEGER, PRIMARY KEY (sync_id, key)) WITHOUT ROWID; PRAGMA user_version = 1")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	d, err := Open(path)
	require.NoError(t, err)
	defer func() { _ = d.Close() }()
	ctx := context.Background()
	require.NoError(t, d.AddTargetKeys(ctx, id, []TargetObject{{"a", 1, time.Unix(1, 0)}}))
}

func TestRemoveStaleCopies(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, ".s3-sync-dry-run-1.db")
	oldWAL := old + "-wal"
	fresh := filepath.Join(dir, ".s3-sync-dry-run-2.db")
	for _, f := range []string{old, oldWAL, fresh} {
		require.NoError(t, os.WriteFile(f, nil, 0o600))
	}
	past := time.Now().Add(-48 * time.Hour)
	require.NoError(t, os.Chtimes(old, past, past))
	require.NoError(t, os.Chtimes(oldWAL, past, past))

	removeStaleCopies(dir)
	for f, want := range map[string]bool{old: false, oldWAL: false, fresh: true} {
		_, err := os.Stat(f)
		require.Equal(t, want, err == nil, f)
	}
}
