package syncer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metanovii/s3-sync/internal/config"
)

func TestHeld(t *testing.T) {
	d := config.Delete{MinCount: 100, MaxFraction: 0.01, MaxCount: 10000}
	tests := []struct {
		due, total int64
		d          config.Delete
		want       bool
	}{
		{0, 0, d, false},
		{1, 50, d, false},      // below min_count only max_count applies
		{1, 100, d, false},     // exactly 1%
		{2, 100, d, true},      // above 1%
		{10001, 1e7, d, true},  // above max_count
		{10000, 1e7, d, false}, // at max_count and 0.1%
		{3, 5, config.Delete{MaxCount: 2}, true},
		{3, 5, config.Delete{}, false}, // zero limits: no limit
		{50, 100, config.Delete{MinCount: 100, MaxFraction: 0}, false},
	}
	for i, tt := range tests {
		require.Equal(t, tt.want, Held(tt.due, tt.total, tt.d), "case %d", i)
	}
}

func TestPartSize(t *testing.T) {
	require.Equal(t, int64(64<<20), partSize(100<<20))
	require.Equal(t, int64(64<<20), partSize(maxParts*64<<20)) // exactly 10000 parts of 64 MiB
	require.Equal(t, int64(65<<20), partSize(maxParts*64<<20+1))
	p := partSize(1 << 40) // 1 TiB
	require.GreaterOrEqual(t, p*maxParts, int64(1<<40))
	require.Zero(t, p%(1<<20))
}

func TestKeyMapping(t *testing.T) {
	s := &Syncer{cfg: config.Sync{
		Source: config.Location{Prefix: "uploads/images/"},
		Target: config.Location{Prefix: "img/"},
	}}
	require.Equal(t, "img/a/b.jpg", s.targetKey("uploads/images/a/b.jpg"))
	require.Equal(t, "uploads/images/a/b.jpg", s.sourceKey("img/a/b.jpg"))
	require.True(t, skipKey("uploads/images/", "uploads/images/"))
	require.True(t, skipKey("uploads/images/.s3-sync-probe/x", "uploads/images/"))
	require.False(t, skipKey("uploads/images/a", "uploads/images/"))
}

func TestCount412(t *testing.T) {
	s := &Syncer{cur412: make(map[string]int)}
	newPass := func() { s.prev412, s.cur412 = s.cur412, make(map[string]int) }
	newPass()
	require.Equal(t, 1, s.count412("a"))
	newPass()
	require.Equal(t, 2, s.count412("a"))
	require.Equal(t, 1, s.count412("b"))
	newPass() // "a" copied fine this time
	require.Equal(t, 2, s.count412("b"))
	newPass()
	require.Equal(t, 1, s.count412("a"), "a streak must restart")
	require.Len(t, s.prev412, 1, "keys without 412 must drop out")
}
