//go:build e2e

// End-to-end tests against MinIO. They start a container with docker:
//
//	go test -tags e2e ./internal/syncer
//
// S3SYNC_E2E_IMAGE overrides the image (default quay.io/minio/minio:latest).
package syncer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/metanovii/s3-sync/internal/config"
	"github.com/metanovii/s3-sync/internal/metrics"
	"github.com/metanovii/s3-sync/internal/state"
	"github.com/metanovii/s3-sync/internal/storage"
)

var endpoint string

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	image := os.Getenv("S3SYNC_E2E_IMAGE")
	if image == "" {
		image = "quay.io/minio/minio:latest"
	}
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", "127.0.0.1::9000",
		"-e", "MINIO_ROOT_USER=minioadmin", "-e", "MINIO_ROOT_PASSWORD=minioadmin",
		image, "server", "/data").Output()
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot start MinIO:", err)
		os.Exit(1)
	}
	id := strings.TrimSpace(string(out))
	port, err := exec.Command("docker", "port", id, "9000/tcp").Output()
	if err != nil {
		_ = exec.Command("docker", "rm", "-f", id).Run()
		fmt.Fprintln(os.Stderr, "cannot read MinIO port:", err)
		os.Exit(1)
	}
	endpoint = "http://" + strings.TrimSpace(strings.Split(string(port), "\n")[0])
	for i := 0; ; i++ {
		resp, err := http.Get(endpoint + "/minio/health/ready")
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			break
		}
		if i > 60 {
			_ = exec.Command("docker", "rm", "-f", id).Run()
			fmt.Fprintln(os.Stderr, "MinIO did not start")
			os.Exit(1)
		}
		time.Sleep(500 * time.Millisecond)
	}
	code := m.Run()
	_ = exec.Command("docker", "rm", "-f", id).Run()
	os.Exit(code)
}

var bucketSeq atomic.Int64

type env struct {
	t        *testing.T
	ctx      context.Context
	raw      *s3.Client
	src, dst string
	client   *storage.Client
	db       *state.DB
	now      time.Time
}

func provider() config.Provider {
	return config.Provider{Endpoint: endpoint, Region: "us-east-1", PathStyle: true,
		Checksum: config.ChecksumWhenRequired, AccessKey: "minioadmin", SecretKey: "minioadmin"}
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")))
	require.NoError(t, err)
	raw := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	n := bucketSeq.Add(1)
	e := &env{t: t, ctx: ctx, raw: raw,
		src: fmt.Sprintf("src-%d-%d", time.Now().UnixNano()%1e6, n),
		dst: fmt.Sprintf("dst-%d-%d", time.Now().UnixNano()%1e6, n),
		now: time.Now()}
	for _, b := range []string{e.src, e.dst} {
		_, err := raw.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)})
		require.NoError(t, err)
	}
	e.client, err = storage.New(ctx, "minio", provider(), 16, storage.NewLimits())
	require.NoError(t, err)
	e.db, err = state.Open(filepath.Join(t.TempDir(), "state.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.db.Close() })
	return e
}

func (e *env) sync(srcPrefix, dstPrefix string, opts func(*config.Sync)) *Syncer {
	sc := config.Sync{
		Source: config.Location{Provider: "minio", Bucket: e.src, Prefix: srcPrefix},
		Target: config.Location{Provider: "minio", Bucket: e.dst, Prefix: dstPrefix},
		SyncOptions: config.SyncOptions{
			Interval: config.Duration(time.Minute), Timeout: config.Duration(time.Minute),
			FullCheck: config.Duration(24 * time.Hour), ACL: config.ACLSkip,
			Delete: config.Delete{Enabled: true, Delay: config.Duration(time.Hour), MinCount: 100, MaxFraction: 0.01, MaxCount: 10000},
		},
	}
	if opts != nil {
		opts(&sc)
	}
	return e.syncer(sc, e.db, false)
}

func (e *env) syncer(sc config.Sync, db *state.DB, dryRun bool) *Syncer {
	s := New(sc, e.client, e.client, db, NewPool(8), metrics.New(prometheus.NewRegistry()), dryRun)
	s.now = func() time.Time { return e.now }
	require.NoError(e.t, s.Prepare(e.ctx, "minio", "minio"))
	return s
}

func (e *env) put(bucket, key string, body []byte, in func(*s3.PutObjectInput)) {
	p := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body)}
	if in != nil {
		in(p)
	}
	_, err := e.raw.PutObject(e.ctx, p)
	require.NoError(e.t, err)
}

func (e *env) get(bucket, key string) (*s3.GetObjectOutput, []byte) {
	out, err := e.raw.GetObject(e.ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(e.t, err, key)
	b, err := io.ReadAll(out.Body)
	require.NoError(e.t, err)
	_ = out.Body.Close()
	return out, b
}

func (e *env) keys(bucket string) []string {
	var keys []string
	err := e.client.List(e.ctx, bucket, "", func(objs []storage.Object) error {
		for _, o := range objs {
			keys = append(keys, o.Key)
		}
		return nil
	})
	require.NoError(e.t, err)
	return keys
}

func (e *env) remove(bucket, key string) {
	_, err := e.raw.DeleteObject(e.ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(e.t, err)
}

func (e *env) pass(s *Syncer) *Result {
	res, err := s.Pass(e.ctx)
	require.NoError(e.t, err)
	require.Zero(e.t, res.Failed, "failed objects")
	return res
}

func TestCopyChangeDelete(t *testing.T) {
	e := newEnv(t)
	expires := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	e.put(e.src, "a.txt", []byte("hello"), func(p *s3.PutObjectInput) {
		p.ContentType = aws.String("text/plain")
		p.CacheControl = aws.String("max-age=60")
		p.ContentDisposition = aws.String(`attachment; filename="a.txt"`)
		p.ContentLanguage = aws.String("ru")
		p.Expires = aws.Time(expires)
		p.Metadata = map[string]string{"owner": "team"}
	})
	e.put(e.src, "gz/b.js", []byte("\x1f\x8bnot really gzip"), func(p *s3.PutObjectInput) {
		p.ContentEncoding = aws.String("gzip")
	})
	e.put(e.src, "dir/", nil, nil)
	s := e.sync("", "", nil)

	res := e.pass(s)
	require.Equal(t, int64(3), res.Copied)
	require.ElementsMatch(t, []string{"a.txt", "gz/b.js", "dir/"}, e.keys(e.dst))

	out, body := e.get(e.dst, "a.txt")
	require.Equal(t, "hello", string(body))
	require.Equal(t, "text/plain", aws.ToString(out.ContentType))
	require.Equal(t, "max-age=60", aws.ToString(out.CacheControl))
	require.Equal(t, `attachment; filename="a.txt"`, aws.ToString(out.ContentDisposition))
	require.Equal(t, "ru", aws.ToString(out.ContentLanguage))
	require.Equal(t, "team", out.Metadata["owner"])
	exp, err := http.ParseTime(aws.ToString(out.ExpiresString))
	require.NoError(t, err)
	require.True(t, exp.Equal(expires))

	// Bytes are copied as they are, not decompressed.
	out, body = e.get(e.dst, "gz/b.js")
	require.Equal(t, "\x1f\x8bnot really gzip", string(body))
	require.Equal(t, "gzip", aws.ToString(out.ContentEncoding))

	// Nothing changed: nothing copied.
	res = e.pass(s)
	require.Zero(t, res.Copied)

	// A change is copied.
	e.put(e.src, "a.txt", []byte("hello again"), nil)
	res = e.pass(s)
	require.Equal(t, int64(1), res.Copied)
	_, body = e.get(e.dst, "a.txt")
	require.Equal(t, "hello again", string(body))

	// A deletion waits for delete.delay.
	e.remove(e.src, "gz/b.js")
	res = e.pass(s)
	require.Zero(t, res.Deleted)
	require.Contains(t, e.keys(e.dst), "gz/b.js")

	e.now = e.now.Add(2 * time.Hour)
	res = e.pass(s)
	require.Equal(t, int64(1), res.Deleted)
	require.NotContains(t, e.keys(e.dst), "gz/b.js")

	// A key that comes back before the delay is not deleted.
	e.remove(e.src, "dir/")
	e.pass(s)
	e.put(e.src, "dir/", nil, nil)
	e.now = e.now.Add(2 * time.Hour)
	res = e.pass(s)
	require.Zero(t, res.Deleted)
	require.Contains(t, e.keys(e.dst), "dir/")
}

func TestDeletionGuard(t *testing.T) {
	e := newEnv(t)
	for i := range 5 {
		e.put(e.src, fmt.Sprintf("k%d", i), []byte("x"), nil)
	}
	s := e.sync("", "", func(sc *config.Sync) { sc.Delete.MaxCount = 2 })
	e.pass(s)

	for i := range 3 {
		e.remove(e.src, fmt.Sprintf("k%d", i))
	}
	e.pass(s)
	e.now = e.now.Add(2 * time.Hour)
	res := e.pass(s)
	require.Equal(t, int64(3), res.Held)
	require.Zero(t, res.Deleted)
	require.Len(t, e.keys(e.dst), 5)

	// Raising the limit lets the deletions run.
	s = e.sync("", "", func(sc *config.Sync) { sc.Delete.MaxCount = 10 })
	res = e.pass(s)
	require.Equal(t, int64(3), res.Deleted)
	require.ElementsMatch(t, []string{"k3", "k4"}, e.keys(e.dst))
}

func TestFirstPassAdoptsTarget(t *testing.T) {
	e := newEnv(t)
	e.put(e.src, "same", []byte("abc"), nil)
	e.put(e.src, "resized", []byte("abcdef"), nil)
	e.put(e.src, "new", []byte("n"), nil)
	e.put(e.dst, "same", []byte("abc"), nil)
	e.put(e.dst, "resized", []byte("abc"), nil)
	e.put(e.dst, "extra", []byte("e"), nil)

	res := e.pass(e.sync("", "", nil))
	require.Equal(t, int64(1), res.Adopted)
	require.Equal(t, int64(2), res.Copied)
	_, body := e.get(e.dst, "resized")
	require.Equal(t, "abcdef", string(body))
	// Objects the source does not have are not touched.
	require.Contains(t, e.keys(e.dst), "extra")
}

func TestFullCheckRepairsTarget(t *testing.T) {
	e := newEnv(t)
	e.put(e.src, "a", []byte("a"), nil)
	e.put(e.src, "b", []byte("b"), nil)
	s := e.sync("", "", nil)
	e.pass(s)

	e.remove(e.dst, "a")
	e.put(e.dst, "stray", []byte("s"), nil)
	res := e.pass(s)
	require.False(t, res.FullCheck)
	require.Zero(t, res.Copied)

	e.now = e.now.Add(25 * time.Hour)
	res = e.pass(s)
	require.True(t, res.FullCheck)
	require.Equal(t, int64(1), res.Copied)
	require.ElementsMatch(t, []string{"a", "b", "stray"}, e.keys(e.dst))
}

func TestPrefixMapping(t *testing.T) {
	e := newEnv(t)
	e.put(e.src, "uploads/images/a.jpg", []byte("a"), nil)
	e.put(e.src, "uploads/images/", nil, nil) // the prefix itself is skipped
	e.put(e.src, "uploads/images2/b.jpg", []byte("b"), nil)
	e.put(e.src, "uploads/c.jpg", []byte("c"), nil)
	res := e.pass(e.sync("uploads/images/", "img/", nil))
	require.Equal(t, int64(1), res.Copied)
	require.Equal(t, []string{"img/a.jpg"}, e.keys(e.dst))
}

func TestMultipart(t *testing.T) {
	e := newEnv(t)
	big := make([]byte, multipartThreshold+5<<20)
	_, _ = rand.Read(big)
	e.put(e.src, "big.bin", big, func(p *s3.PutObjectInput) { p.ContentType = aws.String("application/x-test") })

	res := e.pass(e.sync("", "", nil))
	require.Equal(t, int64(1), res.Copied)
	require.Equal(t, int64(len(big)), res.Bytes)
	out, body := e.get(e.dst, "big.bin")
	require.Equal(t, sha256.Sum256(big), sha256.Sum256(body))
	require.Equal(t, "application/x-test", aws.ToString(out.ContentType))
}

func TestCancelAbortsMultipart(t *testing.T) {
	e := newEnv(t)
	big := make([]byte, multipartThreshold+5<<20)
	e.put(e.src, "big.bin", big, nil)

	slow := provider()
	slow.RateLimit.BandwidthTotal = 8 << 20 // about 9 s for the object
	client, err := storage.New(e.ctx, "slow", slow, 4, storage.NewLimits())
	require.NoError(t, err)
	e.client = client
	s := e.sync("", "", nil)

	ctx, cancel := context.WithTimeout(e.ctx, 3*time.Second)
	defer cancel()
	res, err := s.Pass(ctx)
	require.Error(t, err)
	// Cut by the timeout, not failed: no false alerts during a long first copy.
	require.Zero(t, res.Failed)
	require.Equal(t, int64(1), res.Interrupted)

	uploads, err := client.ListUploads(e.ctx, e.dst, "")
	require.NoError(t, err)
	require.Empty(t, uploads)
	require.Empty(t, e.keys(e.dst))
}

func TestDryRunChangesNothing(t *testing.T) {
	e := newEnv(t)
	e.put(e.src, "a", []byte("a"), nil)
	e.put(e.src, "b", []byte("b"), nil)
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := state.Open(path)
	require.NoError(t, err)
	e.db = db
	s := e.sync("", "", nil)
	e.pass(s)
	e.remove(e.src, "a")
	e.put(e.src, "c", []byte("c"), nil)
	e.pass(s) // "a" becomes missing
	e.now = e.now.Add(2 * time.Hour)

	// A dry run works while the database is locked by run.
	cp, err := state.OpenCopy(path)
	require.NoError(t, err)
	defer func() { _ = cp.Close() }()
	dry := e.syncer(s.cfg, cp, true)
	res, err := dry.Pass(e.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Deleted)
	require.ElementsMatch(t, []string{"a", "b", "c"}, e.keys(e.dst))

	// The real pass still has everything to do.
	res = e.pass(s)
	require.Equal(t, int64(1), res.Deleted)
	require.ElementsMatch(t, []string{"b", "c"}, e.keys(e.dst))
}

func TestACLProbeDetectsIgnoredACL(t *testing.T) {
	e := newEnv(t)
	sc := config.Sync{
		Source:      config.Location{Provider: "minio", Bucket: e.src},
		Target:      config.Location{Provider: "minio", Bucket: e.dst},
		SyncOptions: config.SyncOptions{ACL: "public-read", Timeout: config.Duration(time.Minute)},
	}
	s := New(sc, e.client, e.client, e.db, NewPool(2), metrics.New(prometheus.NewRegistry()), false)
	err := s.Prepare(e.ctx, "minio", "minio")
	// MinIO does not keep object ACLs: the probe must say so instead of
	// letting every object be copied without them.
	require.ErrorContains(t, err, "ACL probe")
	require.Empty(t, e.keys(e.dst), "probe object left behind")
}

func TestKeysWithSpecialCharacters(t *testing.T) {
	e := newEnv(t)
	keys := []string{"bad\x01key", "space key", "plus+key", "pct%20key", "кириллица/ключ", "q?h#", "empty"}
	for _, k := range keys {
		body := []byte(k)
		if k == "empty" {
			body = nil
		}
		e.put(e.src, k, body, nil)
	}
	s := e.sync("", "", nil)
	res := e.pass(s)
	require.Equal(t, int64(len(keys)), res.Copied)
	require.ElementsMatch(t, keys, e.keys(e.dst))
	res = e.pass(s)
	require.Zero(t, res.Copied, "unchanged keys copied again")
}

func TestInterruptedFirstPassStillAdopts(t *testing.T) {
	e := newEnv(t)
	for _, k := range []string{"a", "b", "c"} {
		e.put(e.src, k, []byte(k), nil)
	}
	time.Sleep(1100 * time.Millisecond) // target copies are newer, in whole seconds
	for _, k := range []string{"a", "b", "c"} {
		e.put(e.dst, k, []byte(k), nil)
	}
	s := e.sync("", "", nil)
	// One record as left by a first pass that was cancelled.
	require.NoError(t, e.db.PutObject(e.ctx, s.ID, 1, state.Object{Key: "a", Size: 1, ETag: "x", LastModified: e.now}, e.now))

	res := e.pass(s)
	require.Zero(t, res.Copied)
	require.Equal(t, int64(3), res.Adopted)
}

func TestOlderTargetCopyIsNotAdopted(t *testing.T) {
	e := newEnv(t)
	e.put(e.dst, "a", []byte("old"), nil)
	time.Sleep(1100 * time.Millisecond)
	e.put(e.src, "a", []byte("new"), nil) // same size, written later

	res := e.pass(e.sync("", "", nil))
	require.Zero(t, res.Adopted)
	require.Equal(t, int64(1), res.Copied)
	_, body := e.get(e.dst, "a")
	require.Equal(t, "new", string(body))
}
