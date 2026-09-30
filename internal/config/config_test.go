package config

import (
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func env(vars map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := vars[k]
		return v, ok
	}
}

const base = `
providers:
  src:
    endpoint: https://fra1.digitaloceanspaces.com
    region: fra1
    access_key: a
    secret_key: b
  dst:
    endpoint: https://s3.ru-1.storage.selcloud.ru
    region: ru-1
    access_key: c
    secret_key: d
`

func TestExampleConfig(t *testing.T) {
	data, err := os.ReadFile("../../config.example.yaml")
	require.NoError(t, err)
	cfg, err := Parse(data, env(map[string]string{
		"DO_ACCESS_KEY": "a", "DO_SECRET_KEY": "b",
		"SELECTEL_ACCESS_KEY": "c", "SELECTEL_SECRET_KEY": "d",
	}))
	require.NoError(t, err)

	require.Equal(t, 64, cfg.Workers)
	require.Equal(t, "a", cfg.Providers["do"].AccessKey)
	require.Equal(t, Size(100<<20), cfg.Providers["do"].RateLimit.BandwidthTotal)
	require.Equal(t, ChecksumWhenRequired, cfg.Providers["selectel"].Checksum)
	require.Equal(t, ChecksumWhenSupported, cfg.Providers["aws"].Checksum)
	require.Len(t, cfg.Syncs, 4)

	require.Equal(t, "do/uploads -> selectel/uploads", cfg.Syncs[1].ID())
	require.Equal(t, Duration(5*time.Minute), cfg.Syncs[1].Interval)
	require.Equal(t, Duration(2*time.Hour), cfg.Syncs[1].Timeout)

	require.Equal(t, "images/", cfg.Syncs[2].Source.Prefix)
	require.Equal(t, "", cfg.Syncs[2].Target.Prefix)

	last := cfg.Syncs[3]
	require.Equal(t, ACLCopy, last.ACL)
	require.False(t, last.Delete.Enabled)
	// Other delete fields come from defaults.
	require.Equal(t, 10000, last.Delete.MaxCount)
	require.Equal(t, Duration(time.Hour), last.Delete.Delay)
}

func TestBuiltinDefaults(t *testing.T) {
	cfg, err := Parse([]byte(base+`
sync:
  - source: src/aaa
    target: dst/aaa
`), env(nil))
	require.NoError(t, err)
	require.Equal(t, "/var/lib/s3-sync/state.db", cfg.State.Path)
	require.Equal(t, ":9090", cfg.Metrics.Listen)
	require.Equal(t, 32, cfg.Workers)
	require.Equal(t, defaultSyncOptions(), cfg.Syncs[0].SyncOptions)
}

func TestFullCheckZero(t *testing.T) {
	cfg, err := Parse([]byte(base+`
defaults:
  full_check: 0
sync:
  - source: src/aaa
    target: dst/aaa
`), env(nil))
	require.NoError(t, err)
	require.Equal(t, Duration(0), cfg.Syncs[0].FullCheck)
}

func TestEnvNumber(t *testing.T) {
	cfg, err := Parse([]byte(`
providers:
  src:
    endpoint: https://example.com
    region: x
    access_key: ${KEY}
    secret_key: "${KEY}"
    rate_limit:
      requests_per_bucket: ${RPS}
  dst:
    endpoint: https://example.org
    region: x
    access_key: a
    secret_key: b
sync:
  - source: src/aaa
    target: dst/aaa
`), env(map[string]string{"KEY": "123", "RPS": "150"}))
	require.NoError(t, err)
	require.Equal(t, "123", cfg.Providers["src"].AccessKey)
	require.Equal(t, "123", cfg.Providers["src"].SecretKey)
	require.Equal(t, 150.0, cfg.Providers["src"].RateLimit.RequestsPerBucket)
}

func TestErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{"unset env", `
providers:
  src: {endpoint: https://a.example, region: x, access_key: "${NOPE}", secret_key: b}
sync: [{source: src/aaa, target: src/bbb}]
`, "environment variable NOPE is not set"},
		{"unknown key", base + `
sync:
  - source: src/aaa
    target: dst/aaa
    intervall: 5m
`, `line 17: unknown key "intervall" in sync[0]`},
		{"ambiguous size", `
providers:
  src: {endpoint: https://a.example, region: x, access_key: a, secret_key: b, rate_limit: {bandwidth_total: 100M}}
sync: [{source: src/aaa, target: src/bbb}]
`, "unit must be one of"},
		{"unknown provider", base + `
sync: [{source: nope/aaa, target: dst/aaa}]
`, `unknown provider "nope"`},
		{"bad bucket", base + `
sync: [{source: src/a/b, target: dst/aaa}]
`, "invalid bucket name"},
		{"duplicate", base + `
sync:
  - {source: src/aaa, target: dst/aaa}
  - {source: src/aaa/, target: dst/aaa}
`, "duplicates sync[0]"},
		{"targets overlap", base + `
sync:
  - {source: src/aaa, target: dst/aaa}
  - {source: src/bbb, target: dst/aaa/sub}
`, "overlap"},
		{"cycle", base + `
sync:
  - {source: src/aaa, target: dst/aaa}
  - {source: dst/aaa, target: src/bbb}
`, "overlaps the source"},
		{"target is own source", base + `
sync: [{source: src/aaa/x, target: src/aaa}]
`, "overlaps its source"},
		{"shared default chain", `
providers:
  src: {endpoint: https://a.example, region: x}
  dst: {endpoint: https://s3.eu-central-1.amazonaws.com, region: eu-central-1}
sync: [{source: src/aaa, target: dst/aaa}]
`, "would share them"},
		{"half credentials", base + `
  third: {endpoint: https://c.example, region: x, access_key: a}
sync: [{source: src/aaa, target: dst/aaa}]
`, "must be set together"},
		{"bad acl", base + `
sync: [{source: src/aaa, target: dst/aaa, acl: public}]
`, "acl must be"},
		{"bad fraction", base + `
sync: [{source: src/aaa, target: dst/aaa, delete: {max_fraction: 2}}]
`, "max_fraction"},
		{"no sync", base, "at least one sync"},
		{"quoted merge key", base + `
sync: [{"<<": 1, source: src/aaa, target: dst/aaa}]
`, `unknown key "<<" in sync[0]`},
		{"top level key", base + `
bogus: 1
sync: [{source: src/aaa, target: dst/aaa}]
`, `unknown key "bogus" in top level`},
		{"block is not a mapping", base + `
sync: [{source: src/aaa, target: dst/aaa, delete: 5}]
`, "sync[0].delete must be a mapping"},
		{"double dot bucket", base + `
sync: [{source: src/a..b, target: dst/aaa}]
`, "invalid bucket name"},
		{"zero workers", base + `
workers: 0
sync: [{source: src/aaa, target: dst/aaa}]
`, "workers must be at least 1"},
		{"same AWS bucket via different endpoints", `
providers:
  a: {endpoint: https://s3.amazonaws.com, region: us-east-1, access_key: a, secret_key: b}
  b: {endpoint: https://s3.eu-central-1.amazonaws.com, region: eu-central-1, access_key: c, secret_key: d}
  src: {endpoint: https://src.example, region: x, access_key: e, secret_key: f}
sync:
  - {source: src/aaa, target: a/bbb}
  - {source: src/ccc, target: b/bbb}
`, "overlap"},
		{"default port", `
providers:
  a: {endpoint: https://s3.example.com, region: x, access_key: a, secret_key: b}
  b: {endpoint: "https://S3.example.com:443", region: x, access_key: c, secret_key: d}
sync:
  - {source: a/aaa, target: b/aaa}
`, "overlaps its source"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml), env(nil))
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestPrefixIsDirectory(t *testing.T) {
	_, err := Parse([]byte(base+`
sync:
  - {source: src/aaa, target: dst/aaa/images}
  - {source: src/bbb, target: dst/aaa/images2}
`), env(nil))
	require.NoError(t, err)
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]Size{"0": 0, "10": 10, "10B": 10, "1KiB": 1024, "64MiB": 64 << 20, "2 GiB": 2 << 30, "1TiB": 1 << 40} {
		got, err := ParseSize(in)
		require.NoError(t, err, in)
		require.Equal(t, want, got, in)
	}
	for _, in := range []string{"", "1M", "1MB", "-1", "1.5GiB", "99999999999TiB"} {
		_, err := ParseSize(in)
		require.Error(t, err, in)
	}
}

func TestParseLocation(t *testing.T) {
	l, err := ParseLocation("do/uploads//images/")
	require.NoError(t, err)
	require.Equal(t, Location{Provider: "do", Bucket: "uploads", Prefix: "images/"}, l)
	require.Equal(t, "do/uploads/images", l.String())

	l, err = ParseLocation("do/uploads/")
	require.NoError(t, err)
	require.Equal(t, "", l.Prefix)
	require.Equal(t, "do/uploads", l.String())

	for _, in := range []string{"do", "do/", "/bucket", "do/ab"} {
		_, err := ParseLocation(in)
		require.Error(t, err, in)
	}
}

func TestErrorLines(t *testing.T) {
	// Blank lines and comments must not shift the reported lines.
	data := base + `

# comment

defaults:

  interval: 5 minutes

sync:

  - source: src/aaa
    target: dst/aaa
    timeout: forever
    colour: red
`
	_, err := Parse([]byte(data), env(nil))
	require.Error(t, err)
	// All errors are reported, not only the first one.
	require.ErrorContains(t, err, `line 19: defaults.interval: invalid duration "5 minutes"`)
	require.ErrorContains(t, err, `line 25: sync[0].timeout: invalid duration "forever"`)
	require.ErrorContains(t, err, `line 26: unknown key "colour" in sync[0]`)
}

func TestEnvStrings(t *testing.T) {
	for _, v := range []string{"null", "~", "Null", "yes", "0x10", "007", "a: b", "#x", " x "} {
		cfg, err := Parse([]byte(`
providers:
  src:
    endpoint: https://a.example
    region: x
    access_key: ${K}
    secret_key: b
  dst: {endpoint: https://b.example, region: x, access_key: c, secret_key: d}
sync: [{source: src/aaa, target: dst/aaa}]
`), env(map[string]string{"K": v}))
		require.NoError(t, err, v)
		require.Equal(t, v, cfg.Providers["src"].AccessKey, v)
	}
}

func TestLegacyBucketName(t *testing.T) {
	cfg, err := Parse([]byte(base+`
sync: [{source: src/Legacy_Bucket, target: dst/aaa}]
`), env(nil))
	require.NoError(t, err)
	require.Equal(t, "Legacy_Bucket", cfg.Syncs[0].Source.Bucket)
}

func TestScalarErrorsTogether(t *testing.T) {
	_, err := Parse([]byte(base+`
workers: 0x
defaults:
  delete:
    min_count: abc
sync:
  - source: src/aaa
    target: dst/aaa
    intervall: 5m
`), env(nil))
	require.Error(t, err)
	require.ErrorContains(t, err, "(workers)")
	require.ErrorContains(t, err, "(defaults.delete.min_count)")
	require.ErrorContains(t, err, `unknown key "intervall"`)
}

func TestEnvBoolCase(t *testing.T) {
	cfg, err := Parse([]byte(`
providers:
  src:
    endpoint: https://a.example
    region: x
    path_style: ${B}
    access_key: a
    secret_key: b
  dst: {endpoint: https://b.example, region: x, access_key: c, secret_key: d}
sync: [{source: src/aaa, target: dst/aaa}]
`), env(map[string]string{"B": "True"}))
	require.NoError(t, err)
	require.True(t, cfg.Providers["src"].PathStyle)
}

// schema and fileConfig must describe the same top-level keys: a key missing
// from fileConfig would pass the check and then be ignored. Nested structs are
// shared by both, so only the top level can drift.
func TestSchemaMatchesFileConfig(t *testing.T) {
	keys := func(v any) []string {
		var k []string
		for name := range yamlFields(reflect.TypeOf(v)) {
			k = append(k, name)
		}
		sort.Strings(k)
		return k
	}
	require.Equal(t, keys(schema{}), keys(fileConfig{}))
}

func TestEnvValueNotInErrors(t *testing.T) {
	_, err := Parse([]byte(`
providers:
  src:
    endpoint: https://a.example
    region: x
    access_key: a
    secret_key: b
    rate_limit:
      requests_per_bucket: ${SECRET}
      bandwidth_total: ${SECRET}
  dst: {endpoint: https://b.example, region: x, access_key: c, secret_key: d}
sync: [{source: src/aaa, target: dst/aaa}]
`), env(map[string]string{"SECRET": "s3cr3t-value"}))
	require.ErrorContains(t, err, "providers.src.rate_limit.requests_per_bucket: invalid value from environment variable")
	require.ErrorContains(t, err, "providers.src.rate_limit.bandwidth_total: invalid value from environment variable")
	require.NotContains(t, err.Error(), "s3cr3t-value")
}

func TestLookupEnvOrZero(t *testing.T) {
	data, err := os.ReadFile("../../config.example.yaml")
	require.NoError(t, err)
	_, err = Parse(data, LookupEnvOrZero)
	require.NoError(t, err)
}

func TestIsAWS(t *testing.T) {
	for e, want := range map[string]bool{
		"https://s3.eu-central-1.amazonaws.com":  true,
		"https://s3.cn-north-1.amazonaws.com.cn": true,
		"https://fra1.digitaloceanspaces.com":    false,
		"https://amazonaws.com.example.org":      false,
	} {
		require.Equal(t, want, Provider{Endpoint: e}.IsAWS(), e)
	}
}
