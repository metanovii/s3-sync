// Package config loads and validates the s3-sync configuration file.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Values accepted by Provider.Checksum.
const (
	ChecksumWhenRequired  = "when_required"
	ChecksumWhenSupported = "when_supported"
)

// Values accepted by SyncOptions.ACL besides canned ACLs.
const (
	ACLSkip = "skip"
	ACLCopy = "copy"
)

// CannedACLs are the canned ACLs accepted by SyncOptions.ACL. With ACLCopy
// only the first four can be recognised in a source ACL (see docs/spec.md).
var CannedACLs = []string{
	"private",
	"public-read",
	"public-read-write",
	"authenticated-read",
	"aws-exec-read",
	"bucket-owner-read",
	"bucket-owner-full-control",
}

type Config struct {
	State     State
	Metrics   Metrics
	Workers   int
	Providers map[string]Provider
	Syncs     []Sync
}

type State struct {
	Path string `yaml:"path"`
}

type Metrics struct {
	Listen string `yaml:"listen"`
}

type Provider struct {
	Endpoint  string    `yaml:"endpoint"`
	Region    string    `yaml:"region"`
	PathStyle bool      `yaml:"path_style"`
	Checksum  string    `yaml:"checksum"`
	AccessKey string    `yaml:"access_key"`
	SecretKey string    `yaml:"secret_key"`
	RateLimit RateLimit `yaml:"rate_limit"`
}

// DefaultChain reports whether the provider takes credentials from the AWS
// SDK default chain.
func (p Provider) DefaultChain() bool {
	return p.AccessKey == "" && p.SecretKey == ""
}

// IsAWS reports whether the endpoint belongs to AWS.
func (p Provider) IsAWS() bool {
	u, err := url.Parse(p.Endpoint)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	return strings.HasSuffix(host, ".amazonaws.com") || strings.HasSuffix(host, ".amazonaws.com.cn")
}

type RateLimit struct {
	// RequestsPerBucket is requests per second to one bucket; 0 is unlimited.
	RequestsPerBucket float64 `yaml:"requests_per_bucket"`
	// BandwidthTotal is bytes per second to and from the provider; 0 is unlimited.
	BandwidthTotal Size `yaml:"bandwidth_total"`
}

// SyncOptions are the settings a sync inherits from defaults.
type SyncOptions struct {
	Interval  Duration `yaml:"interval"`
	Timeout   Duration `yaml:"timeout"`
	FullCheck Duration `yaml:"full_check"`
	ACL       string   `yaml:"acl"`
	Delete    Delete   `yaml:"delete"`
}

type Delete struct {
	Enabled     bool     `yaml:"enabled"`
	Delay       Duration `yaml:"delay"`
	MinCount    int      `yaml:"min_count"`
	MaxFraction float64  `yaml:"max_fraction"`
	MaxCount    int      `yaml:"max_count"`
}

type Sync struct {
	Source Location
	Target Location
	SyncOptions
}

// ID identifies the sync in state, metrics and the CLI.
func (s Sync) ID() string {
	return s.Source.String() + " -> " + s.Target.String()
}

// fileConfig is the file layout. Sync entries are kept as nodes and decoded
// over the defaults one by one.
type fileConfig struct {
	State     State               `yaml:"state"`
	Metrics   Metrics             `yaml:"metrics"`
	Workers   *int                `yaml:"workers"`
	Providers map[string]Provider `yaml:"providers"`
	Defaults  yaml.Node           `yaml:"defaults"`
	Sync      []yaml.Node         `yaml:"sync"`
}

// schema is the same layout with typed sync settings, used by checkNode.
type schema struct {
	State     State               `yaml:"state"`
	Metrics   Metrics             `yaml:"metrics"`
	Workers   int                 `yaml:"workers"`
	Providers map[string]Provider `yaml:"providers"`
	Defaults  SyncOptions         `yaml:"defaults"`
	Sync      []fileSync          `yaml:"sync"`
}

type fileSync struct {
	Source      string `yaml:"source"`
	Target      string `yaml:"target"`
	SyncOptions `yaml:",inline"`
}

func defaultSyncOptions() SyncOptions {
	return SyncOptions{
		Interval:  Duration(10 * time.Minute),
		Timeout:   Duration(2 * time.Hour),
		FullCheck: Duration(24 * time.Hour),
		ACL:       ACLSkip,
		Delete: Delete{
			Enabled:     true,
			Delay:       Duration(time.Hour),
			MinCount:    100,
			MaxFraction: 0.01,
			MaxCount:    10000,
		},
	}
}

// Load reads, expands and validates the configuration file. lookupEnv
// resolves ${NAME}; os.LookupEnv is the usual choice.
func Load(path string, lookupEnv func(string) (string, bool)) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg, err := Parse(data, lookupEnv)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// LookupEnvOrZero is os.LookupEnv that treats unset variables as "0". It lets
// `validate --no-env` check a configuration without its secrets: "0" is a
// valid key, number, size and duration.
func LookupEnvOrZero(name string) (string, bool) {
	if v, ok := os.LookupEnv(name); ok {
		return v, true
	}
	return "0", true
}

// Parse builds the configuration from YAML. lookupEnv resolves ${NAME}.
// The document is decoded from the parsed node tree, so line numbers in
// errors match the file.
func Parse(data []byte, lookupEnv func(string) (string, bool)) (*Config, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, err
	}
	if root.Kind == 0 || len(root.Content) == 0 || root.Content[0].ShortTag() == "!!null" {
		return nil, errors.New("configuration is empty")
	}
	expanded, errs := expandEnv(&root, lookupEnv)
	c := checker{expanded: expanded}
	errs = append(errs, c.node(&root, reflect.TypeOf(schema{}), "")...)
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	var fc fileConfig
	if err := root.Decode(&fc); err != nil {
		return nil, err
	}
	// Every sync starts from defaults, which start from the built-in values;
	// nested blocks are merged field by field.
	defaults := defaultSyncOptions()
	if fc.Defaults.Kind != 0 {
		if err := fc.Defaults.Decode(&defaults); err != nil {
			return nil, err
		}
	}

	cfg := &Config{
		State:     fc.State,
		Metrics:   fc.Metrics,
		Workers:   32,
		Providers: fc.Providers,
	}
	if fc.Workers != nil {
		cfg.Workers = *fc.Workers
	}
	if cfg.State.Path == "" {
		cfg.State.Path = "/var/lib/s3-sync/state.db"
	}
	if cfg.Metrics.Listen == "" {
		cfg.Metrics.Listen = ":9090"
	}
	for name, p := range cfg.Providers {
		if p.Checksum == "" {
			p.Checksum = ChecksumWhenRequired
			if p.IsAWS() {
				p.Checksum = ChecksumWhenSupported
			}
			cfg.Providers[name] = p
		}
	}

	for i, n := range fc.Sync {
		fs := fileSync{SyncOptions: defaults}
		if err := n.Decode(&fs); err != nil {
			errs = append(errs, err)
			continue
		}
		src, err := ParseLocation(fs.Source)
		if err != nil {
			errs = append(errs, fmt.Errorf("line %d: sync[%d].source: %w", n.Line, i, err))
		}
		dst, err2 := ParseLocation(fs.Target)
		if err2 != nil {
			errs = append(errs, fmt.Errorf("line %d: sync[%d].target: %w", n.Line, i, err2))
		}
		if err != nil || err2 != nil {
			continue
		}
		cfg.Syncs = append(cfg.Syncs, Sync{Source: src, Target: dst, SyncOptions: fs.SyncOptions})
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

var (
	envRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)
	// plainRe matches values that stay numbers or booleans after expansion.
	plainRe = regexp.MustCompile(`^((?i:true|false)|[-+]?(\d+|\d*\.\d+)([eE][-+]?\d+)?)$`)
)

// expandEnv replaces ${NAME} in every scalar value. An unset variable is an
// error; keys are left as they are. An expanded value is a string unless it
// is a plain number or boolean, so that ${RPS} can be a number while a secret
// such as "null" stays a string. It returns the expanded nodes.
func expandEnv(n *yaml.Node, lookupEnv func(string) (string, bool)) (map[*yaml.Node]bool, []error) {
	expanded := make(map[*yaml.Node]bool)
	var errs []error
	var walk func(n *yaml.Node, isKey bool)
	walk = func(n *yaml.Node, isKey bool) {
		switch n.Kind {
		case yaml.ScalarNode:
			if isKey || !strings.Contains(n.Value, "${") {
				return
			}
			n.Value = envRe.ReplaceAllStringFunc(n.Value, func(m string) string {
				name := envRe.FindStringSubmatch(m)[1]
				v, ok := lookupEnv(name)
				if !ok {
					errs = append(errs, fmt.Errorf("line %d: environment variable %s is not set", n.Line, name))
				}
				return v
			})
			expanded[n] = true
			if n.Style == 0 && plainRe.MatchString(n.Value) {
				n.Tag = ""
			} else {
				n.Tag = "!!str"
			}
		case yaml.MappingNode:
			for i := 0; i+1 < len(n.Content); i += 2 {
				walk(n.Content[i], true)
				walk(n.Content[i+1], false)
			}
		default:
			for _, c := range n.Content {
				walk(c, false)
			}
		}
	}
	walk(n, false)
	return expanded, errs
}
