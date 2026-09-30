package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
)

var providerNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func (c *Config) validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if c.Workers < 1 {
		add("workers must be at least 1")
	}
	if len(c.Providers) == 0 {
		add("providers: at least one provider is required")
	}
	if len(c.Syncs) == 0 {
		add("sync: at least one sync is required")
	}

	names := make([]string, 0, len(c.Providers))
	for name := range c.Providers {
		names = append(names, name)
	}
	sort.Strings(names)

	var defaultChain []string
	defaultChainNonAWS := false
	for _, name := range names {
		p := c.Providers[name]
		pfx := "providers." + name
		if !providerNameRe.MatchString(name) {
			add("%s: name may contain only letters, digits, '_' and '-'", pfx)
		}
		if u, err := url.Parse(p.Endpoint); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			add("%s: endpoint must be an http or https URL, got %q", pfx, p.Endpoint)
		}
		if p.Region == "" {
			add("%s: region is required", pfx)
		}
		if p.Checksum != ChecksumWhenRequired && p.Checksum != ChecksumWhenSupported {
			add("%s: checksum must be %s or %s, got %q", pfx, ChecksumWhenRequired, ChecksumWhenSupported, p.Checksum)
		}
		if (p.AccessKey == "") != (p.SecretKey == "") {
			add("%s: access_key and secret_key must be set together", pfx)
		}
		if p.DefaultChain() {
			defaultChain = append(defaultChain, name)
			if !p.IsAWS() {
				defaultChainNonAWS = true
			}
		}
		if p.RateLimit.RequestsPerBucket < 0 {
			add("%s: rate_limit.requests_per_bucket must not be negative", pfx)
		}
	}
	if len(defaultChain) > 1 && defaultChainNonAWS {
		add("providers %s take credentials from the AWS SDK default chain and would share them; set access_key and secret_key",
			strings.Join(defaultChain, ", "))
	}

	seen := make(map[string]int)
	for i, s := range c.Syncs {
		pfx := fmt.Sprintf("sync[%d] (%s)", i, s.ID())
		for _, l := range []Location{s.Source, s.Target} {
			if _, ok := c.Providers[l.Provider]; !ok {
				add("%s: unknown provider %q", pfx, l.Provider)
			}
		}
		if j, ok := seen[s.ID()]; ok {
			add("%s: duplicates sync[%d]", pfx, j)
		} else {
			seen[s.ID()] = i
		}
		errs = append(errs, validateOptions(pfx, s.SyncOptions)...)
	}
	if len(errs) > 0 {
		// Overlap checks need known providers.
		return errors.Join(errs...)
	}

	for i, a := range c.Syncs {
		for j, b := range c.Syncs {
			if i < j && c.overlap(a.Target, b.Target) {
				add("sync[%d] and sync[%d]: targets %s and %s overlap", i, j, a.Target, b.Target)
			}
			if c.overlap(a.Target, b.Source) {
				if i == j {
					add("sync[%d]: target %s overlaps its source %s", i, a.Target, b.Source)
				} else {
					add("sync[%d]: target %s overlaps the source %s of sync[%d]", i, a.Target, b.Source, j)
				}
			}
		}
	}
	return errors.Join(errs...)
}

func validateOptions(pfx string, o SyncOptions) []error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(pfx+": "+format, a...)) }
	if o.Interval <= 0 {
		add("interval must be positive")
	}
	if o.Timeout <= 0 {
		add("timeout must be positive")
	}
	if o.FullCheck < 0 {
		add("full_check must not be negative")
	}
	if o.ACL != ACLSkip && o.ACL != ACLCopy && !slices.Contains(CannedACLs, o.ACL) {
		add("acl must be %s, %s or one of %s, got %q", ACLSkip, ACLCopy, strings.Join(CannedACLs, ", "), o.ACL)
	}
	d := o.Delete
	if d.Delay < 0 {
		add("delete.delay must not be negative")
	}
	if d.MinCount < 0 {
		add("delete.min_count must not be negative")
	}
	if d.MaxCount < 0 {
		add("delete.max_count must not be negative")
	}
	if d.MaxFraction < 0 || d.MaxFraction > 1 {
		add("delete.max_fraction must be between 0 and 1")
	}
	return errs
}

// overlap reports whether two locations share keys: same bucket on the same
// endpoint (any AWS endpoint, as AWS bucket names are global), and one prefix
// contains the other.
func (c *Config) overlap(a, b Location) bool {
	pa, pb := c.Providers[a.Provider], c.Providers[b.Provider]
	if a.Bucket != b.Bucket {
		return false
	}
	sameStorage := pa.IsAWS() && pb.IsAWS() || NormalizeEndpoint(pa.Endpoint) == NormalizeEndpoint(pb.Endpoint)
	if !sameStorage {
		return false
	}
	return strings.HasPrefix(a.Prefix, b.Prefix) || strings.HasPrefix(b.Prefix, a.Prefix)
}

// NormalizeEndpoint returns the lower-case host with the default port of the
// scheme removed.
func NormalizeEndpoint(e string) string {
	u, err := url.Parse(e)
	if err != nil {
		return e
	}
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" || (u.Scheme == "https" && port == "443") || (u.Scheme == "http" && port == "80") {
		return host
	}
	return host + ":" + port
}
