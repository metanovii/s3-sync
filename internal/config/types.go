package config

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration written in Go syntax ("10m", "2h").
// A bare "0" is also accepted.
type Duration time.Duration

// ParseDuration parses a duration in Go syntax; a bare "0" is also accepted.
func ParseDuration(s string) (Duration, error) {
	if s == "0" {
		return 0, nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", s)
	}
	return Duration(v), nil
}

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: duration must be a scalar", n.Line)
	}
	v, err := ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*d = v
	return nil
}

// Size is a number of bytes written with a binary unit ("64MiB").
// A bare integer means bytes.
type Size int64

var sizeUnits = map[string]int64{
	"":    1,
	"B":   1,
	"KiB": 1 << 10,
	"MiB": 1 << 20,
	"GiB": 1 << 30,
	"TiB": 1 << 40,
}

var sizeRe = regexp.MustCompile(`^(\d+)\s*([A-Za-z]*)$`)

// ParseSize parses a size such as "100MiB".
func ParseSize(s string) (Size, error) {
	m := sizeRe.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	mult, ok := sizeUnits[m[2]]
	if !ok {
		return 0, fmt.Errorf("invalid size %q: unit must be one of B, KiB, MiB, GiB, TiB", s)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || n > (1<<63-1)/mult {
		return 0, fmt.Errorf("invalid size %q: out of range", s)
	}
	return Size(n * mult), nil
}

func (s *Size) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: size must be a scalar", n.Line)
	}
	v, err := ParseSize(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*s = v
	return nil
}

// Location is <provider>/<bucket>[/<prefix>].
type Location struct {
	Provider string
	Bucket   string
	// Prefix is empty or ends with "/".
	Prefix string
}

// bucketRe is deliberately loose: providers differ, and legacy AWS buckets
// allow upper case and underscores.
var bucketRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{1,253}[A-Za-z0-9]$`)

// ParseLocation parses <provider>/<bucket>[/<prefix>]. Leading and trailing
// slashes of the prefix are ignored; the prefix is treated as a directory.
func ParseLocation(s string) (Location, error) {
	parts := strings.SplitN(s, "/", 3)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return Location{}, fmt.Errorf("invalid location %q: want <provider>/<bucket>[/<prefix>]", s)
	}
	if !bucketRe.MatchString(parts[1]) || strings.Contains(parts[1], "..") {
		return Location{}, fmt.Errorf("invalid location %q: invalid bucket name %q", s, parts[1])
	}
	l := Location{Provider: parts[0], Bucket: parts[1]}
	if len(parts) == 3 {
		if p := strings.Trim(parts[2], "/"); p != "" {
			l.Prefix = p + "/"
		}
	}
	return l, nil
}

func (l Location) String() string {
	s := l.Provider + "/" + l.Bucket
	if l.Prefix != "" {
		s += "/" + strings.TrimSuffix(l.Prefix, "/")
	}
	return s
}
