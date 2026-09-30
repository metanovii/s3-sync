package config

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	durationType = reflect.TypeOf(Duration(0))
	sizeType     = reflect.TypeOf(Size(0))
	nodeType     = reflect.TypeOf(yaml.Node{})
)

// node walks n against the Go type t and reports, with their lines,
// unknown keys, invalid sizes and durations, wrong kinds of nodes and scalars
// that do not fit their field. yaml.v3 has no strict mode for Node.Decode, and
// a failing UnmarshalYAML stops decoding at the first error, so everything is
// checked here before decoding.
// checker checks a document against its schema. Values in expanded came from
// environment variables and are never quoted in errors, as they may be secrets.
type checker struct {
	expanded map[*yaml.Node]bool
}

func (c *checker) node(n *yaml.Node, t reflect.Type, path string) []error {
	for n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if n.Kind == yaml.DocumentNode {
		if len(n.Content) == 0 {
			return nil
		}
		return c.node(n.Content[0], t, path)
	}
	if n.ShortTag() == "!!null" {
		return nil
	}

	switch t {
	case nodeType:
		return nil
	case durationType, sizeType:
		if n.Kind != yaml.ScalarNode {
			return []error{fmt.Errorf("line %d: %s must be a scalar", n.Line, path)}
		}
		var err error
		if t == durationType {
			_, err = ParseDuration(n.Value)
		} else {
			_, err = ParseSize(n.Value)
		}
		if err != nil {
			if c.expanded[n] {
				return []error{fmt.Errorf("line %d: %s: invalid value from environment variable", n.Line, path)}
			}
			return []error{fmt.Errorf("line %d: %s: %w", n.Line, path, err)}
		}
		return nil
	}

	where := path
	if where == "" {
		where = "top level"
	}
	var errs []error
	switch t.Kind() {
	case reflect.Struct:
		if n.Kind != yaml.MappingNode {
			return []error{fmt.Errorf("line %d: %s must be a mapping", n.Line, where)}
		}
		fields := yamlFields(t)
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			if k.ShortTag() == "!!merge" {
				errs = append(errs, c.merge(v, t, path)...)
				continue
			}
			ft, ok := fields[k.Value]
			if !ok {
				errs = append(errs, fmt.Errorf("line %d: unknown key %q in %s", k.Line, k.Value, where))
				continue
			}
			errs = append(errs, c.node(v, ft, join(path, k.Value))...)
		}
	case reflect.Map:
		if n.Kind != yaml.MappingNode {
			return []error{fmt.Errorf("line %d: %s must be a mapping", n.Line, where)}
		}
		for i := 0; i+1 < len(n.Content); i += 2 {
			errs = append(errs, c.node(n.Content[i+1], t.Elem(), join(path, n.Content[i].Value))...)
		}
	case reflect.Slice:
		if n.Kind != yaml.SequenceNode {
			return []error{fmt.Errorf("line %d: %s must be a list", n.Line, where)}
		}
		for i, item := range n.Content {
			errs = append(errs, c.node(item, t.Elem(), fmt.Sprintf("%s[%d]", path, i))...)
		}
	default:
		// Scalars: let the decoder check the value now, so that all errors
		// are reported together.
		var te *yaml.TypeError
		err := n.Decode(reflect.New(t).Interface())
		if err != nil && c.expanded[n] {
			errs = append(errs, fmt.Errorf("line %d: %s: invalid value from environment variable", n.Line, where))
		} else if errors.As(err, &te) {
			for _, e := range te.Errors {
				errs = append(errs, fmt.Errorf("%s (%s)", e, where))
			}
		} else if err != nil {
			errs = append(errs, fmt.Errorf("line %d: %s: %w", n.Line, where, err))
		}
	}
	return errs
}

// merge checks the value of a "<<" merge key: a mapping or a sequence of
// mappings.
func (c *checker) merge(v *yaml.Node, t reflect.Type, path string) []error {
	for v.Kind == yaml.AliasNode {
		v = v.Alias
	}
	if v.Kind != yaml.SequenceNode {
		return c.node(v, t, path)
	}
	var errs []error
	for _, item := range v.Content {
		errs = append(errs, c.node(item, t, path)...)
	}
	return errs
}

// yamlFields maps yaml keys of a struct, including inline structs, to types.
func yamlFields(t reflect.Type) map[string]reflect.Type {
	fields := make(map[string]reflect.Type)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name, opts, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		if name == "-" {
			continue
		}
		if strings.Contains(opts, "inline") {
			for k, v := range yamlFields(f.Type) {
				fields[k] = v
			}
			continue
		}
		if name == "" {
			name = strings.ToLower(f.Name)
		}
		fields[name] = f.Type
	}
	return fields
}

func join(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}
