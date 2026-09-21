package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	"go.yaml.in/yaml/v3"
)

const MaxDocumentBytes = 1 << 20

func decodeDocument(r io.Reader, target any) error {
	// Read one extra byte so truncation is rejected instead of admitting a valid
	// prefix. Bounded depth and no aliases keep parsing work proportional to input.
	b, err := io.ReadAll(io.LimitReader(r, MaxDocumentBytes+1))
	if err != nil {
		return err
	}
	if len(b) > MaxDocumentBytes {
		return fmt.Errorf("specification exceeds 1 MiB")
	}
	d := yaml.NewDecoder(bytes.NewReader(b))
	var node yaml.Node
	if err = d.Decode(&node); err != nil {
		return err
	}
	if err = checkNode(&node, 0); err != nil {
		return err
	}
	var extra yaml.Node
	if err = d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("exactly one document is required")
	}
	var value any
	if err = node.Decode(&value); err != nil {
		return err
	}
	if err = checkFields(value, reflect.TypeOf(target).Elem()); err != nil {
		return err
	}
	b, err = json.Marshal(value)
	if err != nil {
		return err
	}
	// Decode through JSON so YAML numbers cannot silently become string env
	// values; both accepted input formats produce the same typed representation.
	strict := json.NewDecoder(bytes.NewReader(b))
	strict.DisallowUnknownFields()
	return strict.Decode(target)
}

func checkFields(value any, t reflect.Type) error {
	// encoding/json intentionally accepts case-insensitive struct fields. The
	// public schema uses exact names so typos cannot alter canonical identity.
	switch t.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("expected object")
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			fields[strings.Split(f.Tag.Get("json"), ",")[0]] = f.Type
		}
		for k, v := range object {
			field, ok := fields[k]
			if !ok {
				return fmt.Errorf("unknown field %q", k)
			}
			if err := checkFields(v, field); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if values, ok := value.([]any); ok {
			for _, v := range values {
				if err := checkFields(v, t.Elem()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func checkNode(n *yaml.Node, depth int) error {
	if depth > 32 || n.Kind == yaml.AliasNode || n.Tag == "!!null" {
		return fmt.Errorf("nulls, aliases, and nesting beyond 32 levels are unsupported")
	}
	if n.Kind == yaml.MappingNode {
		seen := map[string]bool{}
		for i := 0; i < len(n.Content); i += 2 {
			key := n.Content[i]
			if key.Tag != "!!str" || seen[key.Value] {
				return fmt.Errorf("mapping keys must be unique strings")
			}
			seen[key.Value] = true
		}
	}
	for _, child := range n.Content {
		if err := checkNode(child, depth+1); err != nil {
			return err
		}
	}
	return nil
}
