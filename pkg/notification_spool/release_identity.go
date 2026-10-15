package notification_spool

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// Feishu renders carry JSON as a string within FrozenNotification. Equivalent
// object key order/spacing must not invalidate a byte-hash-verified AM retry.
// Strings, arrays, numbers and credential/config values remain exact. Other
// channels retain their original byte identity.
func canonicalFeishuTarget(t Target) Target {
	if t.Channel != "feishu" || t.ContentRevision != Hash(t.Payload) {
		return t
	}
	raw, err := uniqueJSON(t.Payload)
	if err != nil {
		return t
	}
	payload, ok := raw.(map[string]interface{})
	if !ok {
		return t
	}
	content, ok := payload["content"].(string)
	if !ok {
		return t
	}
	decoded, err := uniqueJSON([]byte(content))
	if err != nil {
		return t
	}
	if _, ok := decoded.(map[string]interface{}); !ok {
		return t
	}
	normalized, err := json.Marshal(decoded)
	if err != nil {
		return t
	}
	payload["content"] = string(normalized)
	normalized, err = json.Marshal(payload)
	if err != nil {
		return t
	}
	t.Payload = normalized
	t.ContentRevision = Hash(normalized)
	return t
}

// Duplicate object keys are deliberately unsupported: silently selecting one
// occurrence could equate two messages that a platform interprets differently.
func uniqueJSON(raw []byte) (interface{}, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	v, err := readUniqueJSON(d)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errors.New("JSON contains trailing tokens")
	}
	return v, nil
}
func readUniqueJSON(d *json.Decoder) (interface{}, error) {
	tok, err := d.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := tok.(json.Delim)
	if !ok {
		return tok, nil
	}
	switch delimiter {
	case '{':
		out := map[string]interface{}{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, errors.New("invalid JSON object key")
			}
			if _, exists := out[name]; exists {
				return nil, errors.New("duplicate JSON object key")
			}
			v, err := readUniqueJSON(d)
			if err != nil {
				return nil, err
			}
			out[name] = v
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return nil, errors.New("invalid JSON object")
		}
		return out, nil
	case '[':
		out := []interface{}{}
		for d.More() {
			v, err := readUniqueJSON(d)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return nil, errors.New("invalid JSON array")
		}
		return out, nil
	default:
		return nil, errors.New("unexpected JSON delimiter")
	}
}
