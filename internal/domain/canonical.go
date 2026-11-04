package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// ContentHash is the idempotency identity of a create request.
// Object key order and insignificant whitespace do not change the hash.
// JSON numbers keep their original literal, so 1 and 1.0 are different content.
// scheduledAt is normalized to UTC microseconds before it is hashed.
func ContentHash(recipientID, sender string, scheduledAt time.Time, payload []byte) ([]byte, error) {
	canon, err := CanonicalJSON(payload)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteString(`{"payload":`)
	buf.Write(canon)
	buf.WriteString(`,"recipient_id":`)
	if err := writeString(&buf, recipientID); err != nil {
		return nil, err
	}
	buf.WriteString(`,"scheduled_at":`)
	if err := writeString(&buf, FormatTime(scheduledAt)); err != nil {
		return nil, err
	}
	buf.WriteString(`,"sender":`)
	if err := writeString(&buf, sender); err != nil {
		return nil, err
	}
	buf.WriteByte('}')
	sum := sha256.Sum256(buf.Bytes())
	return sum[:], nil
}

func CanonicalJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := writeCanonical(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCanonical(buf *bytes.Buffer, v any) error {
	switch t := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if t {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case json.Number:
		if t == "" {
			return fmt.Errorf("empty json number")
		}
		buf.WriteString(string(t))
	case string:
		return writeString(buf, t)
	case []any:
		buf.WriteByte('[')
		for i, el := range t {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, el); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeString(buf, k); err != nil {
				return err
			}
			buf.WriteByte(':')
			if err := writeCanonical(buf, t[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("unsupported json value %T", v)
	}
	return nil
}

func writeString(buf *bytes.Buffer, s string) error {
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return err
	}
	if buf.Len() > 0 && buf.Bytes()[buf.Len()-1] == '\n' {
		buf.Truncate(buf.Len() - 1)
	}
	return nil
}
