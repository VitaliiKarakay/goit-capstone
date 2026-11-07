package domain

import (
	"strings"
	"unicode/utf8"
)

const MaxTemplateFieldBytes = 8 * 1024

// RenderTemplate substitutes [name] placeholders in subject and body using a single left-to-right pass.
// Values are not scanned for further placeholders. Escapes: \[ \] \\ only; other \ sequences are errors.
func RenderTemplate(subjectPrefix, bodyPrefix, subject, body string, variables map[string]string) (string, string, []FieldError) {
	sub, errs := renderField(subjectPrefix, subject, variables)
	bod, err2 := renderField(bodyPrefix, body, variables)
	return sub, bod, append(errs, err2...)
}

func ScanTemplateSyntax(subjectPrefix, bodyPrefix, subject, body string) []FieldError {
	if errs := scanSyntax(subjectPrefix, subject); len(errs) > 0 {
		return errs
	}
	return scanSyntax(bodyPrefix, body)
}

func scanSyntax(field, text string) []FieldError {
	i := 0
	for i < len(text) {
		if text[i] == '\\' {
			if i+1 >= len(text) {
				return []FieldError{{Field: field, Code: "invalid_escape"}}
			}
			switch text[i+1] {
			case '[', ']', '\\':
				i += 2
			default:
				return []FieldError{{Field: field, Code: "invalid_escape"}}
			}
			continue
		}
		if text[i] == ']' {
			return []FieldError{{Field: field, Code: "invalid"}}
		}
		if text[i] == '[' {
			_, next, ok := parsePlaceholder(text, i)
			if !ok {
				return []FieldError{{Field: field, Code: "invalid"}}
			}
			i = next
			continue
		}
		_, size := utf8.DecodeRuneInString(text[i:])
		i += size
	}
	return nil
}

func renderField(field, text string, variables map[string]string) (string, []FieldError) {
	var out strings.Builder
	var details []FieldError
	limit := MaxPayloadBytes
	appendBytes := func(b []byte) bool {
		if out.Len()+len(b) > limit {
			details = append(details, FieldError{Field: field, Code: "too_large"})
			return false
		}
		out.Write(b)
		return true
	}
	i := 0
loop:
	for i < len(text) {
		if text[i] == '\\' {
			if i+1 >= len(text) {
				details = append(details, FieldError{Field: field, Code: "invalid_escape"})
				break
			}
			switch text[i+1] {
			case '[', ']', '\\':
				if !appendBytes([]byte{text[i+1]}) {
					break loop
				}
				i += 2
			default:
				details = append(details, FieldError{Field: field, Code: "invalid_escape"})
				i = len(text)
			}
			continue
		}
		if text[i] == ']' {
			details = append(details, FieldError{Field: field, Code: "invalid"})
			break
		}
		if text[i] == '[' {
			name, next, ok := parsePlaceholder(text, i)
			if !ok {
				details = append(details, FieldError{Field: field, Code: "invalid"})
				break
			}
			value, found := variables[name]
			if !found {
				details = append(details, FieldError{Field: field + ".variables." + name, Code: "missing_variable"})
			} else if !appendBytes([]byte(value)) {
				break loop
			}
			i = next
			continue
		}
		r, size := utf8.DecodeRuneInString(text[i:])
		if out.Len()+size > limit {
			details = append(details, FieldError{Field: field, Code: "too_large"})
			break
		}
		out.WriteRune(r)
		i += size
	}
	if len(details) > 0 {
		return "", details
	}
	return out.String(), nil
}

func parsePlaceholder(text string, start int) (name string, next int, ok bool) {
	if start >= len(text) || text[start] != '[' {
		return "", start, false
	}
	i := start + 1
	for i < len(text) && text[i] != ']' {
		if text[i] == '\\' {
			return "", start, false
		}
		i++
	}
	if i >= len(text) {
		return "", start, false
	}
	name = text[start+1 : i]
	if !isPlaceholderName(name) {
		return "", start, false
	}
	return name, i + 1, true
}

func isPlaceholderName(s string) bool {
	if s == "" {
		return false
	}
	first := s[0]
	if first != '_' && (first < 'A' || first > 'Z') && (first < 'a' || first > 'z') {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if c != '_' && (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}
